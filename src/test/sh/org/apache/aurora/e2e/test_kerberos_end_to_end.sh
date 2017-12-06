#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# An integration test for the client, using the vagrant environment as a testbed.
set -eux

readonly KRB5_MAJOR_MINOR=1.13
readonly KRB5_VERSION=1.13.1
readonly KRB5_URL_BASE=http://web.mit.edu/kerberos/dist/krb5/
readonly KRB5_SIGNED_TARBALL=krb5-$KRB5_VERSION-signed.tar
readonly KRB5_TARBALL=krb5-$KRB5_VERSION.tar.gz
readonly KRB5_KEY_ID=0x749D7889
readonly SCHEDULER_HOSTNAME=aurora.local

function enter_vagrant {
  exec vagrant ssh -- /vagrant/src/test/sh/org/apache/aurora/e2e/test_kerberos_end_to_end.sh "$@"
}

function enter_testrealm {
  cd $HOME
    [[ -f $KRB5_SIGNED_TARBALL ]] || wget "$KRB5_URL_BASE/$KRB5_MAJOR_MINOR/$KRB5_SIGNED_TARBALL"
    [[ -f $KRB5_TARBALL.asc ]] || tar xvf $KRB5_SIGNED_TARBALL
    gpg --list-keys $KRB5_KEY_ID &>/dev/null || gpg --keyserver pgp.mit.edu --recv-keys $KRB5_KEY_ID
    gpg --verify $KRB5_TARBALL.asc
    [[ -d `basename $KRB5_TARBALL .tar.gz` ]] || tar zxvf $KRB5_TARBALL
    cd `basename $KRB5_TARBALL .tar.gz`
      mkdir -p build
      cd build
        [[ -f Makefile ]] || ../src/configure
        make
        # Reinvokes this script with a full kerberos test realm configured.
        SHELL=$0 exec make testrealm
}

function await_scheduler_ready {
  while ! curl -s localhost:8081/vars | grep "framework_registered 1"; do
    sleep 3
  done
}

readonly SNAPSHOT_RPC_DATA="[1,\"snapshot\",1,0,{}]"
readonly SNAPSHOT_RESPONSE_OUTFILE="snapshot-response.%s.json"
function snapshot_as {
  local principal=$1
  kinit -k -t "testdir/${principal}.keytab" $principal
  curl -u : --negotiate -w '%{http_code}\n' \
    -o $(printf $SNAPSHOT_RESPONSE_OUTFILE $principal) \
    -v "http://$SCHEDULER_HOSTNAME:8081/api" \
    -H "Content-Type:application/vnd.apache.thrift.json" \
    --data-binary "$SNAPSHOT_RPC_DATA"
  kdestroy
}

function setup {
  cat >> $KRB5_CONFIG <<EOF
[domain_realm]
  .local = KRBTEST.COM
EOF

  aurorabuild all
  sudo cp /vagrant/examples/vagrant/upstart/aurora-scheduler-kerberos.conf \
    /etc/init/aurora-scheduler-kerberos.conf
  sudo stop aurora-scheduler || true
  sudo start aurora-scheduler-kerberos
  await_scheduler_ready

  kadmin.local -q "addprinc -randkey HTTP/$SCHEDULER_HOSTNAME"
  rm -f testdir/HTTP-$SCHEDULER_HOSTNAME.keytab.keytab
  kadmin.local -q "ktadd -keytab testdir/HTTP-$SCHEDULER_HOSTNAME.keytab HTTP/$SCHEDULER_HOSTNAME"

  kadmin.local -q "addprinc -randkey vagrant"
  rm -f testdir/vagrant.keytab
  kadmin.local -q "ktadd -keytab testdir/vagrant.keytab vagrant"

  kadmin.local -q "addprinc -randkey unpriv"
  rm -f testdir/unpriv.keytab
  kadmin.local -q "ktadd -keytab testdir/unpriv.keytab unpriv"

  kadmin.local -q "addprinc -randkey root"
  rm -f testdir/root.keytab
  kadmin.local -q "ktadd -keytab testdir/root.keytab root"
}

function test_snapshot {
  snapshot_as vagrant
  cat snapshot-response.vagrant.json
  grep -q 'lacks permission' snapshot-response.vagrant.json
  snapshot_as unpriv
  cat snapshot-response.unpriv.json
  grep -q 'lacks permission' snapshot-response.unpriv.json
  snapshot_as root
  cat snapshot-response.root.json
  grep -qv 'lacks permission' snapshot-response.root.json
}

function test_clients {
  sudo cp /vagrant/examples/vagrant/clusters_kerberos.json /etc/aurora/clusters.json

  kinit -k -t "testdir/root.keytab" root
  aurora_admin set_quota devcluster kerberos-test 0.0 0MB 0MB /dev/null 2>&1 | grep 'OK' | true
  aurora update pause devcluster/role/env/job /dev/null 2>&1 | grep 'No active update found' | true
  kdestroy
}

function tear_down {
  local retcode=$1
  sudo cp /vagrant/examples/vagrant/clusters.json /etc/aurora/clusters.json
  sudo stop aurora-scheduler-kerberos || true
  sudo rm -f /etc/init/aurora-scheduler-kerberos.conf
  sudo start aurora-scheduler || true
  if [[ $retcode -ne 0 ]]; then
    echo
    echo '!!! FAILED'
    echo
  fi
  exit $retcode
}

function main {
  if [[ "$USER" != "vagrant" ]]; then
    enter_vagrant "$@"
  elif [[ -z "${KRB5_CONFIG:-}" ]]; then
    enter_testrealm "$@"
  else
    trap 'tear_down 1' EXIT
    setup
    test_snapshot
    test_clients
    set +x
    echo
    echo '*** OK (All tests passed) ***'
    echo
    trap '' EXIT
    tear_down 0
  fi
}

main "$@"
