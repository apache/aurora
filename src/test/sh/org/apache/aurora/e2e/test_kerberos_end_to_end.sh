#!/bin/bash
set -eux

readonly KRB5_MAJOR_MINOR=1.13
readonly KRB5_VERSION=1.13.1
readonly KRB5_URL_BASE=http://web.mit.edu/kerberos/dist/krb5/
readonly KRB5_SIGNED_TARBALL=krb5-$KRB5_VERSION-signed.tar
readonly KRB5_TARBALL=krb5-$KRB5_VERSION.tar.gz
readonly KRB5_KEY_ID=0x749D7889

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
  while ! curl -s localhost:8081/vars | grep framework_registered; do
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
    -s 'http://localhost:8081/api' \
    --data-binary "$SNAPSHOT_RPC_DATA"
  kdestroy
}

function test_snapshot {
  cat >> $KRB5_CONFIG <<EOF
[domain_realm]
  .local = KRBTEST.COM
EOF
  kadmin.local -q "addprinc -randkey HTTP/localhost"
  rm -f testdir/HTTP-localhost.keytab
  kadmin.local -q "ktadd -keytab testdir/HTTP-localhost.keytab HTTP/localhost"

  kadmin.local -q "addprinc -randkey vagrant"
  rm -f testdir/vagrant.keytab
  kadmin.local -q "ktadd -keytab testdir/vagrant.keytab vagrant"

  kadmin.local -q "addprinc -randkey unpriv"
  rm -f testdir/unpriv.keytab
  kadmin.local -q "ktadd -keytab testdir/unpriv.keytab unpriv"

  kadmin.local -q "addprinc -randkey root"
  rm -f testdir/root.keytab
  kadmin.local -q "ktadd -keytab testdir/root.keytab root"

  sudo cp /vagrant/examples/vagrant/upstart/aurora-scheduler-kerberos.conf \
    /etc/init/aurora-scheduler-kerberos.conf
  aurorabuild scheduler
  sudo stop aurora-scheduler || true
  sudo start aurora-scheduler-kerberos
  await_scheduler_ready
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

function tear_down {
  sudo stop aurora-scheduler-kerberos || true
  sudo rm -f /etc/init/aurora-scheduler-kerberos.conf
  sudo start aurora-scheduler || true
}

function main {
  if [[ "$USER" != "vagrant" ]]; then
    enter_vagrant "$@"
  elif [[ -z "${KRB5_CONFIG:-}" ]]; then
    enter_testrealm "$@"
  else
    trap tear_down EXIT
    test_snapshot
    set +x
    echo
    echo '*** OK (All tests passed) ***'
    echo
  fi
}

main "$@"
