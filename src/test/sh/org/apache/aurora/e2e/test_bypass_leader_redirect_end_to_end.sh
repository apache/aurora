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
# An integration test for the bypassing the leader redirect filter, using the vagrant environment as
# a testbed.
set -eux

function enter_vagrant {
  exec vagrant ssh -- \
    /vagrant/src/test/sh/org/apache/aurora/e2e/test_bypass_leader_redirect_end_to_end.sh "$@"
}

function await_scheduler_ready {
  while ! curl -s localhost:8081/vars | grep "scheduler_lifecycle_LEADER_AWAITING_REGISTRATION 1"; do
    sleep 3
  done
}

function setup {
  aurorabuild all
  sudo cp /vagrant/examples/vagrant/clusters_direct.json /etc/aurora/clusters.json
  sudo systemctl stop mesos-master || true
  sudo systemctl restart aurora-scheduler
  await_scheduler_ready
}

function test_bypass_leader_redirect {
  aurora_admin query --bypass-leader-redirect devcluster vagrant http_example
}

function tear_down {
  local retcode=$1
  sudo cp /vagrant/examples/vagrant/clusters.json /etc/aurora/clusters.json
  sudo systemctl start mesos-master || true
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
  else
    trap 'tear_down 1' EXIT
    setup
    test_bypass_leader_redirect
    set +x
    echo
    echo '*** OK (All tests passed) ***'
    echo
    trap '' EXIT
    tear_down 0
  fi
}

main "$@"
