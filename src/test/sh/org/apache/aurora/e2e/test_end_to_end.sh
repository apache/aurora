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
# A simple integration test for the mesos client, intended to be run  before checkin of major
#client changes, and as a part of an integrated build process.
#
# This test uses the vagrant demonstration environment. It loads up a virtual cluster, and then
# launches a job, verifies that it's running, updates it, verifies that the update succeeded,
# and then kills the job.

set -u -e -x

. src/test/sh/org/apache/aurora/e2e/test_common.sh

test_http_example() {
  local _cluster=$1 _role=$2 _env=$3 _job=$4 _sched_ip=$5
  local _base_config=$6 _updated_config=$7
  jobkey="$_cluster/$_role/$_env/$_job"

  echo '== Creating job'
  vagrant ssh -c "aurora create $jobkey $_base_config"

  # Check that scheduler /vars being exported
  base_url="http://$_sched_ip:8081"
  uptime=$(_curl -s "$base_url/vars" | grep jvm_uptime_secs | wc -l)
  test $uptime -eq 1

  echo '== Updating test job'
  vagrant ssh -c "aurora update $jobkey $_updated_config"

  echo '== Validating announce'
  validate_serverset "/aurora/$_role/$_env/$_job"

  echo "== Probing job via 'aurora run'"
  # In order for `aurora run` to work, the VM needs to be forwarded a local ssh identity. To avoid
  # polluting the global ssh-agent with this identity, we run this test in the context of a local
  # agent. A slightly cleaner solution would be to use a here doc (ssh-agent sh <<EOF ...), but
  # due to a strange confluence of issues, this required some unpalatable hacks. Simply putting
  # the meat of this test in a separate file seems preferable.
  ssh-agent src/test/sh/org/apache/aurora/e2e/test_run.sh $jobkey $_sched_ip "aurora run"
  test $? -eq 0

  vagrant ssh -c "aurora get_quota --cluster=$_cluster $_role"
  vagrant ssh -c "aurora killall  $jobkey"
}

test_admin() {
  local _cluster=$1 _sched_ip=$2

  base_url="http://$_sched_ip:8081"

  echo '== Testing Aurora Admin commands...'
  echo '== Getting leading scheduler'
  vagrant ssh -c "aurora_admin get_scheduler $_cluster" | grep "$base_url"
}

RETCODE=1
# Set up shorthands for test
export EXAMPLE_DIR=/vagrant/src/test/sh/org/apache/aurora/e2e/http
TEST_CLUSTER=devcluster
TEST_ROLE=vagrant
TEST_ENV=test
TEST_JOB=http_example
TEST_SCHEDULER_IP=192.168.33.7
TEST_ARGS=(
  $TEST_CLUSTER
  $TEST_ROLE
  $TEST_ENV
  $TEST_JOB
  $TEST_SCHEDULER_IP
  $EXAMPLE_DIR/http_example.aurora
  $EXAMPLE_DIR/http_example_updated.aurora
  )

TEST_ADMIN_ARGS=(
  $TEST_CLUSTER
  $TEST_SCHEDULER_IP
)

trap collect_result EXIT
vagrant up
vagrant ssh -c "aurorabuild all"

# wipe the pseudo-deploy dir, and then create it fresh, to guarantee that the
# test runs clean.
test_http_example "${TEST_ARGS[@]}"
test_admin "${TEST_ADMIN_ARGS[@]}"
RETCODE=0
