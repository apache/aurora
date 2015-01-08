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


function check_url_live() {
  test $(curl -sL -w '%{http_code}' $1 -o /dev/null) == 200
}

test_http_example() {
  local _cluster=$1 _role=$2 _env=$3 _job=$4 _sched_ip=$5
  local _base_config=$6 _updated_config=$7
  jobkey="$_cluster/$_role/$_env/$_job"

  # The vagrant/ssh command brings a trailing carriage return and newline, tr strips that.
  joblist=$(vagrant ssh -c "aurora config list $_base_config" | tr -dc '[[:print:]]')
  test "$joblist" = "jobs=[$jobkey]"

  vagrant ssh -c "aurora job inspect $jobkey $_base_config"

  echo '== Creating job'
  vagrant ssh -c "aurora job create $jobkey $_base_config"

  echo "== Checking job status"
  vagrant ssh -c "aurora job list $_cluster/$_role/$_env" | grep "$jobkey"
  vagrant ssh -c "aurora job status $jobkey"
  # Check that scheduler UI pages shown
  base_url="http://$_sched_ip:8081"
  check_url_live "$base_url/scheduler"
  check_url_live "$base_url/scheduler/$_role"
  check_url_live "$base_url/scheduler/$_role/$_env/$_job"

  echo "== Restarting test job"

  vagrant ssh -c "aurora job restart $jobkey"

  echo '== Updating test job'
  vagrant ssh -c "aurora job update $jobkey $_updated_config"

  echo '== Validating announce'
  validate_serverset "/aurora/$_role/$_env/$_job"

  # In order for `aurora task run` to work, the VM needs to be forwarded a local ssh identity. To avoid
  # polluting the global ssh-agent with this identity, we run this test in the context of a local
  # agent. A slightly cleaner solution would be to use a here doc (ssh-agent sh <<EOF ...), but
  # due to a strange confluence of issues, this required some unpalatable hacks. Simply putting
  # the meat of this test in a separate file seems preferable.
  ssh-agent src/test/sh/org/apache/aurora/e2e/test_run.sh $jobkey $_sched_ip
  test $? -eq 0

  # Run a kill without specifying instances, and verify that it gets an error, and the job
  # isn't affected. (TODO(maxim): the failed kill should return non-zero!)
  vagrant ssh -c "aurora job kill $jobkey" 2>&1 | grep -q "The instances list cannot be omitted in a kill command"
  check_url_live "$base_url/scheduler/$_role/$_env/$_job"

  vagrant ssh -c "aurora job kill $jobkey/1"

  vagrant ssh -c "aurora job killall $jobkey"

  vagrant ssh -c "aurora quota get $_cluster/$_role"
}

test_admin() {
  local _cluster=$1 _sched_ip=$2

  base_url="http://$_sched_ip:8081"

  echo '== Testing Aurora Admin commands...'
  echo '== Getting leading scheduler'
  vagrant ssh -c "aurora_admin get_scheduler $_cluster" | grep ":8081"
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
