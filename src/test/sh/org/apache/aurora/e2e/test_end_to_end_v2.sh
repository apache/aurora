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
  echo '== Creating job'
  vagrant ssh -c "aurora2 job create $jobkey $_base_config"

  # Check that scheduler UI pages shown
  base_url="http://$_sched_ip:8081"
  check_url_live "$base_url/scheduler"
  check_url_live "$base_url/scheduler/$_role"
  check_url_live "$base_url/scheduler/$_role/$_env/$_job"

  echo '== Updating test job'
  vagrant ssh -c "aurora2 job update $jobkey $_updated_config"

  # TODO(mchucarroll): Get "run" working: the vagrant configuration currently doesn't set up ssh
  # to allow automatic logins to the slaves. "aurora run" therefore tries to prompt the user for
  # a password, finds that it's not running in a TTY, and aborts.
  runlen=$(vagrant ssh -c "aurora2 task run $jobkey 'pwd'" | wc -l)
  test $runlen -eq 4

  vagrant ssh -c "aurora2 quota get $_cluster/$_role"
  vagrant ssh -c "aurora2 job killall  $jobkey"
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

trap collect_result EXIT
vagrant up


# wipe the pseudo-deploy dir, and then create it fresh, to guarantee that the
# test runs clean.
test_http_example "${TEST_ARGS[@]}"
RETCODE=0
