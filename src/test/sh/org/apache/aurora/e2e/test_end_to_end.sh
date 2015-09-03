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

# Determine if we are already in the vagrant environment.  If not, start it up and invoke the script
# from within the environment.
if [[ "$USER" != "vagrant" ]]; then
  vagrant up
  vagrant ssh -c /vagrant/src/test/sh/org/apache/aurora/e2e/test_end_to_end.sh "$@"
  exit $?
fi

set -u -e -x
set -o pipefail

readonly TEST_SCHEDULER_IP=192.168.33.7

_curl() { curl --silent --fail --retry 4 --retry-delay 10 "$@" ; }

tear_down() {
  set +x  # Disable command echo, as this makes it more difficult see which command failed.

  for job in http_example http_example_revocable http_example_docker; do
    aurora job cancel-update devcluster/vagrant/test/$job >/dev/null 2>&1
    aurora update abort devcluster/vagrant/test/$job || true >/dev/null 2>&1
    aurora job killall --no-batching devcluster/vagrant/test/$job >/dev/null 2>&1
  done
}

collect_result() {
  if [[ $RETCODE = 0 ]]
  then
    echo "OK (all tests passed)"
  else
    echo "!!! FAIL (something returned non-zero)"
    # Attempt to clean up any state we left behind.
    tear_down
  fi
  exit $RETCODE
}

check_url_live() {
  [[ $(curl -sL -w '%{http_code}' $1 -o /dev/null) == 200 ]]
}

test_version() {
  # The version number is written to stderr, making it necessary to redirect the output.
  [[ $(aurora --version 2>&1) = $(cat /vagrant/.auroraversion) ]]
}

test_config() {
  local _config=$1 _jobkey=$2

  joblist=$(aurora config list $_config | tr -dc '[[:print:]]')
  [[ "$joblist" = *"$_jobkey"* ]]
}

test_inspect() {
  local _jobkey=$1 _config=$2

  aurora job inspect $_jobkey $_config
}

test_create() {
  local _jobkey=$1 _config=$2

  aurora job create $_jobkey $_config
}

test_job_status() {
  local _cluster=$1 _role=$2 _env=$3 _job=$4
  local _jobkey="$_cluster/$_role/$_env/$_job"

  echo "== Checking job status"
  aurora job list $_cluster/$_role/$_env | grep "$_jobkey"
  aurora job status $_jobkey
}

test_scheduler_ui() {
  local _role=$1 _env=$2 _job=$3

  # Check that scheduler UI pages shown
  base_url="localhost:8081"
  check_url_live "$base_url/scheduler"
  check_url_live "$base_url/scheduler/$_role"
  check_url_live "$base_url/scheduler/$_role/$_env/$_job"
}

test_observer_ui() {
  local _cluster=$1 _role=$2 _job=$3

  # Check the observer page
  observer_url="localhost:1338"
  check_url_live "$observer_url"

  # Poll the observer, waiting for it to receive and show information about the task.
  local _success=0
  for i in $(seq 1 120); do
    task_id=$(aurora_admin query -l '%taskId%' --shards=0 --states=RUNNING $_cluster $_role $_job)
    if check_url_live "$observer_url/task/$task_id"; then
      _success=1
      break
    else
      sleep 1
    fi
  done

  if [[ "$_success" -ne "1" ]]; then
    echo "Observer task detail page is not available."
    exit 1
  fi
}

test_restart() {
  local _jobkey=$1

  aurora job restart --batch-size=2 $_jobkey
}

test_legacy_update() {
  local _jobkey=$1 _config=$2

  aurora job update $_jobkey $_config
}

assert_update_state() {
  local _jobkey=$1 _expected_state=$2

  local _state=$(aurora update list $_jobkey --status active | tail -n +2 | awk '{print $3}')
  if [[ $_state != $_expected_state ]]; then
    echo "Expected update to be in state $_expected_state, but found $_state"
    exit 1
  fi
}

test_update() {
  local _jobkey=$1 _config=$2 _cluster=$3

  aurora update start $_jobkey $_config
  assert_update_state $_jobkey 'ROLLING_FORWARD'
  local _update_id=$(aurora update list $_jobkey --status ROLLING_FORWARD \
      | tail -n +2 | awk '{print $2}')
  sudo restart aurora-scheduler
  assert_update_state $_jobkey 'ROLLING_FORWARD'
  aurora update pause $_jobkey --message='hello'
  assert_update_state $_jobkey 'ROLL_FORWARD_PAUSED'
  aurora update resume $_jobkey
  assert_update_state $_jobkey 'ROLLING_FORWARD'
  aurora update wait $_jobkey $_update_id

  # Check that the update ended in ROLLED_FORWARD state.  Assumes the status is the last column.
  local status=$(aurora update info $_jobkey $_update_id | grep 'Current status' | awk '{print $NF}')
  if [[ $status != "ROLLED_FORWARD" ]]; then
    echo "Update should have completed in ROLLED_FORWARD state"
    exit 1
  fi
}

test_announce() {
  local _role=$1 _env=$2 _job=$3

  # default python return code
  local retcode=0

  # launch aurora client in interpreter mode to get access to the kazoo client
  env SERVERSET="/aurora/$_role/$_env/$_job" PEX_INTERPRETER=1 \
    aurora /vagrant/src/test/sh/org/apache/aurora/e2e/validate_serverset.py || retcode=$?

  if [[ $retcode = 1 ]]; then
    echo "Validated announced job."
    return 0
  elif [[ $retcode = 2 ]]; then
    echo "Job failed to announce in serverset."
  elif [[ $retcode = 3 ]]; then
    echo "Job failed to re-announce when expired."
  else
    echo "Unknown failure in test script."
  fi

  exit 1

  validate_serverset "/aurora/$_jobkey"
}

test_run() {
  local _jobkey=$1

  # Create an SSH public key so that local SSH works without a password.
  local _ssh_key=~/.ssh/id_rsa
  rm -f ${_ssh_key}*
  ssh-keygen -t rsa -N "" -f $_ssh_key
  cat ${_ssh_key}.pub >> ~/.ssh/authorized_keys

  # Using the sandbox contents as a proxy for functioning SSH.  List sandbox contents, we expect
  # 3 instances of the same thing - our python script.
  sandbox_contents=$(aurora task run $_jobkey 'ls' | awk '{print $2}' | sort | uniq -c)
  echo "$sandbox_contents"
  [[ "$sandbox_contents" = "      3 http_example.py" ]]
}

test_kill() {
  local _jobkey=$1

  aurora job kill $_jobkey/1
  aurora job killall $_jobkey
}

test_quota() {
  local _cluster=$1 _role=$2

  aurora quota get $_cluster/$_role
}

test_http_example() {
  local _cluster=$1 _role=$2 _env=$3
  local _base_config=$4 _updated_config=$5
  local _job=$6
  local _jobkey="$_cluster/$_role/$_env/$_job"

  test_config $_base_config $_jobkey
  test_inspect $_jobkey $_base_config
  test_create $_jobkey $_base_config
  test_job_status $_cluster $_role $_env $_job
  test_scheduler_ui $_role $_env $_job
  test_observer_ui $_cluster $_role $_job
  test_restart $_jobkey
  test_update $_jobkey $_updated_config $_cluster
  test_announce $_role $_env $_job
  test_run $_jobkey
  test_legacy_update $_jobkey $_base_config
  test_kill $_jobkey
  test_quota $_cluster $_role
}

test_http_revocable_example() {
  local _cluster=$1 _role=$2 _env=$3
  local _base_config=$4
  local _job=$6
  local _jobkey="$_cluster/$_role/$_env/$_job"

  test_create $_jobkey $_base_config
  test_observer_ui $_cluster $_role $_job
  test_kill $_jobkey
}

test_admin() {
  local _cluster=$1
  echo '== Testing admin commands'
  echo '== Getting leading scheduler'
  aurora_admin get_scheduler $_cluster | grep ":8081"
}

restore_netrc() {
  mv ~/.netrc.bak ~/.netrc >/dev/null 2>&1 || true
}

test_basic_auth_unauthenticated() {
  local _cluster=$1 _role=$2 _env=$3
  local _config=$4
  local _job=$6
  local _jobkey="$_cluster/$_role/$_env/$_job"

  mv ~/.netrc ~/.netrc.bak
  trap restore_netrc EXIT

  aurora job create $_jobkey $_config || retcode=$?
  if [[ $retcode != 30 ]]; then
    echo "Expected auth error exit code, got $retcode"
    exit 1
  fi
  restore_netrc
}

RETCODE=1
# Set up shorthands for test
export TEST_ROOT=/vagrant/src/test/sh/org/apache/aurora/e2e
export EXAMPLE_DIR=${TEST_ROOT}/http
export DOCKER_DIR=${TEST_ROOT}/docker
TEST_CLUSTER=devcluster
TEST_ROLE=vagrant
TEST_ENV=test
TEST_JOB=http_example
TEST_JOB_REVOCABLE=http_example_revocable
TEST_JOB_DOCKER=http_example_docker
TEST_CONFIG_FILE=$EXAMPLE_DIR/http_example.aurora
TEST_CONFIG_UPDATED_FILE=$EXAMPLE_DIR/http_example_updated.aurora

BASE_ARGS=(
  $TEST_CLUSTER
  $TEST_ROLE
  $TEST_ENV
  $TEST_CONFIG_FILE
  $TEST_CONFIG_UPDATED_FILE
)

TEST_JOB_ARGS=("${BASE_ARGS[@]}" "$TEST_JOB")

TEST_JOB_REVOCABLE_ARGS=("${BASE_ARGS[@]}" "$TEST_JOB_REVOCABLE")

TEST_JOB_DOCKER_ARGS=("${BASE_ARGS[@]}" "$TEST_JOB_DOCKER")

TEST_ADMIN_ARGS=($TEST_CLUSTER)

trap collect_result EXIT

aurorabuild all
test_version
test_http_example "${TEST_JOB_ARGS[@]}"

test_http_revocable_example "${TEST_JOB_REVOCABLE_ARGS[@]}"

# build the test docker image
sudo docker build -t http_example ${TEST_ROOT}
test_http_example "${TEST_JOB_DOCKER_ARGS[@]}"

test_admin "${TEST_ADMIN_ARGS[@]}"
test_basic_auth_unauthenticated  "${TEST_JOB_ARGS[@]}"

/vagrant/src/test/sh/org/apache/aurora/e2e/test_kerberos_end_to_end.sh
RETCODE=0
