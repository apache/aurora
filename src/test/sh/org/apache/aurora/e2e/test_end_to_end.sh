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
  time vagrant ssh -c /vagrant/src/test/sh/org/apache/aurora/e2e/test_end_to_end.sh "$@"
  exit $?
fi

set -u -e -x
set -o pipefail

readonly TEST_SLAVE_IP=192.168.33.7

_curl() { curl --silent --fail --retry 4 --retry-delay 10 "$@" ; }

tear_down() {
  set +x  # Disable command echo, as this makes it more difficult see which command failed.

  for job in http_example http_example_watch_secs http_example_revocable http_example_docker http_example_unified_appc http_example_unified_docker; do
    aurora update abort devcluster/vagrant/test/$job >/dev/null 2>&1 || true
    aurora job killall --no-batching devcluster/vagrant/test/$job >/dev/null 2>&1
  done

  sudo mv /etc/aurora/clusters.json.old /etc/aurora/clusters.json >/dev/null 2>&1 || true
}

collect_result() {
  if [[ $RETCODE = 0 ]]
  then
    echo "OK (all tests passed)"
  else
    echo "!!! FAIL (something returned non-zero) for $BASH_COMMAND"
  fi
  # Attempt to clean up any state we left behind.
  tear_down
  exit $RETCODE
}

check_url_live() {
  [[ $(curl -sL -w '%{http_code}' $1 -o /dev/null) == 200 ]]
}

test_file_removed() {
  local _file=$1
  local _success=0
  for i in {1..10}; do
    if [[ ! -e $_file ]]; then
      _success=1
      break
    fi
    sleep 1
  done

  if [[ $_success -ne 1 ]]; then
    echo "File was not removed."
    exit 1
  fi
}

test_version() {
  # The version number is written to stderr, making it necessary to redirect the output.
  [[ $(aurora --version 2>&1) = $(cat /vagrant/.auroraversion) ]]
}

clear_mesos_maintenance() {
  curl http://"$TEST_SLAVE_IP":5050/maintenance/schedule \
    -H "Content-type: application/json" \
    -X POST \
    -d "{}"
}

test_mesos_maintenance() {
  local _cluster=$1 _role=$2 _env=$3
  local _base_config=$4
  local _job=$7
  local _jobkey="$_cluster/$_role/$_env/$_job"

  # Clear any previous maintenance schedules before running this test.
  clear_mesos_maintenance

  test_create $_jobkey $_base_config

  echo "Waiting job to enter RUNNING..."
  wait_until_task_status $_jobkey "0" "RUNNING"

  # Create the maintenance schedule
  MAINTENANCE_SCHEDULE="/tmp/maintenance_schedule.json"
  python \
  /vagrant/src/test/sh/org/apache/aurora/e2e/generate_mesos_maintenance_schedule.py > "$MAINTENANCE_SCHEDULE"
  echo "Creating maintenance with schedule"
  cat $MAINTENANCE_SCHEDULE | jq .

  curl http://"$TEST_SLAVE_IP":5050/maintenance/schedule \
    -H "Content-type: application/json" \
    -X POST \
    -d @"$MAINTENANCE_SCHEDULE"

  trap clear_mesos_maintenance EXIT

  # Posting of a maintenance schedule should not cause the task to drain right
  # away.
  assert_task_status $_jobkey "0" "RUNNING"

  # When it is drain time, it should be killed.
  echo "Waiting for time to drain tasks..."
  wait_until_task_status $_jobkey "0" "PENDING"

  clear_mesos_maintenance

  echo "Waiting for drained task to re-launch..."
  wait_until_task_status $_jobkey "0" "RUNNING"

  test_kill $_jobkey
}

test_health_check() {
  [[ $(_curl "$TEST_SLAVE_IP:8081/health") == 'OK' ]]
}

test_config() {
  local _config=$1 _jobkey=$2

  joblist=$(aurora config list $_config | tr -dc '[[:print:]]')
  [[ "$joblist" = *"$_jobkey"* ]]
}

test_inspect() {
  local _jobkey=$1 _config=$2
  shift 2
  local _extra_args="${@}"

  aurora job inspect $_jobkey $_config $_extra_args
}

test_create() {
  local _jobkey=$1 _config=$2
  shift 2
  local _extra_args="${@}"

  aurora job create $_jobkey $_config $_extra_args
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
  base_url="$TEST_SLAVE_IP:8081"
  check_url_live "$base_url/leaderhealth"
  check_url_live "$base_url/scheduler"
  check_url_live "$base_url/scheduler/$_role"
  check_url_live "$base_url/scheduler/$_role/$_env/$_job"
}

test_observer_ui() {
  local _cluster=$1 _role=$2 _job=$3

  # Check the observer page
  observer_url="$TEST_SLAVE_IP:1338"
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

  aurora job restart --batch-size=2 --watch-secs=10 $_jobkey
}

assert_update_state() {
  local _jobkey=$1 _expected_state=$2

  local _state=$(aurora update list $_jobkey --status active | tail -n +2 | awk '{print $3}')
  if [[ $_state != $_expected_state ]]; then
    echo "Expected update to be in state $_expected_state, but found $_state"
    exit 1
  fi
}

assert_task_status() {
  local _jobkey=$1 _id=$2 _expected_state=$3

  local _state=$(aurora job status $_jobkey --write-json | jq -r ".[0].active[$_id].status")

  if [[ $_state != $_expected_state ]]; then
    echo "Expected task to be in state $_expected_state, but found $_state"
    exit 1
  fi
}

wait_until_task_status() {
  # Poll the task, waiting for it to enter the target state
  local _jobkey=$1 _id=$2 _expected_state=$3
  local _state=""
  local _success=0

  for i in $(seq 1 120); do
    _state=$(aurora job status $_jobkey --write-json | jq -r ".[0].active[$_id].status")
    if [[ $_state == $_expected_state ]]; then
      _success=1
      break
    else
      sleep 1
    fi
  done

  if [[ "$_success" -ne "1" ]]; then
    echo "Task did not transition to $_expected_state within two minutes."
    exit 1
  fi
}

test_update() {
  local _jobkey=$1 _config=$2 _cluster=$3
  shift 3
  local _extra_args="${@}"

  aurora update start $_jobkey $_config $_extra_args
  assert_update_state $_jobkey 'ROLLING_FORWARD'
  local _update_id=$(aurora update list $_jobkey --status ROLLING_FORWARD \
      | tail -n +2 | awk '{print $2}')
  aurora_admin scheduler_snapshot devcluster
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

test_update_fail() {
  local _jobkey=$1 _config=$2 _cluster=$3  _bad_healthcheck_config=$4
  shift 4
  local _extra_args="${@}"

  # Make sure our updates works.
  aurora update start $_jobkey $_config $_extra_args
  assert_update_state $_jobkey 'ROLLING_FORWARD'
  local _update_id=$(aurora update list $_jobkey --status ROLLING_FORWARD \
      | tail -n +2 | awk '{print $2}')
  # Need to wait until udpate finishes before we can start one that we want to fail.
  aurora update wait $_jobkey $_update_id

  # Starting update with a health check that is meant to fail. Expected behavior is roll back.
  aurora update start $_jobkey $_bad_healthcheck_config $_extra_args
  local _update_id=$(aurora update list $_jobkey --status active \
      | tail -n +2 | awk '{print $2}')
  # || is so that we don't return an EXIT so that `trap collect_result` doesn't get triggered.
  aurora update wait $_jobkey $_update_id || echo $?
  # Making sure we rolled back.
  local status=$(aurora update info $_jobkey $_update_id | grep 'Current status' | awk '{print $NF}')
  if [[ $status != "ROLLED_BACK" ]]; then
    echo "Update should have completed in ROLLED_BACK state due to failed healthcheck."
    exit 1
  fi
}

test_partition_awareness() {
  local _config=$1 _cluster=$2 _default_jobkey=$3 _disabled_jobkey=$4 _delay_jobkey=$5

  # create three jobs with different partition policies
  aurora update start --wait $_default_jobkey $_config
  aurora update start --wait $_disabled_jobkey $_config
  aurora update start --wait $_delay_jobkey $_config

  # partition the agent
  sudo stop mesos-slave

  # the default job should become LOST and then transition to PENDING
  wait_until_task_status $_default_jobkey "0" "PENDING"

  # the other two should be PARTITIONED
  assert_task_status $_disabled_jobkey "0" "PARTITIONED"
  assert_task_status $_delay_jobkey "0" "PARTITIONED"

  # start the agent back up
  sudo start mesos-slave

  # This can be removed when https://issues.apache.org/jira/browse/MESOS-6406 is resolved.
  # We have to pause and let the agent reregister with Mesos, then ask Aurora to explicitly
  # reconcile to get the RUNNING status update.
  sleep 30
  aurora_admin reconcile_tasks $_cluster

  # the PARTITIONED tasks should now be running
  assert_task_status $_disabled_jobkey "0" "RUNNING"
  assert_task_status $_delay_jobkey "0" "RUNNING"

  # Clean up
  aurora job killall $_default_jobkey
  aurora job killall $_disabled_jobkey
  aurora job killall $_delay_jobkey
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

setup_ssh() {
  # Create an SSH public key so that local SSH works without a password.
  local _ssh_key=~/.ssh/id_rsa
  rm -f ${_ssh_key}*
  ssh-keygen -t rsa -N "" -f $_ssh_key
  # Ensure a new line for the new key to start on.
  # See: https://issues.apache.org/jira/browse/AURORA-1728
  echo >> ~/.ssh/authorized_keys
  cat ${_ssh_key}.pub >> ~/.ssh/authorized_keys
}

test_run() {
  local _jobkey=$1

  # Using the sandbox contents as a proxy for functioning SSH.  List sandbox contents, looking for
  # the .logs directory. We expect to find 3 instances.
  sandbox_contents=$(aurora task run $_jobkey 'ls -a' | awk '{print $2}' | grep ".logs" | sort | uniq -c)
  echo "$sandbox_contents"
  [[ "$sandbox_contents" = "      3 .logs" ]]
}

test_scp_success() {
  local _jobkey=$1/0
  local _filename=scp_success.txt
  local _expected_return="      1 scp_success.txt"

  # Unset because grep can return 1 if the file does not exist
  set +e

  # Ensure file does not exists before scp
  pre_sandbox_contents=$(aurora task run $_jobkey "ls" | awk '{print $2}' | grep ${_filename} | sort | uniq -c)
  [[ "$pre_sandbox_contents" != $_expected_return ]]

  # Reset -e after command has been run
  set -e

  # Create a file and move it to the sandbox of a job
  touch $_filename
  aurora task scp $_filename ${_jobkey}:
  sandbox_contents=$(aurora task run $_jobkey "ls" | awk '{print $2}' | grep ${_filename} | sort | uniq -c)
  [[ "$sandbox_contents" == $_expected_return ]]
}

test_scp_permissions() {
  local _jobkey=$1/0
  local _filename=scp_fail_permission.txt
  local _retcode=0
  local _sandbox_contents
  # Create a file and try to move it, ensure we get permission denied
  touch $_filename

  # Unset because we are expecting an error
  set +e

  _sandbox_contents=$(aurora task scp $_filename ${_jobkey}:../ 2>&1 > /dev/null)
  _retcode=$?

  # Reset -e after command has been run
  set -e

  if [[ "$_retcode" != 1 ]]; then
    echo "Permission to exit chroot jail given when should have failed"
    exit 1
  fi
  if [[ "$_sandbox_contents" != *"../scp_fail_permission.txt: Permission denied"* ]]; then
    echo "Unexpected response from invalid scp command"
    exit 1
  fi
}

test_kill() {
  local _jobkey=$1
  shift 1
  local _extra_args="${@}"

  aurora job kill $_jobkey/1 $_extra_args
  aurora job killall $_jobkey $_extra_args
}

test_quota() {
  local _cluster=$1 _role=$2

  aurora quota get $_cluster/$_role
}

test_discovery_info() {
  local _task_id_prefix=$1
  local _discovery_name=$2

  if ! [[ -x "$(command -v jq)" ]]; then
    echo "jq is not installed, skipping discovery info test"
    return 0
  fi

  framework_info=$(curl --silent '192.168.33.7:5050/state' | jq '.frameworks | map(select(.name == "Aurora"))')
  if [[ -z $framework_info ]]; then
    echo "Cannot get framework info for $framework"
    exit 1
  fi

  task_info=$(echo $framework_info | jq --arg task_id_prefix "${_task_id_prefix}" '.[0]["tasks"] | map(select(.id | contains($task_id_prefix)))')
  if [[ -z $task_info ]]; then
    echo "Cannot get task blob json for task id prefix ${_task_id_prefix}"
    exit 1
  fi

  discovery_info=$(echo $task_info | jq '.[0]["discovery"]')
  if [[ -z $discovery_info ]]; then
    echo "Cannot get discovery info json from task blob ${task_blob}"
    exit 1
  fi

  name=$(echo $discovery_info | jq '.["name"]')
  if [[ "$name" -ne "\"$_discovery_name\"" ]]; then
    echo "discovery info name $name does not equal to expected \"$_discovery_name\""
    exit 1
  fi

  num_ports=$(echo $discovery_info | jq '.["ports"]["ports"] | length')

  if ! [[ "$num_ports" -gt 0 ]]; then
    echo "num of ports in discovery info is $num_ports which is not greater than zero"
    exit 1
  fi
}

test_thermos_profile() {
  read_env_output=$(aurora task ssh $_jobkey/0 --command='tail -1 .logs/read_env/0/stdout' |tr -d '\r\n' 2>/dev/null)
  echo "$read_env_output"
  [[ "$read_env_output" = "hello" ]]
}

test_http_example() {
  local _cluster=$1 _role=$2 _env=$3
  local _base_config=$4 _updated_config=$5
  local _bad_healthcheck_config=$6
  local _job=$7
  local _bind_parameters=${8:-""}

  local _jobkey="$_cluster/$_role/$_env/$_job"
  local _task_id_prefix="${_role}-${_env}-${_job}-0"
  local _discovery_name="${_job}.${_env}.${_role}"

  test_config $_base_config $_jobkey
  test_inspect $_jobkey $_base_config $_bind_parameters
  test_create $_jobkey $_base_config $_bind_parameters
  test_job_status $_cluster $_role $_env $_job
  test_scheduler_ui $_role $_env $_job
  test_observer_ui $_cluster $_role $_job
  test_discovery_info $_task_id_prefix $_discovery_name
  test_thermos_profile $_jobkey
  test_file_mount $_cluster $_role $_env $_job
  test_restart $_jobkey
  test_update $_jobkey $_updated_config $_cluster $_bind_parameters
  test_update_fail $_jobkey $_base_config  $_cluster $_bad_healthcheck_config $_bind_parameters
  # Running test_update second time to change state to success.
  test_update $_jobkey $_updated_config $_cluster $_bind_parameters
  test_announce $_role $_env $_job
  test_run $_jobkey
  # TODO(AURORA-1926): 'aurora task scp' only works fully on Mesos containers (can only read for
  # Docker). See if it is possible to enable write for Docker sandboxes as well then remove the
  # 'if' guard below.
  if [[ $_job != *"docker"* ]]; then
    test_scp_success $_jobkey
    test_scp_permissions $_jobkey
  fi
  test_kill $_jobkey
  test_quota $_cluster $_role
}

test_http_example_basic() {
  local _cluster=$1 _role=$2 _env=$3
  local _base_config=$4
  local _job=$7
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

  # host maintenance commands currently have a separate entry point and use their own api client.
  # Until we address that, at least verify that the command group still works.
  aurora_admin host_status --hosts=$TEST_SLAVE_IP $_cluster
}

test_ephemeral_daemon_with_final() {
  local _cluster=$1 _role=$2 _env=$3 _job=$4 _config=$5
  local _jobkey="$_cluster/$_role/$_env/$_job"
  local _stop_file=$(mktemp)
  local _extra_args="--bind stop_file=$_stop_file"
  rm $_stop_file

  test_create $_jobkey $_config $_extra_args
  test_observer_ui $_cluster $_role $_job
  test_job_status $_cluster $_role $_env $_job
  touch $_stop_file  # Stops 'main_process'.
  test_file_removed $_stop_file  # Removed by 'final_process'.
}

test_daemonizing_process() {
  local _cluster=$1 _role=$2 _env=$3 _job=$4 _config=$5
  local _jobkey="$_cluster/$_role/$_env/$_job"
  local _term_file=$(mktemp)
  local _extra_args="--bind term_file=$_term_file"

  test_create $_jobkey $_config $_extra_args
  test_observer_ui $_cluster $_role $_job
  test_job_status $_cluster $_role $_env $_job
  test_kill $_jobkey
  test_file_removed $_term_file
}

restore_netrc() {
  mv ~/.netrc.bak ~/.netrc >/dev/null 2>&1 || true
}

test_basic_auth_unauthenticated() {
  local _cluster=$1 _role=$2 _env=$3
  local _config=$4
  local _job=$7
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

setup_image_stores() {
  TEMP_PATH=$(mktemp -d)
  pushd "$TEMP_PATH"

  # build the docker image and save it as a tarball.
  sudo docker build -t http_example_netcat -f "${TEST_ROOT}/Dockerfile.netcat" ${TEST_ROOT}
  docker save -o http_example_netcat-latest.tar http_example_netcat

  DOCKER_IMAGE_DIRECTORY="/tmp/mesos/images/docker"
  sudo mkdir -p "$DOCKER_IMAGE_DIRECTORY"
  sudo cp http_example_netcat-latest.tar "$DOCKER_IMAGE_DIRECTORY/http_example_netcat:latest.tar"

  # build the appc image from the docker image
  docker2aci http_example_netcat-latest.tar

  APPC_IMAGE_ID="sha512-$(sha512sum http_example_netcat-latest.aci | awk '{print $1}')"
  export APPC_IMAGE_ID
  APPC_IMAGE_DIRECTORY="/tmp/mesos/images/appc/images/$APPC_IMAGE_ID"

  sudo mkdir -p "$APPC_IMAGE_DIRECTORY"
  sudo tar -xf http_example_netcat-latest.aci -C "$APPC_IMAGE_DIRECTORY"
  # This restart is necessary for mesos to pick up the image from the local store.
  sudo restart mesos-slave

  popd
  rm -rf "$TEMP_PATH"
}

setup_docker_registry() {
  # build the test docker image
  sudo docker build -t http_example -f "${TEST_ROOT}/Dockerfile.python" ${TEST_ROOT}
  docker tag http_example:latest aurora.local:5000/http_example:latest
  docker login -p testpassword -u testuser http://aurora.local:5000
  docker push aurora.local:5000/http_example:latest
  sudo mv /etc/aurora/clusters.json /etc/aurora/clusters.json.old
  sudo sh -c "cat /etc/aurora/clusters.json.old | jq 'map(. + {docker_registry:\"http://aurora.local:5000\"})' > /etc/aurora/clusters.json"
}

test_appc_unified() {
  num_mounts_before=$(mount |wc -l |tr -d '\n')

  TEST_JOB_APPC_ARGS=("${BASE_ARGS[@]}" "$TEST_JOB_UNIFIED_APPC" "--bind appc_image_id=$APPC_IMAGE_ID")
  test_http_example "${TEST_JOB_APPC_ARGS[@]}"

  num_mounts_after=$(mount |wc -l |tr -d '\n')
  # We want to be sure that running the isolated task did not leak any mounts.
  [[ "$num_mounts_before" = "$num_mounts_after" ]]
}

test_file_mount() {
  local _cluster=$1 _role=$2 _env=$3 _job=$4

  if [[ "$_job" = "$TEST_JOB_UNIFIED_DOCKER" ]]; then
    local _jobkey="$_cluster/$_role/$_env/$_job"

    verify_file_mount_output=$(aurora task ssh $_jobkey/0 --command='tail -1 .logs/verify_file_mount/0/stdout' |tr -d '\r\n' 2>/dev/null)
    echo "$verify_file_mount_output"
    [[ "$verify_file_mount_output" = "$(cat /vagrant/.auroraversion |tr -d '\r\n')" ]]
    return $?
  fi

  return 0
}

test_docker_unified() {
  num_mounts_before=$(mount |wc -l |tr -d '\n')

  TEST_JOB_DOCKER_ARGS=("${BASE_ARGS[@]}" "$TEST_JOB_UNIFIED_DOCKER")
  test_http_example "${TEST_JOB_DOCKER_ARGS[@]}"

  num_mounts_after=$(mount |wc -l |tr -d '\n')
  # We want to be sure that running the isolated task did not leak any mounts.
  [[ "$num_mounts_before" = "$num_mounts_after" ]]
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
TEST_MAINTENANCE_JOB=http_example_maintenance
TEST_JOB_WATCH_SECS=http_example_watch_secs
TEST_JOB_REVOCABLE=http_example_revocable
TEST_JOB_GPU=http_example_gpu
TEST_JOB_DOCKER=http_example_docker
TEST_JOB_UNIFIED_APPC=http_example_unified_appc
TEST_JOB_UNIFIED_DOCKER=http_example_unified_docker
TEST_CONFIG_FILE=$EXAMPLE_DIR/http_example.aurora
TEST_CONFIG_UPDATED_FILE=$EXAMPLE_DIR/http_example_updated.aurora
TEST_BAD_HEALTHCHECK_CONFIG_UPDATED_FILE=$EXAMPLE_DIR/http_example_bad_healthcheck.aurora
TEST_EPHEMERAL_DAEMON_WITH_FINAL_JOB=ephemeral_daemon_with_final
TEST_EPHEMERAL_DAEMON_WITH_FINAL_CONFIG_FILE=$TEST_ROOT/ephemeral_daemon_with_final.aurora
TEST_DAEMONIZING_PROCESS_JOB=daemonize
TEST_DAEMONIZING_PROCESS_CONFIG_FILE=$TEST_ROOT/test_daemonizing_process.aurora
TEST_PARTITION_AWARENESS_CONFIG_FILE=$TEST_ROOT/partition_aware.aurora
TEST_JOB_PA_DEFAULT=$TEST_CLUSTER/$TEST_ROLE/$TEST_ENV/partition_aware_default
TEST_JOB_PA_DISABLED=$TEST_CLUSTER/$TEST_ROLE/$TEST_ENV/partition_aware_disabled
TEST_JOB_PA_DELAY=$TEST_CLUSTER/$TEST_ROLE/$TEST_ENV/partition_aware_delay

BASE_ARGS=(
  $TEST_CLUSTER
  $TEST_ROLE
  $TEST_ENV
  $TEST_CONFIG_FILE
  $TEST_CONFIG_UPDATED_FILE
  $TEST_BAD_HEALTHCHECK_CONFIG_UPDATED_FILE
)

TEST_JOB_ARGS=("${BASE_ARGS[@]}" "$TEST_JOB")

TEST_MAINTENANCE_JOB_ARGS=("${BASE_ARGS[@]}" "$TEST_MAINTENANCE_JOB")

TEST_JOB_WATCH_SECS_ARGS=("${BASE_ARGS[@]}" "$TEST_JOB_WATCH_SECS")

TEST_JOB_REVOCABLE_ARGS=("${BASE_ARGS[@]}" "$TEST_JOB_REVOCABLE")

TEST_JOB_GPU_ARGS=("${BASE_ARGS[@]}" "$TEST_JOB_GPU")

TEST_JOB_DOCKER_ARGS=("${BASE_ARGS[@]}" "$TEST_JOB_DOCKER")

TEST_ADMIN_ARGS=($TEST_CLUSTER)

TEST_JOB_EPHEMERAL_DAEMON_WITH_FINAL_ARGS=(
  $TEST_CLUSTER
  $TEST_ROLE
  $TEST_ENV
  $TEST_EPHEMERAL_DAEMON_WITH_FINAL_JOB
  $TEST_EPHEMERAL_DAEMON_WITH_FINAL_CONFIG_FILE
)

TEST_DAEMONIZING_PROCESS_ARGS=(
  $TEST_CLUSTER
  $TEST_ROLE
  $TEST_ENV
  $TEST_DAEMONIZING_PROCESS_JOB
  $TEST_DAEMONIZING_PROCESS_CONFIG_FILE
)

TEST_PARTITION_AWARENESS_ARGS=(
  $TEST_PARTITION_AWARENESS_CONFIG_FILE
  $TEST_CLUSTER
  $TEST_JOB_PA_DEFAULT
  $TEST_JOB_PA_DISABLED
  $TEST_JOB_PA_DELAY
)

TEST_JOB_KILL_MESSAGE_ARGS=("${TEST_JOB_ARGS[@]}" "--message='Test message'")

trap collect_result EXIT

aurorabuild all
setup_ssh
setup_docker_registry

test_partition_awareness "${TEST_PARTITION_AWARENESS_ARGS[@]}"

test_version
test_http_example "${TEST_JOB_ARGS[@]}"
test_http_example "${TEST_JOB_WATCH_SECS_ARGS[@]}"
test_health_check

test_mesos_maintenance "${TEST_MAINTENANCE_JOB_ARGS[@]}"

test_http_example_basic "${TEST_JOB_REVOCABLE_ARGS[@]}"

test_http_example_basic "${TEST_JOB_GPU_ARGS[@]}"

test_http_example_basic "${TEST_JOB_KILL_MESSAGE_ARGS[@]}"

test_http_example "${TEST_JOB_DOCKER_ARGS[@]}"

setup_image_stores
test_appc_unified
test_docker_unified

test_admin "${TEST_ADMIN_ARGS[@]}"
test_basic_auth_unauthenticated  "${TEST_JOB_ARGS[@]}"

test_ephemeral_daemon_with_final "${TEST_JOB_EPHEMERAL_DAEMON_WITH_FINAL_ARGS[@]}"

test_daemonizing_process "${TEST_DAEMONIZING_PROCESS_ARGS[@]}"

/vagrant/src/test/sh/org/apache/aurora/e2e/test_kerberos_end_to_end.sh
/vagrant/src/test/sh/org/apache/aurora/e2e/test_bypass_leader_redirect_end_to_end.sh
RETCODE=0
