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

set -o nounset

# Tests the basic functions described in docs/tutorial.md
# Note that this script has copy-pasted contents of hello_world.py
# and hello_world.aurora. These files must be manually kept in sync.

function require_healthy {
  echo "Checking $1 health...\c"
  response=$(curl -sL -w "%{http_code}\\n" "http://$2/health" -o /dev/null)
  if [ "$response" == '200' ]
  then
    echo 'PASS'
  else
    echo 'FAIL'
    echo "The $1 did not respond to a health check, is it running?"
    exit 1
  fi
}

function write_test_files {
  cat > hello_world.py <<EOF
import time

def main(argv):
  SLEEP_DELAY = 10
  # Python ninjas - ignore this blatant bug.
  for i in xrang(100):
    print("Hello world! The time is now: %s. Sleeping for %d secs" % (
      time.asctime(), SLEEP_DELAY))
    time.sleep(SLEEP_DELAY)

if __name__ == "__main__":
  main()
EOF

  cat > hello_world.aurora <<EOF
pkg_path = '/vagrant/hello_world.py'

# we use a trick here to make the configuration change with
# the contents of the file, for simplicity.  in a normal setting, packages would be
# versioned, and the version number would be changed in the configuration.
import hashlib
with open(pkg_path, 'rb') as f:
  pkg_checksum = hashlib.md5(f.read()).hexdigest()

# copy hello_world.py into the local sandbox
install = Process(
  name = 'fetch_package',
  cmdline = 'cp %s . && echo %s && chmod +x hello_world.py' % (pkg_path, pkg_checksum))

# run the script
hello_world = Process(
  name = 'hello_world',
  cmdline = 'python -u hello_world.py')

# describe the task
hello_world_task = SequentialTask(
  processes = [install, hello_world],
  resources = Resources(cpu = 1, ram = 1*MB, disk=8*MB))

jobs = [
  Service(cluster = 'devcluster',
          environment = 'devel',
          role = 'www-data',
          name = 'hello_world',
          task = hello_world_task)
]
EOF
}

function aurora_command {
  echo "Running $1 command...\c"
  result=$(vagrant ssh -c "aurora $2" 2>&1)
  if [ $? -eq 0 ]
  then
    echo 'PASS'
  else
    echo 'FAIL'
    echo "Command output:\n$result"
    exit 1
  fi
}

function await_task_in_state {
  echo "Waiting to observe $2 task...\c"
  for i in $(seq 0 5)
  do
    # TODO(wfarner): This check is not that great, since it will detect any task
    # in the job, rather a newly-failed task.
    result=$(vagrant ssh -c "aurora job status $1 2>&1 | grep 'status: $2'" 2>&1)
    if [ $? -eq 0 ]
    then
      echo 'PASS'
      return
    else
      sleep 2
    fi
  done
  echo 'FAIL'
  echo "Command output:\n$result"
}

require_healthy scheduler 192.168.33.7:8081
require_healthy observer 192.168.33.7:1338
require_healthy master 192.168.33.7:5050/master
require_healthy slave '192.168.33.7:5051/slave(1)'

# Move to repository root
cd "$(git rev-parse --show-toplevel)"

JOB_KEY=devcluster/www-data/devel/hello_world

write_test_files
aurora_command create "job create $JOB_KEY /vagrant/hello_world.aurora"
await_task_in_state $JOB_KEY FAILED

# Fix the bug in our test script.
perl -pi -e 's|xrang\(|xrange\(|g' hello_world.py
aurora_command update "update start $JOB_KEY /vagrant/hello_world.aurora"
await_task_in_state $JOB_KEY RUNNING

aurora_command killall "job killall $JOB_KEY"

exit 0
