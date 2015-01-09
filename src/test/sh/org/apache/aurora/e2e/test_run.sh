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
# Test aurora run/aurora task run. This script is meant to be run within an ssh-agent to
# avoid polluting the global ssh-agent w/ the vagrant private key.

set -u -e -x

jobkey=$1
scheduler_ip=$2
aurora_command=${3:-"aurora task run"}

ssh-add $HOME/.vagrant.d/insecure_private_key
vagrant ssh -c "$aurora_command $jobkey 'pwd'" -- -A -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
  | grep "^$scheduler_ip" | awk '{print $2}' | cut -c 1-14 \
  | while read line; do
    test "$line" = "/var/lib/mesos"
  done
