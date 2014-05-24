#!/bin/bash -x
#
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
# Common utility functions used by different variants of the
# end to end tests.

# Preserve original stderr so output from signal handlers doesn't get redirected to /dev/null.
exec 4>&2

_curl() { curl --silent --fail --retry 4 --retry-delay 10 "$@" ; }

collect_result() {
  (
    if [[ $RETCODE = 0 ]]
    then
      echo "***"
      echo "OK (all tests passed)"
      echo "***"
    else
      echo "!!!"
      echo "FAIL (something returned non-zero)"
      echo ""
      echo "This may be a transient failure (as in scheduler failover) or it could be a real issue"
      echo "with your code. Either way, this script DNR merging to master. Note you may need to"
      echo "reconcile state manually."
      echo "!!!"
      vagrant ssh -c "aurora killall devcluster/vagrant/test/http_example"
    fi
    exit $RETCODE
  ) >&4 # Send to the stderr we had at startup.
}
