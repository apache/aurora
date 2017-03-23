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


# This generates a mesos maintnenace schedule for draining the only host in two
# minutes.
import json
import time

# Get the current time + two minutes and convert it into nanos
start_secs = int(time.time() + 120)
start_ns = start_secs * 10**9

machine_id = {
        "hostname": "192.168.33.7",
        "ip": "192.168.33.7"
}

unavailability = {
        "start": {"nanoseconds": start_ns}
        }

window = {
        "machine_ids": [machine_id],
        "unavailability": unavailability
        }

schedule = {
        "windows": [window]
}

print json.dumps(schedule)
