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

from apache.aurora.client.api.scheduler_client import SchedulerProxy


class FakeSchedulerProxy(SchedulerProxy):
  def __init__(self, cluster, scheduler, session_key):
    self._cluster = cluster
    self._scheduler = scheduler
    self._session_key = session_key

  def client(self):
    return self._scheduler

  def session_key(self):
    return self._session_key
