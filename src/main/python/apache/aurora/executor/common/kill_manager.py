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

from .status_checker import ExitState, StatusChecker, StatusResult


class KillManager(StatusChecker):
  """
    A health interface that provides a kill-switch for a task monitored by the status manager.
  """
  def __init__(self):
    self._killed = False
    self._reason = None

  @property
  def status(self):
    if self._killed:
      return StatusResult(self._reason, ExitState.KILLED)

  def name(self):
    return 'kill_manager'

  def kill(self, reason):
    self._reason = reason
    self._killed = True
