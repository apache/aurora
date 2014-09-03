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

from unittest import TestCase

from mesos.interface import mesos_pb2

from apache.aurora.executor.common.kill_manager import KillManager


class TestKillManager(TestCase):
  def setUp(self):
    self.kill_manager = KillManager()

  def test_status(self):
    reason = 'reason'
    assert self.kill_manager.status is None
    self.kill_manager.kill(reason)
    result = self.kill_manager.status
    assert result.reason == reason
    assert result.status == mesos_pb2.TaskState.Value('TASK_KILLED')

  def test_name(self):
    assert self.kill_manager.name() == 'kill_manager'

  def test_kill(self):
    reason = 'reason'
    assert self.kill_manager._killed is False
    self.kill_manager.kill(reason)
    assert self.kill_manager._reason == reason
    assert self.kill_manager._killed is True
