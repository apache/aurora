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

import time
from unittest import TestCase

import mock
from mesos.interface.mesos_pb2 import TaskState

from apache.aurora.executor.common.status_checker import StatusChecker, StatusResult
from apache.aurora.executor.status_manager import StatusManager


class FakeStatusChecker(StatusChecker):
  def __init__(self, status):
    self.call_count = 0
    self._status = status

  @property
  def status(self):
    if self.call_count == 2:
      return StatusResult('Fake reason', TaskState.Value('TASK_KILLED'))
    self.call_count += 1
    return self._status


class TestStatusManager(TestCase):
  def setUp(self):
    self.unhealthy_callback_called = False
    self.running_callback_called = 0

  def test_run_with_none_status(self):
    self.do_test_run_with_status(None, 0)

  def test_run_with_starting_status(self):
    self.do_test_run_with_status(StatusResult(None, TaskState.Value('TASK_STARTING')), 0)

  def test_run_with_running_status(self):
    self.do_test_run_with_status(StatusResult(None, TaskState.Value('TASK_RUNNING')), 1)

  def do_test_run_with_status(self, status_result, expected_running_callback_call_count):
    checker = FakeStatusChecker(status_result)
    def unhealthy_callback(result):
      assert result == StatusResult('Fake reason', TaskState.Value('TASK_KILLED'))
      self.unhealthy_callback_called = True
    def running_callback(result):
      assert result == StatusResult(None, TaskState.Value('TASK_RUNNING'))
      self.running_callback_called += 1
    mock_time = mock.create_autospec(spec=time, instance=True)
    status_manager = StatusManager(checker, running_callback, unhealthy_callback, mock_time)
    status_manager.run()
    assert mock_time.sleep.call_count == 2
    assert self.unhealthy_callback_called is True
    assert self.running_callback_called == expected_running_callback_call_count
