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

import mock
from mesos.interface import ExecutorDriver, mesos_pb2
from twitter.common import log

from apache.aurora.executor.executor_base import ExecutorBase


class TestExecutorBase(TestCase):
  def setUp(self):
    self.executor_base = ExecutorBase()

  def test_status_is_terminal(self):
    for terminal_status in ExecutorBase.TERMINAL_STATES:
      assert ExecutorBase.status_is_terminal(terminal_status)
    assert not ExecutorBase.status_is_terminal('RUNNING')
    assert not ExecutorBase.status_is_terminal('BASSCANNON')

  @mock.patch('twitter.common.log.info', spec=log.info)
  def test_log(self, mock_info):
    test_message = 'testing'
    self.executor_base.log(test_message)
    mock_info.assert_called_once_with('Executor [None]: %s' % test_message)

  def test_registered(self):
    driver = ExecutorDriver()
    executor_info = mesos_pb2.ExecutorInfo()
    framework_info = mesos_pb2.FrameworkInfo()
    slave_info = mesos_pb2.SlaveInfo()

    self.executor_base.registered(driver, executor_info, framework_info, slave_info)
    assert self.executor_base._driver == driver
    assert self.executor_base._executor_info == executor_info
    assert self.executor_base._framework_info == framework_info
    assert self.executor_base._slave_info == slave_info

  def test_reregistered(self):
    driver = ExecutorDriver()
    slave_info = mesos_pb2.SlaveInfo()
    self.executor_base.reregistered(driver, slave_info)

  def test_disconnected(self):
    driver = ExecutorDriver()
    self.executor_base.disconnected(driver)

  @mock.patch('mesos.interface.mesos_pb2.TaskStatus', spec=mesos_pb2.TaskStatus)
  def test_send_update(self, MockTaskStatus):
    driver = mock.Mock(ExecutorDriver)
    task_id = 'task_id'
    state = mesos_pb2.TASK_RUNNING
    message = 'test_message'
    self.executor_base.send_update(driver, task_id, state, message)
    driver.sendStatusUpdate.assert_called_once_with(MockTaskStatus.return_value)
    assert MockTaskStatus.return_value.state == state
    assert MockTaskStatus.return_value.task_id.value == task_id
    assert MockTaskStatus.return_value.message == message

  def test_frameworkMessage(self):
    driver = ExecutorDriver()
    self.executor_base.frameworkMessage(driver, 'test_message')

  def test_error(self):
    driver = ExecutorDriver()
    self.executor_base.error(driver, 'message')
