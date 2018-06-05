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
import pytest
from mock import call, patch

from apache.aurora.client.cli import Context
from apache.aurora.client.cli.jobs import AddCommand
from apache.aurora.client.cli.options import TaskInstanceKey

from .util import AuroraClientCommandTest, FakeAuroraCommandContext, mock_verb_options

from gen.apache.aurora.api.constants import ACTIVE_STATES
from gen.apache.aurora.api.ttypes import ScheduleStatus, TaskQuery


class TestAddCommand(AuroraClientCommandTest):
  def setUp(self):
    self._command = AddCommand()
    self._mock_options = mock_verb_options(self._command)
    self._mock_options.task_instance = TaskInstanceKey(self.TEST_JOBKEY, 1)
    self._mock_options.instance_count = 3
    self._fake_context = FakeAuroraCommandContext()
    self._fake_context.set_options(self._mock_options)
    self._mock_api = self._fake_context.get_api('test')

  def test_add_instances(self):
    self._mock_options.open_browser = True
    self._fake_context.add_expected_query_result(self.create_query_call_result(
        self.create_scheduled_task(1, ScheduleStatus.RUNNING)))
    self._fake_context.add_expected_query_result(
      self.create_query_call_result(), job_key=self.TEST_JOBKEY)

    self._mock_api.add_instances.return_value = self.create_simple_success_response()

    with patch('webbrowser.open_new_tab') as mock_webbrowser:
      self._command.execute(self._fake_context)

    assert self._mock_api.add_instances.mock_calls == [call(
        self.TEST_JOBKEY,
        self._mock_options.task_instance.instance,
        3)]
    assert mock_webbrowser.mock_calls == [
        call("http://something_or_other/scheduler/bozo/test/hello")
    ]
    assert self._mock_api.query_no_configs.mock_calls == [
      call(TaskQuery(jobKeys=[self.TEST_JOBKEY.to_thrift()], statuses=ACTIVE_STATES)),
      call(TaskQuery(jobKeys=[self.TEST_JOBKEY.to_thrift()], statuses=ACTIVE_STATES))
    ]

  def test_wait_added_instances(self):
    self._mock_options.wait_until = 'RUNNING'
    self._fake_context.add_expected_query_result(self.create_query_call_result(
        self.create_scheduled_task(1, ScheduleStatus.PENDING)))
    self._fake_context.add_expected_query_result(
      self.create_query_call_result(), job_key=self.TEST_JOBKEY)

    self._mock_api.add_instances.return_value = self.create_simple_success_response()

    with patch('apache.aurora.client.cli.jobs.wait_until') as mock_wait:
      self._command.execute(self._fake_context)

    assert self._mock_api.add_instances.mock_calls == [call(
        self.TEST_JOBKEY,
        self._mock_options.task_instance.instance,
        3)]
    assert mock_wait.mock_calls == [call(
        self._mock_options.wait_until,
        self.TEST_JOBKEY,
        self._mock_api,
        [2, 3, 4])]
    assert self._mock_api.query_no_configs.mock_calls == [
      call(TaskQuery(jobKeys=[self.TEST_JOBKEY.to_thrift()], statuses=ACTIVE_STATES)),
      call(TaskQuery(jobKeys=[self.TEST_JOBKEY.to_thrift()], statuses=ACTIVE_STATES))
    ]

  def test_no_active_instance(self):
    self._fake_context.add_expected_query_result(self.create_empty_task_result())
    with pytest.raises(Context.CommandError):
      self._command.execute(self._fake_context)

  def test_add_instances_raises(self):
    self._fake_context.add_expected_query_result(self.create_query_call_result(
        self.create_scheduled_task(1, ScheduleStatus.PENDING)))
    self._fake_context.add_expected_query_result(
      self.create_query_call_result(), job_key=self.TEST_JOBKEY)

    self._mock_api.add_instances.return_value = self.create_error_response()

    with pytest.raises(Context.CommandError):
      self._command.execute(self._fake_context)

    assert self._mock_api.query_no_configs.mock_calls == [
      call(TaskQuery(jobKeys=[self.TEST_JOBKEY.to_thrift()], statuses=ACTIVE_STATES)),
      call(TaskQuery(jobKeys=[self.TEST_JOBKEY.to_thrift()], statuses=ACTIVE_STATES))
    ]
