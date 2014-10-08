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

import logging
from logging import Handler

from mock import Mock, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext
from apache.aurora.config import AuroraConfig

from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskEvent
)


class MockHandler(Handler):
  def __init__(self):
    Handler.__init__(self)
    self.logs = []

  def emit(self, record):
    self.logs.append(record)


class TestLogging(AuroraClientCommandTest):

  @classmethod
  def create_mock_task(cls, task_id, instance_id, initial_time, status):
    mock_task = Mock(spec=ScheduledTask)
    mock_task.assignedTask = Mock(spec=AssignedTask)
    mock_task.assignedTask.taskId = task_id
    mock_task.assignedTask.instanceId = instance_id
    mock_task.status = status
    mock_task_event = Mock(spec=TaskEvent)
    mock_task_event.timestamp = initial_time
    mock_task.taskEvents = [mock_task_event]
    return mock_task

  @classmethod
  def create_mock_status_query_result(cls, scheduleStatus):
    mock_query_result = cls.create_simple_success_response()
    mock_query_result.result.scheduleStatusResult = Mock(spec=ScheduleStatusResult)
    if scheduleStatus == ScheduleStatus.INIT:
      # status query result for before job is launched.
      mock_query_result.result.scheduleStatusResult.tasks = []
    else:
      mock_task_one = cls.create_mock_task('hello', 0, 1000, scheduleStatus)
      mock_task_two = cls.create_mock_task('hello', 1, 1004, scheduleStatus)
      mock_query_result.result.scheduleStatusResult.tasks = [mock_task_one, mock_task_two]
    return mock_query_result

  @classmethod
  def get_createjob_response(cls):
    # Then, we call api.create_job(config)
    return cls.create_simple_success_response()

  @classmethod
  def get_failed_createjob_response(cls):
    return cls.create_error_response()

  @classmethod
  def assert_create_job_called(cls, mock_api):
    # Check that create_job was called exactly once, with an AuroraConfig parameter.
    assert mock_api.create_job.call_count == 1
    assert isinstance(mock_api.create_job.call_args_list[0][0][0], AuroraConfig)

  @classmethod
  def assert_scheduler_called(cls, mock_api, mock_query, num_queries):
    assert mock_api.scheduler_proxy.getTasksStatus.call_count == num_queries
    mock_api.scheduler_proxy.getTasksStatus.assert_called_with(mock_query)

  def test_command_invocation_logging(self):
    """Sets up a log handler, registers it with the logger, and then verifies that calls
    to the client logging methods correctly get captured in the logs.
    """
    mock_log_handler = MockHandler()
    logger = logging.getLogger('aurora_client')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(mock_log_handler)
    # We'll patch out create_context, which will give us a fake context
    # object, and everything can be stubbed through that.
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      # After making the client, create sets up a job monitor.
      # The monitor uses TaskQuery to get the tasks. It's called at least twice:once before
      # the job is created, and once after. So we need to set up mocks for the query results.
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.INIT))
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.RUNNING))
      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_createjob_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'create', '--wait-until=RUNNING', 'west/bozo/test/hello',
            fp.name])

      # Check that things were logged correctly:
      # there should be at least two entries, with the clientid and username;
      # and one entry should log the command being invoked.
      assert any(("'job', 'create', '--wait-until=RUNNING', 'west/bozo/test/hello'" in
          r.getMessage()) for r in mock_log_handler.logs)
      assert mock_log_handler.logs[0].clientid == mock_log_handler.logs[1].clientid
      assert mock_log_handler.logs[0].user == mock_log_handler.logs[1].user

  def test_configuration_logging(self):
    """Sets up a log handler, registers it with the logger, and then verifies that calls
    to the client logging methods correctly get captured in the logs.
    """
    mock_log_handler = MockHandler()
    logger = logging.getLogger('aurora_client')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(mock_log_handler)
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.INIT))
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.RUNNING))
      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_createjob_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        with open(fp.name, "r") as rp:
          lines = rp.readlines()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'create', 'west/bozo/test/hello',
            fp.name])
        # Check that the contents of the config file were logged, as expected.
        expected_config_msg = "Config: %s" % lines
        assert any(expected_config_msg == r.getMessage() for r in mock_log_handler.logs)

      # Check that things were logged correctly:
      # there should be at least two entries, with the clientid and username;
      # and one entry should log the command being invoked.
      assert any(("'job', 'create', 'west/bozo/test/hello'" in
          r.getMessage()) for r in mock_log_handler.logs)

      assert mock_log_handler.logs[0].clientid == mock_log_handler.logs[1].clientid
      assert mock_log_handler.logs[0].user == mock_log_handler.logs[1].user
