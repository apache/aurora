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

from mock import call, create_autospec, patch

from apache.aurora.client.api.scheduler_client import SchedulerClient
from apache.aurora.client.cli import EXIT_UNKNOWN_ERROR
from apache.aurora.client.cli.client import AuroraCommandLine

from .util import AuroraClientCommandTest

from gen.apache.aurora.api import AuroraAdmin
from gen.apache.aurora.api.ttypes import (
    JobKey,
    ResponseCode,
    Result,
    ScheduleStatusResult,
    TaskQuery
)


class TestApiFromCLI(AuroraClientCommandTest):
  """A container for tests that are probing at API functionality,
  to see if the CLI can handle API-level errors.
  """

  @classmethod
  def create_mock_scheduled_task_no_metadata(cls):
    result = cls.create_mock_scheduled_tasks()
    for task in result:
      task.assignedTask.task.metadata = None
    return result

  @classmethod
  def create_status_response(cls):
    resp = cls.create_simple_success_response()
    resp.result = Result(
        scheduleStatusResult=ScheduleStatusResult(tasks=set(cls.create_scheduled_tasks())))
    return resp

  @classmethod
  def create_status_response_null_metadata(cls):
    resp = cls.create_simple_success_response()
    resp.result = Result(
        scheduleStatusResult=ScheduleStatusResult(
            tasks=set(cls.create_mock_scheduled_task_no_metadata())))
    return resp

  @classmethod
  def create_failed_status_response(cls):
    return cls.create_blank_response(ResponseCode.INVALID_REQUEST, 'No tasks found for query')

  def test_successful_status_deep(self):
    """Test the status command more deeply: in a request with a fully specified
    job, it should end up doing a query using getTasksWithoutConfigs."""
    mock_scheduler_client = create_autospec(spec=SchedulerClient, instance=True)
    mock_thrift_client = create_autospec(spec=AuroraAdmin.Client, instance=True)
    mock_scheduler_client.get_thrift_client.return_value = mock_thrift_client
    mock_thrift_client.getTasksWithoutConfigs.return_value = self.create_status_response()
    with patch('apache.aurora.client.api.scheduler_client.SchedulerClient.get',
               return_value=mock_scheduler_client):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      assert mock_thrift_client.getTasksWithoutConfigs.mock_calls == [
          call(TaskQuery(jobKeys=[JobKey(role='bozo', environment='test', name='hello')]))]

  def test_status_api_failure(self):
    mock_scheduler_client = create_autospec(spec=SchedulerClient, instance=True)
    mock_thrift_client = create_autospec(spec=AuroraAdmin.Client, instance=True)
    mock_scheduler_client.get_thrift_client.return_value = mock_thrift_client

    mock_thrift_client.getTasksWithoutConfigs.side_effect = IOError("Uh-Oh")
    with patch('apache.aurora.client.api.scheduler_client.SchedulerClient.get',
            return_value=mock_scheduler_client):
      cmd = AuroraCommandLine()
      # This should create a scheduler client, set everything up, and then issue a
      # getTasksWithoutConfigs call against the mock_scheduler_client. That should raise an
      # exception, which results in the command failing with an error code.
      result = cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      assert result == EXIT_UNKNOWN_ERROR
      assert mock_thrift_client.getTasksWithoutConfigs.mock_calls == [
        call(TaskQuery(jobKeys=[JobKey(role='bozo', environment='test', name='hello')]))]
