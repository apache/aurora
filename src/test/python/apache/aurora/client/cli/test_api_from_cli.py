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

import contextlib

from mock import Mock, patch

from apache.aurora.client.api.scheduler_client import SchedulerClient
from apache.aurora.client.cli import EXIT_UNKNOWN_ERROR
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.util import AuroraClientCommandTest

from gen.apache.aurora.api import AuroraAdmin
from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    GetJobsResult,
    Identity,
    JobConfiguration,
    JobKey,
    Response,
    ResponseCode,
    Result,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskConfig,
    TaskEvent,
    TaskQuery
)


class TestApiFromCLI(AuroraClientCommandTest):
  """A container for tests that are probing at API functionality,
  to see if the CLI can handle API-level errors.
  """

  @classmethod
  def create_mock_scheduled_tasks(cls):
    tasks = []
    for name in ['foo', 'bar', 'baz']:
      task = Mock(spec=ScheduledTask)
      task.key = JobKey(role=cls.TEST_ROLE, environment=cls.TEST_ENV, name=name)
      task.failure_count = 0
      task.assignedTask = Mock(spec=AssignedTask)
      task.assignedTask.slaveHost = 'slavehost'
      task.assignedTask.task = Mock(spec=TaskConfig)
      task.assignedTask.task.maxTaskFailures = 1
      task.assignedTask.task.metadata = []
      task.assignedTask.task.owner = Identity(role='bozo')
      task.assignedTask.task.environment = 'test'
      task.assignedTask.task.jobName = 'woops'
      task.assignedTask.task.numCpus = 2
      task.assignedTask.task.ramMb = 2
      task.assignedTask.task.diskMb = 2
      task.assignedTask.instanceId = 4237894
      task.assignedTask.assignedPorts = None
      task.status = ScheduleStatus.RUNNING
      mockEvent = Mock(spec=TaskEvent)
      mockEvent.timestamp = 28234726395
      mockEvent.status = ScheduleStatus.RUNNING
      mockEvent.message = "Hi there"
      task.taskEvents = [mockEvent]
      tasks.append(task)
    return tasks

  @classmethod
  def create_mock_scheduled_task_no_metadata(cls):
    result = cls.create_mock_scheduled_tasks()
    for task in result:
      task.assignedTask.task.metadata = None
    return result

  @classmethod
  def create_getjobs_response(cls):
    result = Mock(spec=Response)
    result.responseCode = ResponseCode.OK
    result.result = Mock(spec=Result)
    result.result.getJobsResult = Mock(spec=GetJobsResult)
    mock_job_one = Mock(spec=JobConfiguration)
    mock_job_one.key = Mock(spec=JobKey)
    mock_job_one.key.role = 'RoleA'
    mock_job_one.key.environment = 'test'
    mock_job_one.key.name = 'hithere'
    mock_job_two = Mock(spec=JobConfiguration)
    mock_job_two.key = Mock(spec=JobKey)
    mock_job_two.key.role = 'bozo'
    mock_job_two.key.environment = 'test'
    mock_job_two.key.name = 'hello'
    result.result.getJobsResult.configs = [mock_job_one, mock_job_two]
    return result

  @classmethod
  def create_status_response(cls):
    resp = cls.create_simple_success_response()
    resp.result.scheduleStatusResult = Mock(spec=ScheduleStatusResult)
    resp.result.scheduleStatusResult.tasks = set(cls.create_mock_scheduled_tasks())
    return resp

  @classmethod
  def create_status_response_null_metadata(cls):
    resp = cls.create_simple_success_response()
    resp.result.scheduleStatusResult = Mock(spec=ScheduleStatusResult)
    resp.result.scheduleStatusResult.tasks = set(cls.create_mock_scheduled_task_no_metadata())
    return resp

  @classmethod
  def create_failed_status_response(cls):
    return cls.create_blank_response(ResponseCode.INVALID_REQUEST, 'No tasks found for query')

  def test_successful_status_deep(self):
    """Test the status command more deeply: in a request with a fully specified
    job, it should end up doing a query using getTasksWithoutConfigs."""
    _, mock_scheduler_proxy = self.create_mock_api()
    mock_scheduler_proxy.query.return_value = self.create_status_response()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      mock_scheduler_proxy.getTasksWithoutConfigs.assert_called_with(
          TaskQuery(jobKeys=[JobKey(role='bozo', environment='test', name='hello')]))

  def test_status_api_failure(self):
    # TODO(wfarner): Consider spec_set instead of spec.
    mock_scheduler_client = Mock(spec=SchedulerClient)
    mock_thrift_client = Mock(spec=AuroraAdmin.Client)
    mock_scheduler_client.get_thrift_client.return_value = mock_thrift_client

    mock_thrift_client.getTasksWithoutConfigs.side_effect = IOError("Uh-Oh")
    with contextlib.nested(
        patch('apache.aurora.client.api.scheduler_client.SchedulerClient.get',
            return_value=mock_scheduler_client),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):

      cmd = AuroraCommandLine()
      # This should create a scheduler client, set everything up, and then issue a
      # getTasksWithoutConfigs call against the mock_scheduler_client. That should raise an
      # exception, which results in the command failing with an error code.
      result = cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      assert result == EXIT_UNKNOWN_ERROR
      mock_thrift_client.getTasksWithoutConfigs.assert_called_with(
        TaskQuery(jobKeys=[JobKey(role='bozo', environment='test', name='hello')]))
