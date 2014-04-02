#
# Copyright 2013 Apache Software Foundation
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

from gen.apache.aurora.ttypes import (
    AssignedTask,
    Identity,
    JobKey,
    ResponseCode,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskConfig,
    TaskEvent,
    TaskQuery,
)

from apache.aurora.client.cli import (
    EXIT_INVALID_PARAMETER
)
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext

from mock import call, Mock, patch


class TestJobStatus(AuroraClientCommandTest):
  @classmethod
  def create_mock_scheduled_tasks(cls):
    jobs = []
    for name in ['foo', 'bar', 'baz']:
      job = Mock()
      job.key = JobKey(role=cls.TEST_ROLE, environment=cls.TEST_ENV, name=name)
      job.failure_count = 0
      job.assignedTask = Mock(spec=AssignedTask)
      job.assignedTask.slaveHost = 'slavehost'
      job.assignedTask.task = Mock(spec=TaskConfig)
      job.assignedTask.task.maxTaskFailures = 1
      job.assignedTask.task.metadata = []
      job.assignedTask.task.owner = Identity(role='bozo')
      job.assignedTask.task.environment = 'test'
      job.assignedTask.task.jobName = 'woops'
      job.assignedTask.task.numCpus = 2
      job.assignedTask.task.ramMb = 2
      job.assignedTask.task.diskMb = 2
      job.assignedTask.instanceId = 4237894
      job.assignedTask.assignedPorts = None
      job.status = ScheduleStatus.RUNNING
      mockEvent = Mock(spec=TaskEvent)
      mockEvent.timestamp = 28234726395
      mockEvent.status = ScheduleStatus.RUNNING
      mockEvent.message = "Hi there"
      job.taskEvents = [mockEvent]
      jobs.append(job)
    return jobs

  @classmethod
  def create_mock_scheduled_task_no_metadata(cls):
    result = cls.create_mock_scheduled_tasks()
    for job in result:
      job.assignedTask.task.metadata = None
    return result

  @classmethod
  def create_getjobs_response(cls):
    result = Mock()
    result.responseCode = ResponseCode.OK
    result.result = Mock()
    result.result.getJobsResult = Mock()
    mock_job_one = Mock()
    mock_job_one.key = Mock()
    mock_job_one.key.role = 'RoleA'
    mock_job_one.key.environment = 'test'
    mock_job_one.key.name = 'hithere'
    mock_job_two = Mock()
    mock_job_two.key = Mock()
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

  def test_successful_status_shallow(self):
    """Test the status command at the shallowest level: calling status should end up invoking
    the local APIs get_status method."""
    mock_context = FakeAuroraCommandContext()
    mock_api = mock_context.get_api('west')
    mock_api.check_status.return_value = self.create_status_response()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      mock_api.check_status.assert_called_with(AuroraJobKey('west', 'bozo', 'test', 'hello'))

  def test_successful_status_shallow_nometadata(self):
    """Regression test: there was a crasher bug when metadata was None."""

    mock_context = FakeAuroraCommandContext()
    mock_api = mock_context.get_api('west')
    mock_api.check_status.return_value = self.create_status_response_null_metadata()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      mock_api.check_status.assert_called_with(AuroraJobKey('west', 'bozo', 'test', 'hello'))

  def test_successful_status_deep(self):
    """Test the status command more deeply: in a request with a fully specified
    job, it should end up doing a query using getTasksStatus."""
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.query.return_value = self.create_status_response()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      mock_scheduler_proxy.getTasksStatus.assert_called_with(TaskQuery(jobName='hello',
          environment='test', owner=Identity(role='bozo')))

  def test_successful_status_deep_null_metadata(self):
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.query.return_value = self.create_status_response_null_metadata()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      mock_scheduler_proxy.getTasksStatus.assert_called_with(TaskQuery(jobName='hello',
          environment='test', owner=Identity(role='bozo')))

  def test_status_wildcard(self):
    """Test status using a wildcard. It should first call api.get_jobs, and then do a
    getTasksStatus on each job."""
    mock_context = FakeAuroraCommandContext()
    mock_api = mock_context.get_api('west')
    mock_api.check_status.return_value = self.create_status_response()
    mock_api.get_jobs.return_value = self.create_getjobs_response()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.cli.context.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', '*'])

    # Wildcard should have expanded to two jobs, so there should be two calls
    # to check_status.
    assert mock_api.check_status.call_count == 2

    assert mock_api.check_status.call_args_list[0][0][0].cluster == 'west'
    assert mock_api.check_status.call_args_list[0][0][0].role == 'RoleA'
    assert mock_api.check_status.call_args_list[0][0][0].env == 'test'
    assert mock_api.check_status.call_args_list[0][0][0].name == 'hithere'

    assert mock_api.check_status.call_args_list[1][0][0].cluster == 'west'
    assert mock_api.check_status.call_args_list[1][0][0].role == 'bozo'
    assert mock_api.check_status.call_args_list[1][0][0].env == 'test'
    assert mock_api.check_status.call_args_list[1][0][0].name == 'hello'

  def test_status_wildcard_two(self):
    """Test status using a wildcard. It should first call api.get_jobs, and then do a
    getTasksStatus on each job. This time, use a pattern that doesn't match all of the jobs."""
    mock_context = FakeAuroraCommandContext()
    mock_api = mock_context.get_api('west')
    mock_api.check_status.return_value = self.create_status_response()
    mock_api.get_jobs.return_value = self.create_getjobs_response()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', 'example/*/*/hello'])

    # Wildcard should have expanded to two jobs, but only matched one,
    # so there should be one call to check_status.
    assert mock_api.check_status.call_count == 1
    mock_api.check_status.assert_called_with(
        AuroraJobKey('example', 'bozo', 'test', 'hello'))

  def test_unsuccessful_status_shallow(self):
    """Test the status command at the shallowest level: calling status should end up invoking
    the local APIs get_status method."""
    # Calls api.check_status, which calls scheduler_proxy.getJobs
    mock_context = FakeAuroraCommandContext()
    mock_api = mock_context.get_api('west')
    mock_api.check_status.return_value = self.create_failed_status_response()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      cmd = AuroraCommandLine()
      result = cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      assert result == EXIT_INVALID_PARAMETER
