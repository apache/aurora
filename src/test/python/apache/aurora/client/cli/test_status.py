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
    AuroraCommandLine,
    EXIT_INVALID_PARAMETER
)
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
      job.assignedTask.task.packages = []
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

  def test_successful_status_deep(self):
    """Test the status command more deeply: in a request with a fully specified
    job, it should end up doing a query using getTasksStatus."""
    (mock_api, mock_scheduler) = self.setup_mock_api()
    mock_scheduler.query.return_value = self.create_status_response()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      mock_scheduler.getTasksStatus.assert_called_with(TaskQuery(jobName='hello',
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
    assert (call(AuroraJobKey('example', 'RoleA', 'test', 'hithere')) in
        mock_api.check_status.call_args_list)
    assert (call(AuroraJobKey('example', 'bozo', 'test', 'hello')) in
        mock_api.check_status.call_args_list)

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
    # Calls api.check_status, which calls scheduler.getJobs
    mock_context = FakeAuroraCommandContext()
    mock_api = mock_context.get_api('west')
    mock_api.check_status.return_value = self.create_failed_status_response()
    #    mock_api.scheduler.getTasksStatus.return_value =
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      cmd = AuroraCommandLine()
      result = cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      assert result == EXIT_INVALID_PARAMETER
