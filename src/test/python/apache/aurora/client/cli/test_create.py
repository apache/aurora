import contextlib

from twitter.aurora.client.cli import (
    AuroraCommandLine,
    EXIT_INVALID_CONFIGURATION,
    EXIT_NETWORK_ERROR
)
from twitter.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext
from twitter.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from twitter.aurora.config import AuroraConfig
from twitter.common.contextutil import temporary_file

from gen.twitter.aurora.ttypes import (
    AssignedTask,
    Identity,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskEvent,
    TaskQuery,
)

from mock import Mock, patch


class TestClientCreateCommand(AuroraClientCommandTest):

  @classmethod
  def setup_mock_options(cls):
    """set up to get a mock options object."""
    mock_options = Mock()
    mock_options.json = False
    mock_options.bindings = {}
    mock_options.open_browser = False
    mock_options.cluster = None
    mock_options.wait_until = 'RUNNING'  # or 'FINISHED' for other tests
    return mock_options

  @classmethod
  def setup_mock_api(cls):
    """Builds up a mock API object, with a mock SchedulerProxy"""
    mock_api = Mock(spec=HookedAuroraClientAPI)
    mock_scheduler = Mock()
    mock_scheduler.url = "http://something_or_other"
    mock_api.scheduler = mock_scheduler
    return (mock_api, mock_scheduler)

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
  def create_mock_query(cls):
    return TaskQuery(owner=Identity(role=cls.TEST_ROLE), environment=cls.TEST_ENV,
        jobName=cls.TEST_JOB)

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
    print('Calls to getTasksStatus: %s' % mock_api.scheduler.getTasksStatus.call_args_list)
    assert mock_api.scheduler.getTasksStatus.call_count == num_queries
    mock_api.scheduler.getTasksStatus.assert_called_with(mock_query)

  def test_simple_successful_create_job(self):
    """Run a test of the "create" command against a mocked-out API:
    Verifies that the creation command sends the right API RPCs, and performs the correct
    tests on the result."""

    # We'll patch out create_context, which will give us a fake context
    # object, and everything can be stubbed through that.
    mock_context = FakeAuroraCommandContext()
    with patch('twitter.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      # After making the client, create sets up a job monitor.
      # The monitor uses TaskQuery to get the tasks. It's called at least twice:once before
      # the job is created, and once after. So we need to set up mocks for the query results.
      mock_query = self.create_mock_query()
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.INIT))
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.RUNNING))
      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_createjob_response()

      # This is the real test: invoke create as if it had been called by the command line.
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'create', '--wait_until=RUNNING', 'west/mchucarroll/test/hello',
            fp.name])

      # Now check that the right API calls got made.
      # Check that create_job was called exactly once, with an AuroraConfig parameter.
      self.assert_create_job_called(api)
      self.assert_scheduler_called(api, mock_query, 2)

  def test_create_job_delayed(self):
    """Run a test of the "create" command against a mocked-out API:
    this time, make the monitor check status several times before successful completion.
    """
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('time.sleep'),
        patch('twitter.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      mock_query = self.create_mock_query()
      for result in [ScheduleStatus.INIT, ScheduleStatus.PENDING, ScheduleStatus.PENDING,
          ScheduleStatus.RUNNING, ScheduleStatus.FINISHED]:
        mock_context.add_expected_status_query_result(self.create_mock_status_query_result(result))
      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_createjob_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'create', '--wait_until=RUNNING', 'west/mchucarroll/test/hello',
            fp.name])
        # Now check that the right API calls got made.
        # Check that create_job was called exactly once, with an AuroraConfig parameter.
        self.assert_create_job_called(api)
        self.assert_scheduler_called(api, mock_query, 4)

  def test_create_job_failed(self):
    """Run a test of the "create" command against a mocked-out API:
    this time, make the monitor check status several times before successful completion.
    """
    mock_context = FakeAuroraCommandContext()
    with patch('twitter.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      mock_context.add_expected_status_query_result(
          self.create_mock_status_query_result(ScheduleStatus.INIT))
      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_failed_createjob_response()
      # This is the real test: invoke create as if it had been called by the command line.
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'create', '--wait_until=RUNNING',
            'west/mchucarroll/test/hello', fp.name])
        assert result == EXIT_NETWORK_ERROR

      # Now check that the right API calls got made.
      # Check that create_job was called exactly once, with an AuroraConfig parameter.
      self.assert_create_job_called(api)

      # getTasksStatus was called once, before the create_job
      assert api.scheduler.getTasksStatus.call_count == 1

  def test_create_job_failed_invalid_config(self):
    """Run a test of the "create" command against a mocked-out API, with a configuration
    containing a syntax error"""
    mock_context = FakeAuroraCommandContext()
    with patch('twitter.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      with temporary_file() as fp:
        fp.write(self.get_invalid_config('invalid_clause=oops'))
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'create', '--wait_until=RUNNING',
            'west/mchucarroll/test/hello', fp.name])
        assert result == EXIT_INVALID_CONFIGURATION

      # Now check that the right API calls got made.
      # Check that create_job was not called.
      api = mock_context.get_api('west')
      assert api.create_job.call_count == 0
      assert api.scheduler.getTasksStatus.call_count == 0
