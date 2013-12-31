import contextlib
import unittest


from twitter.aurora.client.commands.core import create
from twitter.aurora.client.commands.util import AuroraClientCommandTest
from twitter.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from twitter.aurora.config import AuroraConfig
from twitter.common import app
from twitter.common.contextutil import temporary_file

from gen.twitter.aurora.ttypes import (
    AssignedTask,
    Identity,
    Response,
    ResponseCode,
    Result,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskEvent,
    TaskQuery,
)

from mock import Mock, patch
from pystachio.config import Config


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
    # scheduler.scheduler() is called once, as a part of the handle_open call.
    assert mock_api.scheduler.scheduler.call_count == 1
    assert mock_api.scheduler.getTasksStatus.call_count == num_queries
    mock_api.scheduler.getTasksStatus.assert_called_with(mock_query)

  def test_simple_successful_create_job(self):
    """Run a test of the "create" command against a mocked-out API:
    Verifies that the creation command sends the right API RPCs, and performs the correct
    tests on the result."""
    mock_options = self.setup_mock_options()

    # create first calls get_job_config, which calls get_config. As long as we've got the options
    # set up correctly, this should work.

    # Next, create gets an API object via make_client. We need to replace that with a mock API.
    (mock_api, mock_scheduler) = self.setup_mock_api()
    with contextlib.nested(
        patch('twitter.aurora.client.commands.core.make_client', return_value=mock_api),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (make_client,
        options):

      # After making the client, create sets up a job monitor.
      # The monitor uses TaskQuery to get the tasks. It's called at least twice:once before
      # the job is created, and once after. So we need to set up mocks for the query results.
      mock_query = self.create_mock_query()
      mock_scheduler.getTasksStatus.side_effect = [
        self.create_mock_status_query_result(ScheduleStatus.INIT),
        self.create_mock_status_query_result(ScheduleStatus.RUNNING)
      ]

      # With the monitor set up, create finally gets around to calling create_job.
      mock_api.create_job.return_value = self.get_createjob_response()

      # Then it calls handle_open; we need to provide a mock for the API calls it uses.
      mock_api.scheduler.scheduler.return_value = mock_scheduler

      # Finally, it calls the monitor to watch and make sure the jobs started;
      # but we already set that up in the side-effects list for the query mock.

      # This is the real test: invoke create as if it had been called by the command line.
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        create(['west/mchucarroll/test/hello', fp.name])

      # Now check that the right API calls got made.
      # Check that create_job was called exactly once, with an AuroraConfig parameter.
      self.assert_create_job_called(mock_api)
      self.assert_scheduler_called(mock_api, mock_query, 2)
      # make_client should have been called once.
      make_client.assert_called_with('west')

  def test_create_job_wait_until_finished(self):
    """Run a test of the "create" command against a mocked-out API:
    this time, make the monitor check status several times before successful completion.
    """
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler) = self.setup_mock_api()
    with contextlib.nested(
        patch('time.sleep'),
        patch('twitter.aurora.client.commands.core.make_client', return_value=mock_api),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (sleep, make_client,
        options):
      mock_query = self.create_mock_query()
      mock_query_results = [
          self.create_mock_status_query_result(ScheduleStatus.INIT),
          self.create_mock_status_query_result(ScheduleStatus.PENDING),
          self.create_mock_status_query_result(ScheduleStatus.PENDING),
          self.create_mock_status_query_result(ScheduleStatus.RUNNING),
          self.create_mock_status_query_result(ScheduleStatus.FINISHED)
      ]
      mock_scheduler.getTasksStatus.side_effect = mock_query_results
      mock_api.create_job.return_value = self.get_createjob_response()
      mock_api.scheduler.scheduler.return_value = mock_scheduler
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        create(['west/mchucarroll/test/hello', fp.name])

      # Now check that the right API calls got made.
      # Check that create_job was called exactly once, with an AuroraConfig parameter.
      self.assert_create_job_called(mock_api)
      self.assert_scheduler_called(mock_api, mock_query, 4)
      # make_client should have been called once.
      make_client.assert_called_with('west')

  def test_create_job_failed(self):
    """Run a test of the "create" command against a mocked-out API:
    this time, make the monitor check status several times before successful completion.
    """
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler) = self.setup_mock_api()
    with contextlib.nested(
        patch('twitter.aurora.client.commands.core.make_client', return_value=mock_api),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (make_client,
        options):
      mock_query = self.create_mock_query()
      mock_query_results = [
          self.create_mock_status_query_result(ScheduleStatus.INIT)
      ]
      mock_scheduler.getTasksStatus.side_effect = mock_query_results
      mock_api.create_job.return_value = self.get_failed_createjob_response()
      # This is the real test: invoke create as if it had been called by the command line.
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        self.assertRaises(SystemExit, create, (['west/mchucarroll/test/hello', fp.name]))

      # Now check that the right API calls got made.
      # Check that create_job was called exactly once, with an AuroraConfig parameter.
      self.assert_create_job_called(mock_api)

      # scheduler.scheduler() should not have been called, because the create_job failed.
      assert mock_api.scheduler.scheduler.call_count == 0
      # getTasksStatus was called once, before the create_job
      assert mock_scheduler.getTasksStatus.call_count == 1
      mock_scheduler.getTasksStatus.assert_called_with(mock_query)
      # make_client should have been called once.
      make_client.assert_called_with('west')

  def test_delayed_job(self):
    """Run a test of the "create" command against a mocked-out API:
    this time, make the monitor check status several times before successful completion.
    """
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler) = self.setup_mock_api()
    with contextlib.nested(
        patch('time.sleep'),
        patch('twitter.aurora.client.commands.core.make_client', return_value=mock_api),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (sleep, make_client,
        options):
      mock_query = self.create_mock_query()
      mock_query_results = [
          self.create_mock_status_query_result(ScheduleStatus.INIT),
          self.create_mock_status_query_result(ScheduleStatus.PENDING),
          self.create_mock_status_query_result(ScheduleStatus.PENDING),
          self.create_mock_status_query_result(ScheduleStatus.RUNNING),
          self.create_mock_status_query_result(ScheduleStatus.FINISHED)
      ]
      mock_scheduler.getTasksStatus.side_effect = mock_query_results
      mock_api.create_job.return_value = self.get_createjob_response()
      mock_api.scheduler.scheduler.return_value = mock_scheduler
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        create(['west/mchucarroll/test/hello', fp.name])

      # Now check that the right API calls got made.
      self.assert_create_job_called(mock_api)
      self.assert_scheduler_called(mock_api, mock_query, 4)
      # make_client should have been called once.
      make_client.assert_called_with('west')

  def test_create_job_failed_invalid_config(self):
    """Run a test of the "create" command against a mocked-out API, with a configuration
    containing a syntax error"""
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler) = self.setup_mock_api()
    with contextlib.nested(
        patch('twitter.aurora.client.commands.core.make_client', return_value=mock_api),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (make_client,
        options):
      with temporary_file() as fp:
        fp.write(self.get_invalid_config('invalid_clause=oops'))
        fp.flush()
        self.assertRaises(Config.InvalidConfigError, create, (['west/mchucarroll/test/hello',
            fp.name]))

      # Now check that the right API calls got made.
      # Check that create_job was not called.
      assert mock_api.create_job.call_count == 0
      # scheduler.scheduler() should not have been called, because the config was invalid.
      assert mock_api.scheduler.scheduler.call_count == 0

      assert mock_scheduler.getTasksStatus.call_count == 0
      # make_client should not have been called.
      assert make_client.call_count == 0

  def test_create_job_failed_invalid_config_two(self):
    """Run a test of the "create" command against a mocked-out API:
    this time, make the monitor check status several times before successful completion.
    """
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler) = self.setup_mock_api()
    with contextlib.nested(
        patch('twitter.aurora.client.commands.core.make_client', return_value=mock_api),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (make_client,
        options):
      with temporary_file() as fp:
        fp.write(self.get_invalid_config('invalid_clause=\'oops\','))
        fp.flush()
        self.assertRaises(AttributeError, create, (['west/mchucarroll/test/hello', fp.name]))

      # Now check that the right API calls got made.
      # Check that create_job was not called.
      assert mock_api.create_job.call_count == 0
      # scheduler.scheduler() should not have been called, because the config was invalid.
      assert mock_api.scheduler.scheduler.call_count == 0
      # getTasksStatus was called once, before the create_job
      assert mock_scheduler.getTasksStatus.call_count == 0
      # make_client should not have been called.
      assert make_client.call_count == 0
