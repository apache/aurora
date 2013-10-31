import contextlib
import unittest


from twitter.aurora.client.commands.core import create
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


class TestClientCreateCommand(unittest.TestCase):
  # Configuration to use
  CONFIG_BASE = """
HELLO_WORLD = Job(
  name = 'hello',
  role = 'mchucarroll',
  cluster = 'smfd',
  environment = 'test',
  instances = 2,
  task = Task(
    name = 'test',
    processes = [Process(name = 'hello_world', cmdline = 'echo {{thermos.ports[http]}}')],
    resources = Resources(cpu = 0.1, ram = 64 * MB, disk = 64 * MB),
  )
)
jobs = [HELLO_WORLD]
"""

  TEST_ROLE = 'mchucarroll'
  TEST_ENV = 'test'
  TEST_JOB = 'hello'

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
  def get_mock_query_results(cls):
    mock_query_result_one = Mock(spec=Response)
    mock_query_result_one.result = Mock(spec=Result)
    mock_query_result_one.result.scheduleStatusResult = Mock(spec=ScheduleStatusResult)
    mock_query_result_one.result.scheduleStatusResult.tasks = []

    # the query result for second round of calling stuff via the monitor
    mock_task_one = cls.create_mock_task('hello', 0, 1000, ScheduleStatus.RUNNING)
    mock_task_two = cls.create_mock_task('hello', 1, 1004, ScheduleStatus.RUNNING)
    mock_query_result_two = Mock(spec=Response)
    mock_query_result_two.result = Mock(spec=Result)
    mock_query_result_two.result.scheduleStatusResult = Mock(spec=ScheduleStatusResult)
    mock_query_result_two.result.scheduleStatusResult.tasks = [mock_task_one, mock_task_two]

    return [mock_query_result_one, mock_query_result_two]

  @classmethod
  def get_createjob_response(cls):
    # Then, we call api.create_job(config)
    mock_resp = Mock(spec=Response)
    mock_resp.responseCode = ResponseCode.OK
    mock_resp.message = "OK"
    return mock_resp

  def test_create_job(self):
    """Run a test of the "create" command against a mocked-out API:
    Verifies that the creation command sends the right API RPCs, and performs the correct
    tests on the result."""
    mock_options = self.setup_mock_options()

    # create first calls get_job_config, which calls get_config. As long as we've got the options
    # set up correctly, this should work.

    # Next, create gets an API object via make_client. We need to replace that with a mock API.
    (mock_api, mock_scheduler) = self.setup_mock_api()
    with contextlib.nested(patch('twitter.aurora.client.commands.core.make_client'),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (make_client,
        options):
      make_client.return_value = mock_api
      options.return_value = mock_options

      # After making the client, create sets up a job monitor.
      # The monitor uses TaskQuery to get the tasks. It's called at least twice:once before
      # the job is created, and once after. So we need to set up mocks for the query results.
      mock_query = TaskQuery(owner=Identity(role=self.TEST_ROLE),
          environment=self.TEST_ENV, jobName=self.TEST_JOB)
      mock_scheduler.getTasksStatus.side_effect = self.get_mock_query_results()

      # With the monitor set up, create finally gets around to calling create_job.
      mock_api.create_job.return_value = self.get_createjob_response()

      # Then it calls handle_open; we need to provide a mock for the API calls it uses.
      mock_api.scheduler.scheduler.return_value = mock_scheduler

      # Finally, it calls the monitor to watch and make sure the jobs started;
      # but we already set that up in the side-effects list for the query mock.

      # This is the real test: invoke create as if it had been called by the command line.
      with temporary_file() as fp:
        fp.write(self.CONFIG_BASE)
        fp.flush()
        create(['smfd/mchucarroll/test/hello', fp.name])

      # Now check that the right API calls got made.
      # Check that create_job was called exactly once, with an AuroraConfig parameter.
      create_job_calls = mock_api.create_job.call_args_list
      assert len(create_job_calls) == 1
      assert isinstance(create_job_calls[0][0][0], AuroraConfig)
      # scheduler.scheduler() is called once, as a part of the handle_open call.
      assert mock_api.scheduler.scheduler.call_count == 1
      # getTasksStatus was called twice - once before create_job, and once after.
      assert mock_scheduler.getTasksStatus.call_count == 2
      # make_client should have been called once.
      make_client.assert_called_with('smfd')
      # And getTasksStatus should have been called twice. The calls use the
      # same parameter.
      assert mock_scheduler.getTasksStatus.call_count == 2
      mock_scheduler.getTasksStatus.assert_called_with(mock_query)
