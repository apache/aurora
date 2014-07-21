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
from twitter.common.contextutil import temporary_file

from apache.aurora.client.commands.core import create
from apache.aurora.client.commands.util import AuroraClientCommandTest
from apache.aurora.client.config import AuroraConfig, GlobalHookRegistry
from apache.aurora.client.hooks.hooked_api import HookedAuroraClientAPI

from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    Identity,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskEvent,
    TaskQuery
)


class CreateHookForTesting(object):
  def __init__(self, succeed):
    self.created_jobs = []
    self.succeed = succeed

  def pre_create_job(self, api, config):
    self.created_jobs.append(config)
    return self.succeed


class TestClientCreateCommand(AuroraClientCommandTest):

  def setUp(self):
    GlobalHookRegistry.reset()

  @classmethod
  def setup_mock_options(cls):
    """set up to get a mock options object."""
    mock_options = Mock()
    mock_options.json = False
    mock_options.bindings = {}
    mock_options.open_browser = False
    mock_options.cluster = None
    mock_options.wait_until = 'RUNNING'  # or 'FINISHED' for other tests
    mock_options.disable_all_hooks_reason = None
    return mock_options

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
    assert mock_api.scheduler_proxy.getTasksWithoutConfigs.call_count == num_queries
    mock_api.scheduler_proxy.getTasksWithoutConfigs.assert_called_with(mock_query)

  def test_create_job_hook_called(self):
    """Run a test of the "create" command against a mocked API;
    verifies that a required hook runs, even though the config doesn't mention it.
    """
    # Create a hook on "create_job" that just adds something to a list in the test.
    # Patch in HookedAuroraClientAPI to replace the UnhookedAuroraClientAPI with a mock.

    mock_options = self.setup_mock_options()
    hook = CreateHookForTesting(True)
    GlobalHookRegistry.register_global_hook(hook)

    # create first calls get_job_config, which calls get_config. As long as we've got the options
    # set up correctly, this should work.

    # Next, create gets an API object via make_client. We need to replace that with a mock API.
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    with contextlib.nested(
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('twitter.common.app.get_options', return_value=mock_options)):

      mock_scheduler_proxy.createJob.return_value = self.get_createjob_response()

      mock_scheduler_proxy.getTasksWithoutConfigs.side_effect = [
        self.create_mock_status_query_result(ScheduleStatus.INIT),
        self.create_mock_status_query_result(ScheduleStatus.RUNNING)
      ]
      # Finally, it calls the monitor to watch and make sure the jobs started;
      # but we already set that up in the side-effects list for the query mock.

      # This is the real test: invoke create as if it had been called by the command line.
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        create(['west/mchucarroll/test/hello', fp.name])

      # Now check that the right API calls got made.
      assert mock_scheduler_proxy.createJob.call_count == 1
      assert len(hook.created_jobs) == 1

  def test_create_job_hook_aborts(self):
    """Run a test of the "create" command against a mocked API;
    verifies that a required hook runs, even though the config doesn't mention it.
    """
    # Create a hook on "create_job" that just adds something to a list in the test.
    # Patch in HookedAuroraClientAPI to replace the UnhookedAuroraClientAPI with a mock.
    mock_options = self.setup_mock_options()
    hook = CreateHookForTesting(False)
    GlobalHookRegistry.register_global_hook(hook)

    # create first calls get_job_config, which calls get_config. As long as we've got the options
    # set up correctly, this should work.

    # Next, create gets an API object via make_client. We need to replace that with a mock API.
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    with contextlib.nested(
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('twitter.common.app.get_options', return_value=mock_options)):

      mock_scheduler_proxy.createJob.return_value = self.get_createjob_response()

      mock_scheduler_proxy.getTasksWithoutConfigs.side_effect = [
        self.create_mock_status_query_result(ScheduleStatus.INIT),
        self.create_mock_status_query_result(ScheduleStatus.RUNNING)
      ]

      # Finally, it calls the monitor to watch and make sure the jobs started;
      # but we already set that up in the side-effects list for the query mock.

      # This is the real test: invoke create as if it had been called by the command line.
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        self.assertRaises(HookedAuroraClientAPI.PreHooksStoppedCall, create,
            ['west/mchucarroll/test/hello', fp.name])

      # Now check that the right API calls got made.
      assert mock_scheduler_proxy.createJob.call_count == 0
      assert len(hook.created_jobs) == 1

  def test_block_hooks(self):
    """Run a test of the "create" command against a mocked API;
    verifies that a required hook runs, even though the config doesn't mention it.
    """
    # Create a hook on "create_job" that just adds something to a list in the test.
    # Patch in HookedAuroraClientAPI to replace the UnhookedAuroraClientAPI with a mock.

    mock_options = self.setup_mock_options()
    hook = CreateHookForTesting(True)
    GlobalHookRegistry.register_global_hook(hook)
    mock_options.disable_all_hooks_reason = "Because I said so."

    # create first calls get_job_config, which calls get_config. As long as we've got the options
    # set up correctly, this should work.
    # Next, create gets an API object via make_client. We need to replace that with a mock API.
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    with contextlib.nested(
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('twitter.common.app.get_options', return_value=mock_options)):

      mock_scheduler_proxy.createJob.return_value = self.get_createjob_response()

      mock_scheduler_proxy.getTasksWithoutConfigs.side_effect = [
        self.create_mock_status_query_result(ScheduleStatus.INIT),
        self.create_mock_status_query_result(ScheduleStatus.RUNNING)
      ]
      # Finally, it calls the monitor to watch and make sure the jobs started;
      # but we already set that up in the side-effects list for the query mock.

      # This is the real test: invoke create as if it had been called by the command line.
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        create(['west/mchucarroll/test/hello', fp.name])

      # Now check that the right API calls got made.
      assert mock_scheduler_proxy.createJob.call_count == 1
      assert len(hook.created_jobs) == 0
