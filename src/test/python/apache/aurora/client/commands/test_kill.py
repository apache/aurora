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

from apache.aurora.client.commands.core import CoreCommandHook, kill, killall
from apache.aurora.client.commands.util import AuroraClientCommandTest
from apache.aurora.common.aurora_job_key import AuroraJobKey

from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    Identity,
    Response,
    ResponseCode,
    Result,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskEvent,
    TaskQuery
)


class TestClientKillCommand(AuroraClientCommandTest):

  @classmethod
  def setup_mock_options(cls):
    """set up to get a mock options object."""
    mock_options = Mock()
    mock_options.open_browser = False
    mock_options.shards = None
    mock_options.cluster = None
    mock_options.json = False
    mock_options.batch_size = None
    mock_options.max_total_failures = 1
    mock_options.disable_all_hooks = False
    return mock_options

  @classmethod
  def setup_mock_api_factory(cls):
    mock_api_factory, mock_api = cls.create_mock_api_factory()
    mock_api_factory.return_value.kill_job.return_value = cls.get_kill_job_response()
    return mock_api_factory

  @classmethod
  def get_kill_job_response(cls):
    return cls.create_simple_success_response()

  @classmethod
  def get_kill_job_error_response(cls):
    return cls.create_error_response()

  @classmethod
  def assert_kill_job_called(cls, mock_api):
    assert mock_api.kill_job.call_count == 1

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
    task = cls.create_mock_task('hello', 0, 1000, scheduleStatus)
    mock_query_result.result.scheduleStatusResult.tasks = [task]
    return mock_query_result

  @classmethod
  def assert_scheduler_called(cls, mock_api, mock_query, num_queries):
    assert mock_api.scheduler_proxy.getTasksWithoutConfigs.call_count == num_queries
    mock_api.scheduler_proxy.getTasksWithoutConfigs.assert_called_with(mock_query)

  def test_kill_job_tasks_not_killed_in_time(self):
    """Test kill timed out waiting in job monitor."""
    mock_options = self.setup_mock_options()
    mock_config = Mock()
    mock_config.hooks = []
    mock_config.raw.return_value.enable_hooks.return_value.get.return_value = False
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.killTasks.return_value = self.get_kill_job_response()
    mock_query_results = [
        self.create_mock_status_query_result(ScheduleStatus.RUNNING),
        self.create_mock_status_query_result(ScheduleStatus.KILLING),
        self.create_mock_status_query_result(ScheduleStatus.KILLING),
        self.create_mock_status_query_result(ScheduleStatus.KILLING),
        self.create_mock_status_query_result(ScheduleStatus.KILLING),
        self.create_mock_status_query_result(ScheduleStatus.KILLING),
        self.create_mock_status_query_result(ScheduleStatus.KILLING),
        self.create_mock_status_query_result(ScheduleStatus.KILLING),
    ]
    mock_scheduler_proxy.getTasksWithoutConfigs.side_effect = mock_query_results
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.factory.make_client', return_value=mock_api),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.commands.core.get_job_config', return_value=mock_config)):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        self.assertRaises(
            SystemExit, killall, ['west/mchucarroll/test/hello', fp.name], mock_options)

      # Now check that the right API calls got made.
      assert mock_scheduler_proxy.killTasks.call_count == 1
      query = self.get_expected_task_query()
      mock_scheduler_proxy.killTasks.assert_called_with(query, None)
      self.assert_scheduler_called(mock_api, query, 8)

  def test_kill_job_noshards_fail(self):
    mock_options = self.setup_mock_options()
    mock_config = Mock()
    mock_api_factory = self.setup_mock_api_factory()
    with contextlib.nested(
        patch('apache.aurora.client.commands.core.make_client_factory',
            return_value=mock_api_factory),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.commands.core.get_job_config', return_value=mock_config)):

      mock_api = mock_api_factory.return_value
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        self.assertRaises(SystemExit, kill, ['west/mchucarroll/test/hello', fp.name], mock_options)

      # Now check that the right API calls got made.
      mock_api.kill_job.call_count == 0

  def test_simple_successful_killall_job(self):
    """Run a test of the "kill" command against a mocked-out API:
    Verifies that the kill command sends the right API RPCs, and performs the correct
    tests on the result."""

    mock_options = self.setup_mock_options()
    mock_config = Mock()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_api.kill_job.return_value = self.get_kill_job_response()
    mock_scheduler_proxy.killTasks.return_value = self.get_kill_job_response()
    mock_query_results = [
        self.create_mock_status_query_result(ScheduleStatus.RUNNING),
        self.create_mock_status_query_result(ScheduleStatus.KILLING),
        self.create_mock_status_query_result(ScheduleStatus.KILLED),
    ]
    mock_scheduler_proxy.getTasksWithoutConfigs.side_effect = mock_query_results
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.commands.core.make_client',
            return_value=mock_api),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.commands.core.get_job_config', return_value=mock_config)):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        killall(['west/mchucarroll/test/hello', fp.name], mock_options)

      # Now check that the right API calls got made.
      self.assert_kill_job_called(mock_api)
      mock_api.kill_job.assert_called_with(
        AuroraJobKey(cluster=self.TEST_CLUSTER, role=self.TEST_ROLE, env=self.TEST_ENV,
            name=self.TEST_JOB), None, config=mock_config)
      self.assert_scheduler_called(mock_api, self.get_expected_task_query(), 3)

  def test_happy_hook(self):
    """Test that hooks that return 0 don't block command execution"""

    class HappyHook(CoreCommandHook):
      @property
      def name(self):
        return "I'm so happy"

      def execute(self, cmd, options, *args, **kwargs):
        return 0

    CoreCommandHook.register_hook(HappyHook())


    mock_options = self.setup_mock_options()
    mock_config = Mock()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_api.kill_job.return_value = self.get_kill_job_response()
    mock_scheduler_proxy.killTasks.return_value = self.get_kill_job_response()
    mock_query_results = [
        self.create_mock_status_query_result(ScheduleStatus.RUNNING),
        self.create_mock_status_query_result(ScheduleStatus.KILLING),
        self.create_mock_status_query_result(ScheduleStatus.KILLED),
    ]
    mock_scheduler_proxy.getTasksWithoutConfigs.side_effect = mock_query_results
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.commands.core.make_client',
            return_value=mock_api),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.commands.core.get_job_config', return_value=mock_config)) as (
            sleep,
            mock_make_client,
            options,
            mock_get_job_config):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        killall(['west/mchucarroll/test/hello', fp.name], mock_options)

      # Now check that the right API calls got made.
      self.assert_kill_job_called(mock_api)
      mock_api.kill_job.assert_called_with(
        AuroraJobKey(cluster=self.TEST_CLUSTER, role=self.TEST_ROLE, env=self.TEST_ENV,
            name=self.TEST_JOB), None, config=mock_config)
      self.assert_scheduler_called(mock_api, self.get_expected_task_query(), 3)
      CoreCommandHook.clear_hooks()



  def test_hook_aborts_kill(self):
    """Test that a command hook that returns non-zero does block command execution."""
    class FailingKillHook(CoreCommandHook):
      @property
      def name(self):
        return "failure"

      def execute(self, cmd, options, *args, **kwargs):
        if cmd == "killall":
          assert args[0] == 'west/mchucarroll/test/hello'
          return 1
        else:
          return 0

    CoreCommandHook.register_hook(FailingKillHook())

    mock_options = self.setup_mock_options()
    mock_config = Mock()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.commands.core.make_client',
            return_value=mock_api),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.commands.core.get_job_config', return_value=mock_config)) as (
            sleep,
            mock_make_client,
            options,
            mock_get_job_config):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        self.assertRaises(SystemExit, killall, ['west/mchucarroll/test/hello', fp.name], mock_options)

      CoreCommandHook.clear_hooks()
      mock_api.kill_job.call_count == 0



  def create_status_call_result(cls):
    """Set up the mock status call that will be used to get a task list for
    a batched kill command.
    """
    status_response = Mock(spec=Response)
    status_response.responseCode = ResponseCode.OK
    status_response.messageDEPRECATED = "Ok"
    status_response.result = Mock(spec=Result)
    schedule_status = Mock(spec=ScheduleStatusResult)
    status_response.result.scheduleStatusResult = schedule_status
    mock_task_config = Mock()
    # This should be a list of ScheduledTask's.
    schedule_status.tasks = []
    for i in range(20):
      task_status = Mock(spec=ScheduledTask)
      task_status.assignedTask = Mock(spec=AssignedTask)
      task_status.assignedTask.instanceId = i
      task_status.assignedTask.taskId = "Task%s" % i
      task_status.assignedTask.slaveId = "Slave%s" % i
      task_status.slaveHost = "Slave%s" % i
      task_status.assignedTask.task = mock_task_config
      schedule_status.tasks.append(task_status)
    return status_response

  def test_successful_batched_killall_job(self):
    """Run a test of the "kill" command against a mocked-out API:
    Verifies that the kill command sends the right API RPCs, and performs the correct
    tests on the result."""

    mock_options = self.setup_mock_options()
    mock_options.batch_size = 5
    mock_config = Mock()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_api.kill_job.return_value = self.get_kill_job_response()
    mock_api.check_status.return_value = self.create_status_call_result()
    with contextlib.nested(
        patch('apache.aurora.client.commands.core.make_client',
            return_value=mock_api),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.commands.core.get_job_config', return_value=mock_config),
        patch('apache.aurora.client.commands.core.JobMonitor')):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        killall(['west/mchucarroll/test/hello', fp.name], mock_options)

      # Now check that the right API calls got made.
      assert mock_api.kill_job.call_count == 4
      mock_api.kill_job.assert_called_with(
        AuroraJobKey(cluster=self.TEST_CLUSTER, role=self.TEST_ROLE, env=self.TEST_ENV,
            name=self.TEST_JOB), [15, 16, 17, 18, 19])

  @classmethod
  def get_expected_task_query(cls, shards=None):
    """Helper to create the query that will be a parameter to job kill."""
    instance_ids = frozenset(shards) if shards is not None else None
    return TaskQuery(taskIds=None, jobName=cls.TEST_JOB, environment=cls.TEST_ENV,
        instanceIds=instance_ids, owner=Identity(role=cls.TEST_ROLE, user=None))

  def test_kill_job_api_level(self):
    """Test kill client-side API logic."""
    mock_options = self.setup_mock_options()
    mock_config = Mock()
    mock_config.hooks = []
    mock_config.raw.return_value.enable_hooks.return_value.get.return_value = False
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.killTasks.return_value = self.get_kill_job_response()
    mock_query_results = [
        self.create_mock_status_query_result(ScheduleStatus.RUNNING),
        self.create_mock_status_query_result(ScheduleStatus.KILLING),
        self.create_mock_status_query_result(ScheduleStatus.KILLED),
    ]
    mock_scheduler_proxy.getTasksWithoutConfigs.side_effect = mock_query_results
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.factory.make_client', return_value=mock_api),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.commands.core.get_job_config', return_value=mock_config)):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        killall(['west/mchucarroll/test/hello', fp.name], mock_options)

      # Now check that the right API calls got made.
      assert mock_scheduler_proxy.killTasks.call_count == 1
      query = self.get_expected_task_query()
      mock_scheduler_proxy.killTasks.assert_called_with(query, None)
      self.assert_scheduler_called(mock_api, query, 3)

  def test_kill_job_api_level_with_shards(self):
    """Test kill client-side API logic."""
    mock_options = self.setup_mock_options()
    mock_options.shards = [0, 1, 2, 3]
    mock_config = Mock()
    mock_config.hooks = []
    mock_config.raw.return_value.enable_hooks.return_value.get.return_value = False
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.killTasks.return_value = self.get_kill_job_response()
    mock_query_results = [
        self.create_mock_status_query_result(ScheduleStatus.RUNNING),
        self.create_mock_status_query_result(ScheduleStatus.KILLING),
        self.create_mock_status_query_result(ScheduleStatus.KILLED),
    ]
    mock_scheduler_proxy.getTasksWithoutConfigs.side_effect = mock_query_results
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.factory.make_client', return_value=mock_api),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.commands.core.get_job_config', return_value=mock_config)):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        kill(['west/mchucarroll/test/hello', fp.name], mock_options)

      # Now check that the right API calls got made.
      assert mock_scheduler_proxy.killTasks.call_count == 1
      query = self.get_expected_task_query([0, 1, 2, 3])
      mock_scheduler_proxy.killTasks.assert_called_with(query, None)
      self.assert_scheduler_called(mock_api, query, 3)

  def test_kill_job_api_level_with_shards_batched(self):
    """Test kill client-side API logic."""
    mock_options = self.setup_mock_options()
    mock_options.batch_size = 2
    mock_options.shards = [0, 1, 2, 3]
    mock_config = Mock()
    mock_config.hooks = []
    mock_config.raw.return_value.enable_hooks.return_value.get.return_value = False
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_api.check_status.return_value = self.create_status_call_result()
    mock_scheduler_proxy.getTasksWithoutConfigs.return_value = self.create_status_call_result()
    mock_scheduler_proxy.killTasks.return_value = self.get_kill_job_response()
    with contextlib.nested(
        patch('apache.aurora.client.factory.make_client', return_value=mock_api),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.commands.core.get_job_config', return_value=mock_config),
        patch('apache.aurora.client.commands.core.JobMonitor')):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        kill(['west/mchucarroll/test/hello', fp.name], mock_options)

      # Now check that the right API calls got made.
      assert mock_scheduler_proxy.killTasks.call_count == 2
      query = self.get_expected_task_query([2, 3])
      mock_scheduler_proxy.killTasks.assert_called_with(query, None)

  def test_kill_job_api_level_with_shards_batched_and_some_errors(self):
    """Test kill client-side API logic."""
    mock_options = self.setup_mock_options()
    mock_options.batch_size = 2
    mock_options.shards = [0, 1, 2, 3]
    mock_config = Mock()
    mock_config.hooks = []
    mock_config.raw.return_value.enable_hooks.return_value.get.return_value = False
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_api.check_status.return_value = self.create_status_call_result()
    mock_scheduler_proxy.getTasksWithoutConfigs.return_value = self.create_status_call_result()
    mock_api.kill_job.side_effect = [
        self.get_kill_job_error_response(), self.get_kill_job_response()]
    with contextlib.nested(
        patch('apache.aurora.client.factory.make_client', return_value=mock_api),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.commands.core.get_job_config', return_value=mock_config)):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        # We should get an exception in this case, because the one of the two calls fails.
        self.assertRaises(SystemExit, kill, ['west/mchucarroll/test/hello', fp.name], mock_options)

      # killTasks should still have gotten called twice - the first error shouldn't abort
      # the second batch.
      assert mock_scheduler_proxy.killTasks.call_count == 2
      query = self.get_expected_task_query([2, 3])
      mock_scheduler_proxy.killTasks.assert_called_with(query, None)
