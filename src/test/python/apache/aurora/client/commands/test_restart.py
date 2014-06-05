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
import functools

from mock import Mock, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.api.health_check import Retriable, StatusHealthCheck
from apache.aurora.client.commands.core import restart
from apache.aurora.client.commands.util import AuroraClientCommandTest

from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    JobKey,
    PopulateJobResult,
    ScheduledTask,
    ScheduleStatusResult,
    TaskConfig
)


class TestRestartCommand(AuroraClientCommandTest):

  @classmethod
  def setup_mock_options(cls):
    """set up to get a mock options object."""
    mock_options = Mock(
        spec=['json', 'bindings', 'open_browser', 'shards', 'cluster',
              'health_check_interval_seconds', 'batch_size', 'max_per_shard_failures',
              'max_total_failures', 'restart_threshold', 'watch_secs'])
    mock_options.json = False
    mock_options.bindings = {}
    mock_options.open_browser = False
    mock_options.shards = None
    mock_options.cluster = None
    mock_options.health_check_interval_seconds = 3
    mock_options.batch_size = 5
    mock_options.max_per_shard_failures = 0
    mock_options.max_total_failures = 0
    mock_options.restart_threshold = 30
    mock_options.watch_secs = 30
    mock_options.disable_all_hooks_reason = None
    return mock_options

  @classmethod
  def setup_mock_scheduler_for_simple_restart(cls, api):
    """Set up all of the API mocks for scheduler calls during a simple restart"""
    sched_proxy = api.scheduler_proxy
    cls.setup_get_tasks_status_calls(sched_proxy)
    cls.setup_populate_job_config(sched_proxy)
    sched_proxy.restartShards.return_value = cls.create_simple_success_response()

  @classmethod
  def setup_populate_job_config(cls, api):
    populate = cls.create_simple_success_response()
    populate.result.populateJobResult = Mock(spec=PopulateJobResult)
    api.populateJobConfig.return_value = populate
    configs = []
    for i in range(20):
      task_config = Mock(spec=TaskConfig)
      configs.append(task_config)
    populate.result.populateJobResult.populated = set(configs)
    return populate

  @classmethod
  def setup_get_tasks_status_calls(cls, scheduler):
    status_response = cls.create_simple_success_response()
    scheduler.getTasksStatus.return_value = status_response
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

  @classmethod
  def setup_health_checks(cls, mock_api):
    mock_health_check = Mock(spec=StatusHealthCheck)
    mock_health_check.health.return_value = Retriable.alive()
    return mock_health_check

  def test_restart_simple(self):
    # Test the client-side restart logic in its simplest case: everything succeeds
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    self.setup_mock_scheduler_for_simple_restart(mock_api)
    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('time.sleep', return_value=None)
    ) as (options, scheduler_proxy_class, test_clusters, mock_health_check_factory,
        time_patch, sleep_patch):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        restart(['west/mchucarroll/test/hello'], mock_options)

        # Like the update test, the exact number of calls here doesn't matter.
        # what matters is that it must have been called once before batching, plus
        # at least once per batch, and there are 4 batches.
        assert mock_scheduler_proxy.getTasksStatus.call_count >= 4
        # called once per batch
        assert mock_scheduler_proxy.restartShards.call_count == 4
        # parameters for all calls are generated by the same code, so we just check one
        mock_scheduler_proxy.restartShards.assert_called_with(JobKey(environment=self.TEST_ENV,
            role=self.TEST_ROLE, name=self.TEST_JOB), [15, 16, 17, 18, 19], None)

  def test_restart_simple_invalid_max_failures(self):
    # Test the client-side restart logic in its simplest case: everything succeeds
    mock_options = self.setup_mock_options()
    mock_options.max_total_failures = -1
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    self.setup_mock_scheduler_for_simple_restart(mock_api)
    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('time.sleep', return_value=None)
    ) as (options, scheduler_proxy_class, test_clusters, mock_health_check_factory,
        time_patch, sleep_patch):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        self.assertRaises(SystemExit, restart, ['west/mchucarroll/test/hello'], mock_options)

        # Like the update test, the exact number of calls here doesn't matter.
        # what matters is that it must have been called once before batching, plus
        # at least once per batch, and there are 4 batches.
        assert mock_scheduler_proxy.getTasksStatus.call_count == 0
        # called once per batch
        assert mock_scheduler_proxy.restartShards.call_count == 0

  def test_restart_failed_status(self):
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    self.setup_mock_scheduler_for_simple_restart(mock_api)
    mock_scheduler_proxy.getTasksStatus.return_value = self.create_error_response()
    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('time.sleep', return_value=None)
    ) as (options, scheduler_proxy_class, test_clusters, mock_health_check_factory,
        time_patch, sleep_patch):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        self.assertRaises(SystemExit, restart, ['west/mchucarroll/test/hello'], mock_options)
        assert mock_scheduler_proxy.getTasksStatus.call_count == 1
        assert mock_scheduler_proxy.restartShards.call_count == 0

  def test_restart_failed_restart(self):
    # Test the client-side updater logic in its simplest case: everything succeeds, and no rolling
    # updates.
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    self.setup_mock_scheduler_for_simple_restart(mock_api)
    mock_scheduler_proxy.restartShards.return_value = self.create_error_response()
    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('time.sleep', return_value=None)
    ) as (options, scheduler_proxy_class, test_clusters, mock_health_check_factory,
        time_patch, sleep_patch):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        self.assertRaises(SystemExit, restart, ['west/mchucarroll/test/hello'], mock_options)
        assert mock_scheduler_proxy.getTasksStatus.call_count == 1
        assert mock_scheduler_proxy.restartShards.call_count == 1
        mock_scheduler_proxy.restartShards.assert_called_with(JobKey(environment=self.TEST_ENV,
            role=self.TEST_ROLE, name=self.TEST_JOB), [0, 1, 2, 3, 4], None)
