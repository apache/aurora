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
from collections import namedtuple

from mock import call, create_autospec, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.api.health_check import Retriable, StatusHealthCheck
from apache.aurora.client.commands.core import restart

from .util import AuroraClientCommandTest

from gen.apache.aurora.api.constants import ACTIVE_STATES
from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    JobKey,
    PopulateJobResult,
    Result,
    ScheduledTask,
    ScheduleStatusResult,
    TaskConfig,
    TaskQuery
)


class FakeOptions(namedtuple('FakeOptions', ['max_total_failures',
     'disable_all_hooks_reason',
     'batch_size',
     'restart_threshold',
     'watch_secs',
     'max_per_shard_failures',
     'shards',
     'health_check_interval_seconds',
     'open_browser'])):

  def __new__(cls,
      max_total_failures=None,
      disable_all_hooks_reason=None,
      batch_size=None,
      restart_threshold=None,
      watch_secs=None,
      max_per_shard_failures=None,
      shards=None,
      health_check_interval_seconds=None,
      open_browser=None):
    return super(FakeOptions, cls).__new__(
        cls,
        max_total_failures,
        disable_all_hooks_reason,
        batch_size,
        restart_threshold,
        watch_secs,
        max_per_shard_failures,
        shards,
        health_check_interval_seconds,
        open_browser)


class TestRestartCommand(AuroraClientCommandTest):

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
    populate.result.populateJobResult = create_autospec(
        spec=PopulateJobResult,
        spec_set=False,
        instance=True,
        watch_secs=None)
    api.populateJobConfig.return_value = populate
    configs = []
    for i in range(20):
      task_config = create_autospec(spec=TaskConfig, instance=True)
      configs.append(task_config)
    populate.result.populateJobResult.populatedDEPRECATED = set(configs)
    return populate

  @classmethod
  def setup_get_tasks_status_calls(cls, scheduler):

    tasks = []
    for i in range(20):
      tasks.append(ScheduledTask(
        assignedTask=AssignedTask(
          slaveHost='slave%s' % i,
          instanceId=i,
          taskId='task%s' % i,
          slaveId='slave%s' % i,
          task=TaskConfig())))
    status_response = cls.create_simple_success_response()
    status_response.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=tasks))
    scheduler.getTasksWithoutConfigs.return_value = status_response

  @classmethod
  def setup_health_checks(cls, mock_api):
    mock_health_check = create_autospec(spec=StatusHealthCheck, instance=True)
    mock_health_check.health.return_value = Retriable.alive()
    return mock_health_check

  def test_restart_simple(self):
    options = FakeOptions(
        max_total_failures=1,
        batch_size=5,
        restart_threshold=10,
        watch_secs=10)
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    self.setup_mock_scheduler_for_simple_restart(mock_api)
    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=options),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('threading._Event.wait')):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        restart(['west/mchucarroll/test/hello'], options)

        # Like the update test, the exact number of calls here doesn't matter.
        # what matters is that it must have been called once before batching, plus
        # at least once per batch, and there are 4 batches.
        assert mock_scheduler_proxy.getTasksWithoutConfigs.call_count >= 4
        # called once per batch
        assert mock_scheduler_proxy.restartShards.call_count == 4
        # parameters for all calls are generated by the same code, so we just check one
        mock_scheduler_proxy.restartShards.assert_called_with(JobKey(environment=self.TEST_ENV,
            role=self.TEST_ROLE, name=self.TEST_JOB), [15, 16, 17, 18, 19], None)

  def test_restart_simple_invalid_max_failures(self):
    options = FakeOptions(
        max_total_failures=None,
        batch_size=5,
        restart_threshold=10,
        watch_secs=10)
    mock_api, mock_scheduler_proxy = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    self.setup_mock_scheduler_for_simple_restart(mock_api)
    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=options),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('threading._Event.wait')):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        self.assertRaises(SystemExit, restart, ['west/mchucarroll/test/hello'], options)
        assert mock_scheduler_proxy.mock_calls == []

  def test_restart_failed_status(self):
    options = FakeOptions()
    mock_api, mock_scheduler_proxy = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    self.setup_mock_scheduler_for_simple_restart(mock_api)
    mock_scheduler_proxy.getTasksWithoutConfigs.return_value = self.create_error_response()
    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=options),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('threading._Event.wait')
    ) as (options, scheduler_proxy_class, test_clusters, mock_health_check_factory,
        time_patch, sleep_patch):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        self.assertRaises(SystemExit, restart, ['west/mchucarroll/test/hello'], options)
        # TODO(wfarner): Spread this pattern further, as it flags unexpected method calls.
        assert mock_scheduler_proxy.mock_calls == [
          call.getTasksWithoutConfigs(
              TaskQuery(jobKeys=[JobKey('mchucarroll', 'test', 'hello')], statuses=ACTIVE_STATES))
        ]

  def test_restart_failed_restart(self):
    options = FakeOptions(
        max_total_failures=1,
        batch_size=5,
        restart_threshold=10,
        watch_secs=10)
    mock_api, mock_scheduler_proxy = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    self.setup_mock_scheduler_for_simple_restart(mock_api)
    mock_scheduler_proxy.restartShards.return_value = self.create_error_response()
    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=options),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('threading._Event.wait')):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        self.assertRaises(SystemExit, restart, ['west/mchucarroll/test/hello'], options)
        assert mock_scheduler_proxy.getTasksWithoutConfigs.call_count == 1
        assert mock_scheduler_proxy.restartShards.call_count == 1
        mock_scheduler_proxy.restartShards.assert_called_with(
            JobKey(environment=self.TEST_ENV, role=self.TEST_ROLE, name=self.TEST_JOB),
            [0, 1, 2, 3, 4],
            None)
