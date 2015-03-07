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

import pytest
from mock import create_autospec, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.api import UpdaterConfig
from apache.aurora.client.api.health_check import Retriable, StatusHealthCheck
from apache.aurora.client.cli import Context, EXIT_API_ERROR, EXIT_INVALID_PARAMETER
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.jobs import RestartCommand
from apache.aurora.client.cli.options import TaskInstanceKey
from apache.aurora.common.aurora_job_key import AuroraJobKey

from .util import AuroraClientCommandTest, FakeAuroraCommandContext, IOMock, mock_verb_options

from gen.apache.aurora.api.ttypes import JobKey, PopulateJobResult, ResponseCode, Result, TaskConfig


class TestRestartJobCommand(AuroraClientCommandTest):

  def test_restart_with_lock(self):
    command = RestartCommand()

    jobkey = AuroraJobKey("cluster", "role", "env", "job")
    mock_options = mock_verb_options(command)
    mock_options.instance_spec = TaskInstanceKey(jobkey, [])

    fake_context = FakeAuroraCommandContext()
    fake_context.set_options(mock_options)

    mock_api = fake_context.get_api("test")
    mock_api.restart.return_value = AuroraClientCommandTest.create_blank_response(
      ResponseCode.LOCK_ERROR, "Error.")

    with pytest.raises(Context.CommandError):
      command.execute(fake_context)

    updater_config = UpdaterConfig(
      mock_options.batch_size,
      mock_options.restart_threshold,
      mock_options.watch_secs,
      mock_options.max_per_instance_failures,
      mock_options.max_total_failures)

    mock_api.restart.assert_called_once_with(jobkey, mock_options.instance_spec.instance,
      updater_config, mock_options.healthcheck_interval_seconds, config=None)
    self.assert_lock_message(fake_context)

  def test_restart_inactive_instance_spec(self):
    command = RestartCommand()

    jobkey = AuroraJobKey("cluster", "role", "env", "job")
    mock_options = mock_verb_options(command)
    mock_options.instance_spec = TaskInstanceKey(jobkey, [1])
    mock_options.strict = True

    fake_context = FakeAuroraCommandContext()
    fake_context.set_options(mock_options)

    fake_context.add_expected_query_result(AuroraClientCommandTest.create_empty_task_result())

    with pytest.raises(Context.CommandError) as e:
      command.execute(fake_context)
      assert e.message == "Invalid instance parameter: [1]"


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
    populate.result = Result(populateJobResult=PopulateJobResult(
        populatedDEPRECATED={TaskConfig()},
        taskConfig=TaskConfig()
    ))
    api.populateJobConfig.return_value = populate
    return populate

  @classmethod
  def setup_health_checks(cls, mock_api):
    mock_health_check = create_autospec(spec=StatusHealthCheck, instance=True)
    mock_health_check.health.return_value = Retriable.alive()
    return mock_health_check

  def test_restart_simple(self):
    # Test the client-side restart logic in its simplest case: everything succeeds
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    self.setup_mock_scheduler_for_simple_restart(mock_api)
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('threading._Event.wait')
    ):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'restart', '--batch-size=5', 'west/bozo/test/hello',
            '--config', fp.name])

        # Like the update test, the exact number of calls here doesn't matter.
        # what matters is that it must have been called once before batching, plus
        # at least once per batch, and there are 4 batches.
        assert mock_scheduler_proxy.getTasksWithoutConfigs.call_count >= 4
        # called once per batch
        assert mock_scheduler_proxy.restartShards.call_count == 4
        # parameters for all calls are generated by the same code, so we just check one
        mock_scheduler_proxy.restartShards.assert_called_with(JobKey(environment=self.TEST_ENV,
            role=self.TEST_ROLE, name=self.TEST_JOB), [15, 16, 17, 18, 19], None)

  def test_restart_simple_no_config(self):
    # Test the client-side restart logic in its simplest case: everything succeeds
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    self.setup_mock_scheduler_for_simple_restart(mock_api)
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('threading._Event.wait')
    ):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'restart', '--batch-size=5', 'west/bozo/test/hello'])

      assert mock_scheduler_proxy.getTasksWithoutConfigs.call_count >= 4
      assert mock_scheduler_proxy.restartShards.call_count == 4
      mock_scheduler_proxy.restartShards.assert_called_with(JobKey(environment=self.TEST_ENV,
          role=self.TEST_ROLE, name=self.TEST_JOB), [15, 16, 17, 18, 19], None)

  def test_restart_invalid_shards(self):
    # Test the client-side restart when a shard argument is too large, and it's
    # using strict mode.
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    self.setup_mock_scheduler_for_simple_restart(mock_api)
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('threading._Event.wait')
    ):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'restart', '--batch-size=5', '--max-total-failures=-1',
            'west/bozo/test/hello', '--config', fp.name])
        assert result == EXIT_INVALID_PARAMETER
        assert mock_scheduler_proxy.getTasksWithoutConfigs.call_count == 0
        assert mock_scheduler_proxy.restartShards.call_count == 0

  def test_restart_failed_status(self):
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    self.setup_mock_scheduler_for_simple_restart(mock_api)
    mock_scheduler_proxy.getTasksWithoutConfigs.return_value = self.create_error_response()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('threading._Event.wait')):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'restart', '--batch-size=5', 'west/bozo/test/hello',
            '--config', fp.name])
        assert mock_scheduler_proxy.getTasksWithoutConfigs.call_count == 1
        assert mock_scheduler_proxy.restartShards.call_count == 0
        assert result == EXIT_API_ERROR

  def test_restart_no_such_job_with_instances(self):
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    mock_io = IOMock()
    self.setup_mock_scheduler_for_simple_restart(mock_api)
    # Make getTasksWithoutConfigs return an error, which is what happens when a job is not found.
    mock_scheduler_proxy.getTasksWithoutConfigs.return_value = self.create_error_response()
    with contextlib.nested(
        patch('apache.aurora.client.cli.context.AuroraCommandContext.print_err',
              side_effect=mock_io.put),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('threading._Event.wait')):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'restart', '--batch-size=5', 'west/bozo/test/hello/1-3',
            '--config', fp.name])
        # We need to check tat getTasksWithoutConfigs was called, but that restartShards wasn't.
        # In older versions of the client, if shards were specified, but the job didn't
        # exist, the error wouldn't be detected unti0 restartShards was called, which generated
        # the wrong error message.
        assert mock_scheduler_proxy.getTasksWithoutConfigs.call_count == 1
        assert mock_scheduler_proxy.restartShards.call_count == 0
        assert result == EXIT_API_ERROR
        # Error message should be written to log, and it should be what was returned
        # by the getTasksWithoutConfigs call.
        assert mock_io.get() == ["Error restarting job west/bozo/test/hello:", "\tWhoops"]

  def test_restart_failed_restart(self):
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    self.setup_mock_scheduler_for_simple_restart(mock_api)
    mock_scheduler_proxy.restartShards.return_value = self.create_error_response()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('threading._Event.wait')):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'restart', '--batch-size=5', 'west/bozo/test/hello',
            '--config', fp.name])
        assert mock_scheduler_proxy.getTasksWithoutConfigs.call_count == 1
        assert mock_scheduler_proxy.restartShards.call_count == 1
        mock_scheduler_proxy.restartShards.assert_called_with(JobKey(environment=self.TEST_ENV,
            role=self.TEST_ROLE, name=self.TEST_JOB), [0, 1, 2, 3, 4], None)
        assert result == EXIT_API_ERROR

  MOCK_OUT = []
  MOCK_ERR = []

  @classmethod
  def reset_mock_io(cls):
    cls.MOCK_OUT = []
    cls.MOCK_ERR = []

  @classmethod
  def mock_print_out(cls, msg, indent=0):
    indent_str = " " * indent
    cls.MOCK_OUT.append("%s%s" % (indent_str, msg))

  @classmethod
  def mock_print_err(cls, msg, indent=0):
    indent_str = " " * indent
    cls.MOCK_ERR.append("%s%s" % (indent_str, msg))

  def test_restart_simple_output(self):
    self.reset_mock_io()
    # Test the client-side restart logic in its simplest case: everything succeeds
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    self.setup_mock_scheduler_for_simple_restart(mock_api)
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('threading._Event.wait'),
        patch('apache.aurora.client.cli.context.AuroraCommandContext.print_out',
            side_effect=self.mock_print_out),
        patch('apache.aurora.client.cli.context.AuroraCommandContext.print_err',
            side_effect=self.mock_print_err)
    ):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'restart', '--batch-size=5', 'west/bozo/test/hello',
            '--config', fp.name])
        assert self.MOCK_OUT == ['Job west/bozo/test/hello restarted successfully']
        assert self.MOCK_ERR == []

  def test_restart_failed_restart_output(self):
    self.reset_mock_io()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    self.setup_mock_scheduler_for_simple_restart(mock_api)
    mock_scheduler_proxy.restartShards.return_value = self.create_error_response()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('apache.aurora.client.cli.context.AuroraCommandContext.print_out',
            side_effect=self.mock_print_out),
        patch('apache.aurora.client.cli.context.AuroraCommandContext.print_err',
            side_effect=self.mock_print_err),
        patch('threading._Event.wait')):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'restart', '--batch-size=5', 'west/bozo/test/hello',
            '--config', fp.name])
      assert self.MOCK_OUT == []
      assert "Error restarting job west/bozo/test/hello:" in self.MOCK_ERR
