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
import unittest

import pytest
from mock import call, Mock, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.api.job_monitor import JobMonitor
from apache.aurora.client.cli import Context
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.jobs import KillCommand
from apache.aurora.client.cli.options import parse_instances, TaskInstanceKey
from apache.aurora.common.aurora_job_key import AuroraJobKey

from .util import AuroraClientCommandTest, FakeAuroraCommandContext, mock_verb_options

from gen.apache.aurora.api.constants import ACTIVE_STATES
from gen.apache.aurora.api.ttypes import (
    JobKey,
    ResponseCode,
    Result,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskQuery
)


class TestInstancesParser(unittest.TestCase):
  def test_parse_instances(self):
    instances = '0,1-3,5'
    x = parse_instances(instances)
    assert x == [0, 1, 2, 3, 5]

  def test_parse_none(self):
    assert parse_instances(None) is None
    assert parse_instances("") is None


class TestKillCommand(AuroraClientCommandTest):

  def test_kill_lock_error_nobatch(self):
    """Verify that the no batch code path correctly includes the lock error message."""
    command = KillCommand()

    jobkey = AuroraJobKey("cluster", "role", "env", "job")

    mock_options = mock_verb_options(command)
    mock_options.instance_spec = TaskInstanceKey(jobkey, [])
    mock_options.no_batching = True

    fake_context = FakeAuroraCommandContext()
    fake_context.set_options(mock_options)

    mock_api = fake_context.get_api('test')
    mock_api.kill_job.return_value = AuroraClientCommandTest.create_blank_response(
      ResponseCode.LOCK_ERROR, "Error.")

    with pytest.raises(Context.CommandError):
      command.execute(fake_context)

    mock_api.kill_job.assert_called_once_with(jobkey, mock_options.instance_spec.instance)
    self.assert_lock_message(fake_context)

  def test_kill_lock_error_batches(self):
    """Verify that the batch kill path short circuits and includes the lock error message."""
    command = KillCommand()

    jobkey = AuroraJobKey("cluster", "role", "env", "job")

    mock_options = mock_verb_options(command)
    mock_options.instance_spec = TaskInstanceKey(jobkey, [1])
    mock_options.no_batching = False

    fake_context = FakeAuroraCommandContext()
    fake_context.set_options(mock_options)

    fake_context.add_expected_query_result(
      AuroraClientCommandTest.create_query_call_result(
        AuroraClientCommandTest.create_scheduled_task(1, ScheduleStatus.RUNNING)))

    mock_api = fake_context.get_api('test')
    mock_api.kill_job.return_value = AuroraClientCommandTest.create_blank_response(
      ResponseCode.LOCK_ERROR, "Error.")

    with pytest.raises(Context.CommandError):
      command.execute(fake_context)

    mock_api.kill_job.assert_called_once_with(jobkey, mock_options.instance_spec.instance)
    self.assert_lock_message(fake_context)

  def test_kill_inactive_instance_spec(self):
    """Verify the instance spec is validated in a batched kill."""
    command = KillCommand()

    jobkey = AuroraJobKey("cluster", "role", "env", "job")

    mock_options = mock_verb_options(command)
    mock_options.instance_spec = TaskInstanceKey(jobkey, [1])
    mock_options.no_batching = False
    mock_options.strict = True

    fake_context = FakeAuroraCommandContext()
    fake_context.set_options(mock_options)

    fake_context.add_expected_query_result(AuroraClientCommandTest.create_empty_task_result())

    with pytest.raises(Context.CommandError) as e:
      command.execute(fake_context)
    assert e.value.message == "Invalid instance parameter: [1]"

  def test_kill_batched_queries_active_instances(self):
    """Verify that the batch kill operates on active instances only."""
    command = KillCommand()

    jobkey = AuroraJobKey("cluster", "role", "env", "job")

    mock_options = mock_verb_options(command)
    mock_options.instance_spec = TaskInstanceKey(jobkey, [1])
    mock_options.no_batching = False

    fake_context = FakeAuroraCommandContext()
    fake_context.set_options(mock_options)

    fake_context.add_expected_query_result(AuroraClientCommandTest.create_empty_task_result())

    command.execute(fake_context)
    assert fake_context.get_err()[0] == "No tasks to kill found for job cluster/role/env/job"

  def test_kill_opens_url(self):
    """Verify the kill commands opens the job page if requested"""
    command = KillCommand()

    jobkey = AuroraJobKey("cluster", "role", "env", "job")

    mock_options = mock_verb_options(command)
    mock_options.instance_spec = TaskInstanceKey(jobkey, [1])
    mock_options.open_browser = True

    fake_context = FakeAuroraCommandContext()
    fake_context.set_options(mock_options)

    fake_context.add_expected_query_result(AuroraClientCommandTest.create_empty_task_result())

    command.execute(fake_context)

    assert self.mock_webbrowser.mock_calls == [
        call("http://something_or_other/scheduler/role/env/job")
    ]


class TestClientKillCommand(AuroraClientCommandTest):
  @classmethod
  def get_expected_task_query(cls, instances=None):
    instance_ids = frozenset(instances) if instances is not None else None
    return TaskQuery(taskIds=None,
                     instanceIds=instance_ids,
                     jobKeys=[JobKey(role=cls.TEST_ROLE,
                                     environment=cls.TEST_ENV,
                                     name=cls.TEST_JOB)])

  @classmethod
  def get_monitor_mock(cls, result=True):
    mock_monitor = Mock(spec=JobMonitor)
    mock_monitor.wait_until.return_value = result
    return mock_monitor

  @classmethod
  def assert_kill_calls(cls, api, instance_range=None, instances=None):
    if instances:
      kill_calls = [call(AuroraJobKey.from_path(cls.TEST_JOBSPEC), instances)]
    else:
      kill_calls = [call(AuroraJobKey.from_path(cls.TEST_JOBSPEC), [i]) for i in instance_range]
    assert api.kill_job.mock_calls == kill_calls

  @classmethod
  def assert_wait_calls(cls, mock_monitor, terminal, instance_range=None, instances=None):
    if instances:
      wait_calls = [call(terminal, instances=instances, with_timeout=True)]
    else:
      wait_calls = [call(terminal, instances=[i], with_timeout=True) for i in instance_range]
    assert mock_monitor.wait_until.mock_calls == wait_calls

  @classmethod
  def assert_kill_call_no_instances(cls, api):
    assert api.kill_job.mock_calls == call((AuroraJobKey.from_path(cls.TEST_JOBSPEC), None))

  @classmethod
  def assert_query(cls, fake_api):
    calls = [call(TaskQuery(jobKeys=[cls.TEST_JOBKEY.to_thrift()], statuses=ACTIVE_STATES))]
    assert fake_api.query_no_configs.mock_calls == calls

  def test_killall_job(self):
    """Test killall client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    mock_monitor = self.get_monitor_mock()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.cli.jobs.JobMonitor', return_value=mock_monitor)) as (_, m):

      api = mock_context.get_api('west')
      api.kill_job.return_value = self.create_simple_success_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'killall', '--no-batching', '--config=%s' % fp.name, self.TEST_JOBSPEC])

      self.assert_kill_call_no_instances(api)
      assert mock_monitor.wait_until.mock_calls == [
          call(m.terminal, instances=None, with_timeout=True)]

  def test_killall_job_batched(self):
    """Test killall command with batching."""
    mock_context = FakeAuroraCommandContext()
    mock_monitor = self.get_monitor_mock()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.cli.jobs.JobMonitor', return_value=mock_monitor)) as (_, m):

      api = mock_context.get_api('west')
      api.kill_job.return_value = self.create_simple_success_response()
      mock_context.add_expected_query_result(
          self.create_query_call_result(), job_key=self.TEST_JOBKEY)

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'killall', '--config=%s' % fp.name, self.TEST_JOBSPEC])

      self.assert_kill_calls(api, instance_range=range(20))
      self.assert_wait_calls(mock_monitor, m.terminal, instance_range=range(20))
      self.assert_query(api)

  def test_kill_job_with_instances_nobatching(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    mock_monitor = self.get_monitor_mock()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.cli.jobs.JobMonitor', return_value=mock_monitor)) as (_, m):
      api = mock_context.get_api('west')
      api.kill_job.return_value = self.create_simple_success_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, '--no-batching',
            self.get_instance_spec('0,2,4-6')])

      instances = [0, 2, 4, 5, 6]
      self.assert_kill_calls(api, instances=instances)
      self.assert_wait_calls(mock_monitor, m.terminal, instances=instances)

  def test_kill_job_with_invalid_instances_strict(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      api = mock_context.get_api('west')
      api.kill_job.return_value = self.create_simple_success_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, '--no-batching', '--strict',
             self.get_instance_spec('0,2,4-6,11-20')])

      assert api.kill_job.call_count == 0

  def test_kill_job_with_invalid_instances_nonstrict(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    mock_monitor = self.get_monitor_mock()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.cli.jobs.JobMonitor', return_value=mock_monitor)) as (_, m):
      api = mock_context.get_api('west')
      api.kill_job.return_value = self.create_simple_success_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, '--no-batching',
             self.get_instance_spec('0,2,4-6,11-13')])

      instances = [0, 2, 4, 5, 6, 11, 12, 13]
      self.assert_kill_calls(api, instances=instances)
      self.assert_wait_calls(mock_monitor, m.terminal, instances=instances)

  def test_kill_job_with_instances_batched(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    mock_monitor = self.get_monitor_mock()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.cli.jobs.JobMonitor', return_value=mock_monitor)) as (_, m):
      api = mock_context.get_api('west')
      mock_context.add_expected_query_result(
          self.create_query_call_result(), job_key=self.TEST_JOBKEY)

      api.kill_job.return_value = self.create_simple_success_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, self.get_instance_spec('0-6')])

      self.assert_kill_calls(api, instance_range=range(7))
      self.assert_wait_calls(mock_monitor, m.terminal, instance_range=range(7))
      self.assert_query(api)

  def test_kill_job_with_instances_batched_maxerrors(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    mock_monitor = self.get_monitor_mock(result=False)
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.cli.jobs.JobMonitor', return_value=mock_monitor)):
      api = mock_context.get_api('west')
      mock_context.add_expected_query_result(
          self.create_query_call_result(), job_key=self.TEST_JOBKEY)

      api.kill_job.return_value = self.create_simple_success_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--max-total-failures=1', '--config=%s' % fp.name,
            self.get_instance_spec('0-4')])

      # We should have aborted after the second batch.
      self.assert_kill_calls(api, instance_range=range(2))
      self.assert_query(api)

  def test_kill_job_with_empty_instances_batched(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      api = mock_context.get_api('west')
      # set up an empty instance list in the getTasksWithoutConfigs response
      status_response = self.create_simple_success_response()
      status_response.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=[]))
      mock_context.add_expected_query_result(status_response)
      api.kill_job.return_value = self.create_simple_success_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, self.get_instance_spec('0,2,4-13')])

      assert api.kill_job.call_count == 0

  def test_killall_job_output(self):
    """Test kill output."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.cli.jobs.JobMonitor', return_value=self.get_monitor_mock())):
      api = mock_context.get_api('west')
      api.kill_job.return_value = self.create_simple_success_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'killall', '--no-batching', '--config=%s' % fp.name, self.TEST_JOBSPEC])

      assert mock_context.get_out() == ['Job killall succeeded']
      assert mock_context.get_err() == []

  def test_kill_job_with_instances_batched_output(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.cli.jobs.JobMonitor', return_value=self.get_monitor_mock())):
      api = mock_context.get_api('west')
      mock_context.add_expected_query_result(self.create_query_call_result())
      api.kill_job.return_value = self.create_simple_success_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, '--batch-size=5',
                     self.get_instance_spec('0,2,4-6')])

    assert mock_context.get_out() == ['Successfully killed instances [0, 2, 4, 5, 6]',
        'Job kill succeeded']
    assert mock_context.get_err() == []

  def test_kill_job_with_instances_batched_maxerrors_output(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    mock_monitor = self.get_monitor_mock(result=False)
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.cli.jobs.JobMonitor', return_value=mock_monitor)):
      api = mock_context.get_api('west')
      mock_context.add_expected_query_result(self.create_query_call_result())
      api.kill_job.return_value = self.create_simple_success_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--max-total-failures=1', '--config=%s' % fp.name,
                     '--batch-size=5', self.get_instance_spec('0,2,4-13')])

      assert mock_context.get_out() == []
      assert mock_context.get_err() == [
         'Instances [0, 2, 4, 5, 6] were not killed in time',
         'Instances [7, 8, 9, 10, 11] were not killed in time',
         'Exceeded maximum number of errors while killing instances']
