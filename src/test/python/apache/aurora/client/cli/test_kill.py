#
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

from mock import Mock, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.cli import EXIT_TIMEOUT
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.options import parse_instances
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext
from apache.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from apache.aurora.common.aurora_job_key import AuroraJobKey

from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    Identity,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskEvent,
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


class TestClientKillCommand(AuroraClientCommandTest):
  @classmethod
  def get_kill_job_response(cls):
    return cls.create_simple_success_response()

  @classmethod
  def assert_kill_job_called(cls, mock_api):
    assert mock_api.kill_job.call_count == 1

  @classmethod
  def assert_scheduler_called(cls, mock_api, mock_query, num_queries):
    assert mock_api.scheduler_proxy.getTasksStatus.call_count == num_queries
    mock_api.scheduler_proxy.getTasksStatus.assert_called_with(mock_query)

  @classmethod
  def get_expected_task_query(cls, instances=None):
    instance_ids = frozenset(instances) if instances is not None else None
    return TaskQuery(taskIds=None, jobName=cls.TEST_JOB, environment=cls.TEST_ENV,
                     instanceIds=instance_ids, owner=Identity(role=cls.TEST_ROLE, user=None))


  def test_killall_job(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    mock_scheduler_proxy = Mock()
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):

      api = mock_context.get_api('west')
      mock_scheduler_proxy.getTasksStatus.return_value = self.create_status_call_result()
      api.kill_job.return_value = self.get_kill_job_response()
      mock_scheduler_proxy.killTasks.return_value = self.get_kill_job_response()
      mock_context.add_expected_status_query_result(self.create_status_call_result(
          self.create_mock_task(ScheduleStatus.KILLED)))
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'killall', '--no-batching', '--config=%s' % fp.name, 'west/bozo/test/hello'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 1
      api.kill_job.assert_called_with(AuroraJobKey.from_path('west/bozo/test/hello'), None)
      self.assert_scheduler_called(api, self.get_expected_task_query(), 2)

  def test_killall_job_wait_until_timeout(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    mock_scheduler_proxy = Mock()
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):

      api = mock_context.get_api('west')
      mock_scheduler_proxy.getTasksStatus.return_value = self.create_status_call_result()
      api.kill_job.return_value = self.get_kill_job_response()
      mock_scheduler_proxy.killTasks.return_value = self.get_kill_job_response()
      for _ in range(8):
        mock_context.add_expected_status_query_result(self.create_status_call_result(
            self.create_mock_task(ScheduleStatus.RUNNING)))

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        assert EXIT_TIMEOUT == cmd.execute(
            ['job', 'killall', '--no-batching', '--config=%s' % fp.name, 'west/bozo/test/hello'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 1
      api.kill_job.assert_called_with(AuroraJobKey.from_path('west/bozo/test/hello'), None)
      self.assert_scheduler_called(api, self.get_expected_task_query(), 8)

  def test_killall_job(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    mock_scheduler_proxy = Mock()
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):

      api = mock_context.get_api('west')
      api.kill_job.return_value = self.get_kill_job_response()
      mock_context.add_expected_status_query_result(self.create_status_call_result())
      mock_scheduler_proxy.killTasks.return_value = self.get_kill_job_response()
      mock_context.add_expected_status_query_result(self.create_status_call_result(
          self.create_mock_task(ScheduleStatus.KILLED)))
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'killall', '--config=%s' % fp.name, 'west/bozo/test/hello'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 4
      instances = [15, 16, 17, 18, 19]
      api.kill_job.assert_called_with(AuroraJobKey.from_path('west/bozo/test/hello'), instances)
      self.assert_scheduler_called(api, self.get_expected_task_query(instances), 6)

  def test_kill_job_with_instances_nobatching(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      api = mock_context.get_api('west')
      self.setup_get_tasks_status_calls(api.scheduler_proxy)
      api.kill_job.return_value = self.get_kill_job_response()
      mock_context.add_expected_status_query_result(self.create_status_call_result(
          self.create_mock_task(ScheduleStatus.KILLED)))
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, '--no-batching', 'west/bozo/test/hello/0,2,4-6'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 1
      instances = [0, 2, 4, 5, 6]
      api.kill_job.assert_called_with(AuroraJobKey.from_path('west/bozo/test/hello'), instances)
      self.assert_scheduler_called(api, self.get_expected_task_query(instances), 2)

  def test_kill_job_with_invalid_instances_strict(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      api = mock_context.get_api('west')
      self.setup_get_tasks_status_calls(api.scheduler_proxy)
      api.kill_job.return_value = self.get_kill_job_response()
      mock_context.add_expected_status_query_result(self.create_status_call_result(
          self.create_mock_task(ScheduleStatus.KILLED)))
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, '--no-batching', '--strict',
            'west/bozo/test/hello/0,2,4-6,11-20'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 0


  def test_kill_job_with_invalid_instances_nonstrict(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      api = mock_context.get_api('west')
      self.setup_get_tasks_status_calls(api.scheduler_proxy)
      api.kill_job.return_value = self.get_kill_job_response()
      mock_context.add_expected_status_query_result(self.create_status_call_result(
          self.create_mock_task(ScheduleStatus.KILLED)))
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, '--no-batching',
            'west/bozo/test/hello/0,2,4-6,11-13'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 1
      instances = [0, 2, 4, 5, 6, 11, 12, 13]
      api.kill_job.assert_called_with(AuroraJobKey.from_path('west/bozo/test/hello'), instances)
      self.assert_scheduler_called(api, self.get_expected_task_query(instances), 2)


  def test_kill_job_with_instances_batched(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      api = mock_context.get_api('west')
      status_result = self.create_status_call_result()
      mock_context.add_expected_status_query_result(status_result)
      api.kill_job.return_value = self.get_kill_job_response()
      mock_context.add_expected_status_query_result(self.create_status_call_result(
          self.create_mock_task(ScheduleStatus.KILLED)))

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, 'west/bozo/test/hello/0,2,4-6'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 1
      instances = [0, 2, 4, 5, 6]
      api.kill_job.assert_called_with(AuroraJobKey.from_path('west/bozo/test/hello'), instances)
      # Expect total 3 calls (one from JobMonitor).
      self.assert_scheduler_called(api, self.get_expected_task_query(instances), 3)

  def test_kill_job_with_instances_batched_large(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      api = mock_context.get_api('west')
      status_result = self.create_status_call_result()
      mock_context.add_expected_status_query_result(status_result)
      api.kill_job.return_value = self.get_kill_job_response()
      mock_context.add_expected_status_query_result(self.create_status_call_result(
          self.create_mock_task(ScheduleStatus.KILLED)))

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, 'west/bozo/test/hello/0,2,4-13'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 3
      api.kill_job.assert_called_with(AuroraJobKey.from_path('west/bozo/test/hello'),
          [12, 13])
      # Expect total 5 calls (3 from JobMonitor).
      self.assert_scheduler_called(api, self.get_expected_task_query([12, 13]), 5)

  def test_kill_job_with_instances_batched_maxerrors(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      api = mock_context.get_api('west')
      status_result = self.create_status_call_result()
      mock_context.add_expected_status_query_result(status_result)
      api.kill_job.return_value = self.create_error_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--max-total-failures=1', '--config=%s' % fp.name, 'west/bozo/test/hello/0,2,4-13'])

      # Now check that the right API calls got made. We should have aborted after the second batch.
      assert api.kill_job.call_count == 2
      assert api.scheduler_proxy.getTasksStatus.call_count == 0

  def test_kill_job_with_empty_instances_batched(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      api = mock_context.get_api('west')
      # set up an empty instance list in the getTasksStatus response
      status_response = self.create_simple_success_response()
      schedule_status = Mock(spec=ScheduleStatusResult)
      status_response.result.scheduleStatusResult = schedule_status
      schedule_status.tasks = []
      mock_context.add_expected_status_query_result(status_response)
      api.kill_job.return_value = self.get_kill_job_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, 'west/bozo/test/hello/0,2,4-13'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 0


  def test_kill_job_with_instances_deep_api(self):
    """Test kill client-side API logic."""
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_scheduler_proxy.killTasks.return_value = self.get_kill_job_response()
      self.setup_get_tasks_status_calls(mock_scheduler_proxy)
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, 'west/bozo/test/hello/0,2,4-6'])
      # Now check that the right API calls got made.
      assert mock_scheduler_proxy.killTasks.call_count == 1
      mock_scheduler_proxy.killTasks.assert_called_with(
        TaskQuery(jobName='hello', environment='test', instanceIds=frozenset([0, 2, 4, 5, 6]),
            owner=Identity(role='bozo')), None)
