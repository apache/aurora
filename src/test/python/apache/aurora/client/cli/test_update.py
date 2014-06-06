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
from apache.aurora.client.api.job_monitor import JobMonitor
from apache.aurora.client.api.quota_check import QuotaCheck
from apache.aurora.client.api.updater import Updater
from apache.aurora.client.cli import EXIT_INVALID_CONFIGURATION
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext
from apache.aurora.config import AuroraConfig

from gen.apache.aurora.api.constants import ACTIVE_STATES
from gen.apache.aurora.api.ttypes import (
    AcquireLockResult,
    AddInstancesConfig,
    AssignedTask,
    Identity,
    JobKey,
    PopulateJobResult,
    ResponseCode,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskConfig,
    TaskQuery
)


class TestUpdateCommand(AuroraClientCommandTest):

  @classmethod
  def setup_mock_options(cls):
    """set up to get a mock options object."""
    mock_options = Mock()
    mock_options.json = False
    mock_options.bindings = {}
    mock_options.open_browser = False
    mock_options.cluster = None
    mock_options.force = True
    mock_options.env = None
    mock_options.shards = None
    mock_options.health_check_interval_seconds = 3
    return mock_options

  @classmethod
  def setup_mock_updater(cls):
    return Mock(spec=Updater)

  # First, we pretend that the updater isn't really client-side, and test
  # that the client makes the right API call to the updated.
  def test_update_command_line_succeeds(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api('west')
      mock_api.update_job.return_value = self.create_simple_success_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'update', '--force', self.TEST_JOBSPEC, fp.name])

      assert mock_api.update_job.call_count == 1
      args, kwargs = mock_api.update_job.call_args
      assert isinstance(args[0], AuroraConfig)
      assert args[1] == 3
      assert args[2] is None

  def test_update_invalid_config(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api('west')
      with temporary_file() as fp:
        fp.write(self.get_invalid_config('invalid_field=False,'))
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'update', '--force', self.TEST_JOBSPEC, fp.name])
        assert result == EXIT_INVALID_CONFIGURATION
        assert mock_api.update_job.call_count == 0

  @classmethod
  def setup_mock_scheduler_for_simple_update(cls, api):
    """Set up all of the API mocks for scheduler calls during a simple update"""
    sched_proxy = api.scheduler_proxy
    # First, the updater acquires a lock
    sched_proxy.acquireLock.return_value = cls.create_acquire_lock_response(ResponseCode.OK,
         'OK', 'token', False)
    # Then it gets the status of the tasks for the updating job.
    cls.setup_get_tasks_status_calls(sched_proxy)
    # Next, it needs to populate the update config.
    cls.setup_populate_job_config(sched_proxy)
    # Then it does the update, which kills and restarts jobs, and monitors their
    # health with the status call.
    cls.setup_kill_tasks(sched_proxy)
    cls.setup_add_tasks(sched_proxy)
    # Finally, after successful health checks, it releases the lock.
    cls.setup_release_lock_response(sched_proxy)

  @classmethod
  def setup_add_tasks(cls, api):
    add_response = cls.create_simple_success_response()
    api.addInstances.return_value = add_response
    return add_response

  @classmethod
  def setup_kill_tasks(cls, api):
    kill_response = cls.create_simple_success_response()
    api.killTasks.return_value = kill_response
    return kill_response

  @classmethod
  def setup_populate_job_config(cls, api):
    populate = cls.create_simple_success_response()
    populate.result.populateJobResult = Mock(spec=PopulateJobResult)
    api.populateJobConfig.return_value = populate
    configs = [TaskConfig(numCpus=1.0, ramMb=1, diskMb=1) for i in range(20)]
    populate.result.populateJobResult.populated = set(configs)
    return populate

  @classmethod
  def create_acquire_lock_response(cls, code, msg, token, rolling):
    """Set up the response to a startUpdate API call."""
    start_update_response = cls.create_blank_response(code, msg)
    start_update_response.result.acquireLockResult = Mock(spec=AcquireLockResult)
    start_update_response.result.acquireLockResult.lock = "foo"
    start_update_response.result.acquireLockResult.updateToken = 'token'
    return start_update_response

  @classmethod
  def setup_release_lock_response(cls, api):
    """Set up the response to a startUpdate API call."""
    release_lock_response = cls.create_simple_success_response()
    api.releaseLock.return_value = release_lock_response
    return release_lock_response

  @classmethod
  def setup_get_tasks_status_calls(cls, scheduler):
    status_response = cls.create_simple_success_response()
    scheduler.getTasksStatus.return_value = status_response
    schedule_status = Mock(spec=ScheduleStatusResult)
    status_response.result.scheduleStatusResult = schedule_status
    task_config = TaskConfig(numCpus=1.0, ramMb=10, diskMb=1)
    # This should be a list of ScheduledTask's.
    schedule_status.tasks = []
    for i in range(20):
      task_status = Mock(spec=ScheduledTask)
      task_status.assignedTask = Mock(spec=AssignedTask)
      task_status.assignedTask.instanceId = i
      task_status.assignedTask.taskId = "Task%s" % i
      task_status.assignedTask.slaveId = "Slave%s" % i
      task_status.slaveHost = "Slave%s" % i
      task_status.assignedTask.task = task_config
      schedule_status.tasks.append(task_status)

  @classmethod
  def setup_health_checks(cls, mock_api):
    mock_health_check = Mock(spec=StatusHealthCheck)
    mock_health_check.health.return_value = Retriable.alive()
    return mock_health_check

  @classmethod
  def setup_quota_check(cls):
    mock_quota_check = Mock(spec=QuotaCheck)
    mock_quota_check.validate_quota_from_requested.return_value = (
        cls.create_simple_success_response())
    return mock_quota_check

  @classmethod
  def setup_job_monitor(cls):
    mock_job_monitor = Mock(spec=JobMonitor)
    mock_job_monitor.wait_until.return_value = True
    return mock_job_monitor

  def test_updater_simple(self):
    # Test the client-side updater logic in its simplest case: everything succeeds,
    # and no rolling updates. (Rolling updates are covered by the updated tests.)
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    mock_quota_check = self.setup_quota_check()
    mock_job_monitor = self.setup_job_monitor()
    self.setup_mock_scheduler_for_simple_update(mock_api)
    # This doesn't work, because:
    # - The mock_context stubs out the API.
    # - the test relies on using live code in the API.
    with contextlib.nested(
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('apache.aurora.client.api.updater.JobMonitor', return_value=mock_job_monitor),
        patch('apache.aurora.client.api.updater.QuotaCheck', return_value=mock_quota_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('time.sleep', return_value=None)):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'update', 'west/bozo/test/hello', fp.name])

      # We don't check all calls. The updater should be able to change. What's important
      # is that we verify the key parts of the update process, to have some confidence
      # that update did the right things.
      # Every update should:
      # check its options, acquire an update lock, check status,
      # kill the old tasks, start new ones, wait for the new ones to healthcheck,
      # and finally release the lock.
      # The kill/start should happen in rolling batches.
      mock_scheduler_proxy = mock_api.scheduler_proxy
      assert mock_scheduler_proxy.acquireLock.call_count == 1
      self.assert_correct_killtask_calls(mock_scheduler_proxy)
      self.assert_correct_addinstance_calls(mock_scheduler_proxy)
      self.assert_correct_status_calls(mock_scheduler_proxy)
      assert mock_scheduler_proxy.releaseLock.call_count == 1

  @classmethod
  def assert_correct_addinstance_calls(cls, api):
    assert api.addInstances.call_count == 4
    last_addinst = api.addInstances.call_args
    assert isinstance(last_addinst[0][0], AddInstancesConfig)
    assert last_addinst[0][0].instanceIds == frozenset([15, 16, 17, 18, 19])
    assert last_addinst[0][0].key == JobKey(environment='test', role='bozo', name='hello')

  @classmethod
  def assert_correct_killtask_calls(cls, api):
    assert api.killTasks.call_count == 4
    # Check the last call's parameters.
    api.killTasks.assert_called_with(
        TaskQuery(taskIds=None, jobName='hello', environment='test',
            instanceIds=frozenset([16, 17, 18, 19, 15]),
            owner=Identity(role=u'bozo', user=None),
           statuses=ACTIVE_STATES),
        'foo')

  @classmethod
  def assert_correct_status_calls(cls, api):
    # getTasksStatus gets called a lot of times. The exact number isn't fixed; it loops
    # over the health checks until all of them pass for a configured period of time.
    # The minumum number of calls is 5: once before the tasks are restarted, and then
    # once for each batch of restarts (Since the batch size is set to 5, and the
    # total number of jobs is 20, that's 4 batches.)
    assert api.getTasksStatus.call_count >= 5
    # In the first getStatus call, it uses an expansive query; in the rest, it only queries for
    # status RUNNING.
    status_calls = api.getTasksStatus.call_args_list
    assert status_calls[0][0][0] == TaskQuery(taskIds=None, jobName='hello', environment='test',
        owner=Identity(role=u'bozo', user=None),
        statuses=ACTIVE_STATES)
    for status_call in status_calls[1:]:
      status_call[0][0] == TaskQuery(taskIds=None, jobName='hello', environment='test',
          owner=Identity(role='bozo', user=None), statuses=set([ScheduleStatus.RUNNING]))
