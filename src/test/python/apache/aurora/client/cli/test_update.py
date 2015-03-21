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
from mock import call, create_autospec, Mock, patch
from pystachio import Empty
from twitter.common.contextutil import temporary_file

from apache.aurora.client.api.health_check import Retriable, StatusHealthCheck
from apache.aurora.client.api.job_monitor import JobMonitor
from apache.aurora.client.api.quota_check import QuotaCheck
from apache.aurora.client.api.scheduler_mux import SchedulerMux
from apache.aurora.client.cli import Context, EXIT_INVALID_CONFIGURATION, EXIT_OK
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.jobs import CLIENT_UPDATER_DEPRECATION, UpdateCommand
from apache.aurora.client.cli.options import TaskInstanceKey
from apache.aurora.config import AuroraConfig
from apache.aurora.config.schema.base import Job
from apache.thermos.config.schema_base import MB, Process, Resources, Task

from .util import AuroraClientCommandTest, FakeAuroraCommandContext, IOMock, mock_verb_options

from gen.apache.aurora.api.constants import ACTIVE_STATES
from gen.apache.aurora.api.ttypes import (
    AcquireLockResult,
    AddInstancesConfig,
    AssignedTask,
    JobKey,
    Lock,
    PopulateJobResult,
    ResponseCode,
    Result,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskConfig,
    TaskQuery
)


class TestJobUpdateCommand(AuroraClientCommandTest):

  def setUp(self):
    self._command = UpdateCommand()
    self._mock_options = mock_verb_options(self._command)
    self._mock_options.instance_spec = TaskInstanceKey(self.TEST_JOBKEY, [])
    self._mock_options.force = True
    self._fake_context = FakeAuroraCommandContext()
    self._fake_context.set_options(self._mock_options)
    self._mock_api = self._fake_context.get_api("test")

  @classmethod
  def get_job_config(self, is_service=True, is_cron=False):
    return AuroraConfig(job=Job(
      cluster='west',
      role='bozo',
      environment='test',
      name='the_job',
      service=is_service,
      cron_schedule='* * * * *' if is_cron else Empty,
      task=Task(
        name='task',
        processes=[Process(cmdline='ls -la', name='process')],
        resources=Resources(cpu=1.0, ram=1024 * MB, disk=1024 * MB)
      ),
      contact='bozo@the.clown',
      instances=3,
    ))

  def test_update_with_lock(self):
    config = self.get_job_config()
    self._fake_context.get_job_config = Mock(return_value=config)
    self._mock_api.update_job.return_value = self.create_blank_response(
        ResponseCode.LOCK_ERROR, "Error.")

    with pytest.raises(Context.CommandError):
      self._command.execute(self._fake_context)

    self._mock_api.update_job.assert_called_once_with(
      config,
      self._mock_options.healthcheck_interval_seconds,
      self._mock_options.instance_spec.instance)
    self.assert_lock_message(self._fake_context)

  def test_update_print_error_once(self):
    config = self.get_job_config()
    self._fake_context.get_job_config = Mock(return_value=config)
    error = "Error printed once."
    self._mock_api.update_job.return_value = self.create_blank_response(
        ResponseCode.INVALID_REQUEST,
        error)

    with pytest.raises(Context.CommandErrorLogged):
      self._command.execute(self._fake_context)

    self._mock_api.update_job.assert_called_once_with(
      config,
      self._mock_options.healthcheck_interval_seconds,
      self._mock_options.instance_spec.instance)
    assert self._fake_context.get_err() == [
        CLIENT_UPDATER_DEPRECATION,
        "Update failed due to error:", "\t%s" % error
    ]

  def test_update_no_active_instance_check(self):
    self._mock_options.instance_spec = TaskInstanceKey(self.TEST_JOBKEY, [1])
    self._mock_options.strict = True

    config = self.get_job_config()
    self._fake_context.get_job_config = Mock(return_value=config)
    self._mock_api.update_job.return_value = self.create_simple_success_response()

    self._command.execute(self._fake_context)

    assert self._mock_api.update_job.mock_calls == [
        call(
            config,
            self._mock_options.healthcheck_interval_seconds,
            self._mock_options.instance_spec.instance)]

  def test_update_non_service(self):
    self._fake_context.get_job_config = Mock(return_value=self.get_job_config(is_service=False))

    # Command failure is the only expectation here, as the request was invalid.
    with pytest.raises(Context.CommandError):
      self._command.execute(self._fake_context)

  def test_update_cron(self):
    config = self.get_job_config(is_cron=True)
    self._fake_context.get_job_config = Mock(return_value=config)
    self._mock_api.update_job.return_value = self.create_simple_success_response()

    self._command.execute(self._fake_context)
    assert self._mock_api.update_job.mock_calls == [
        call(
            config,
            self._mock_options.healthcheck_interval_seconds,
            self._mock_options.instance_spec.instance)]


class TestUpdateCommand(AuroraClientCommandTest):
  class FakeSchedulerMux(SchedulerMux):
    def enqueue_and_wait(self, command, data, aggregator=None, timeout=None):
      return command([data])

    def terminate(self):
      pass

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
        fp.write(self.get_service_config())
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
  def setup_mock_scheduler_for_simple_update(cls, api, count=20):
    """Set up all of the API mocks for scheduler calls during a simple update"""
    sched_proxy = api.scheduler_proxy
    # First, the updater acquires a lock
    sched_proxy.acquireLock.return_value = cls.create_acquire_lock_response(ResponseCode.OK,
         'OK', 'token', False)
    # Then it gets the status of the tasks for the updating job.
    cls.setup_get_tasks_status_calls(sched_proxy)
    # Next, it needs to populate the update config.
    cls.setup_populate_job_config(sched_proxy, count)
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
  def setup_populate_job_config(cls, api, count=20):
    populate = cls.create_simple_success_response()
    config = TaskConfig(
        numCpus=1.0,
        ramMb=1,
        diskMb=1,
        job=JobKey(role='bozo', environment='test', name='hello'))
    populate.result = Result(populateJobResult=PopulateJobResult(taskConfig=config))
    api.populateJobConfig.return_value = populate
    return populate

  @classmethod
  def create_acquire_lock_response(cls, code, msg, token, rolling):
    """Set up the response to a startUpdate API call."""
    start_update_response = cls.create_blank_response(code, msg)
    start_update_response.result = Result(acquireLockResult=AcquireLockResult(
        lock=Lock(key='foo', token='token')))
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
    scheduler.getTasksWithoutConfigs.return_value = status_response
    task_config = TaskConfig(
        numCpus=1.0,
        ramMb=10,
        diskMb=1,
        job=JobKey(role='bozo', environment='test', name='hello'))

    # This should be a list of ScheduledTask's.
    tasks = []
    for i in range(20):
      task_status = create_autospec(spec=ScheduledTask, instance=True)
      task_status.assignedTask = create_autospec(spec=AssignedTask, instance=True)
      task_status.assignedTask.instanceId = i
      task_status.assignedTask.taskId = "Task%s" % i
      task_status.assignedTask.slaveId = "Slave%s" % i
      task_status.slaveHost = "Slave%s" % i
      task_status.assignedTask.task = task_config
      tasks.append(task_status)
    status_response.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=tasks))

  @classmethod
  def setup_health_checks(cls, mock_api):
    mock_health_check = create_autospec(spec=StatusHealthCheck, instance=True)
    mock_health_check.health.return_value = Retriable.alive()
    return mock_health_check

  @classmethod
  def setup_quota_check(cls):
    mock_quota_check = create_autospec(spec=QuotaCheck, instance=True)
    mock_quota_check.validate_quota_from_requested.return_value = (
        cls.create_simple_success_response())
    return mock_quota_check

  @classmethod
  def setup_job_monitor(cls):
    mock_job_monitor = create_autospec(spec=JobMonitor, instance=True)
    mock_job_monitor.wait_until.return_value = True
    return mock_job_monitor

  @classmethod
  def get_service_config(cls):
    return cls.get_test_config(
      cls.CONFIG_BASE,
      cls.TEST_CLUSTER,
      cls.TEST_ROLE,
      cls.TEST_ENV,
      cls.TEST_JOB,
      inner='service=True,')

  def test_updater_simple(self):
    # Test the client-side updater logic in its simplest case: everything succeeds,
    # and no rolling updates. (Rolling updates are covered by the updater tests.)
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    mock_quota_check = self.setup_quota_check()
    mock_job_monitor = self.setup_job_monitor()
    fake_mux = self.FakeSchedulerMux()
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
        patch('apache.aurora.client.api.updater.SchedulerMux', return_value=fake_mux),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('time.sleep', return_value=None),
        patch('threading._Event.wait')):
      with temporary_file() as fp:
        fp.write(self.get_service_config())
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

  def test_updater_simple_with_instances(self):
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    mock_quota_check = self.setup_quota_check()
    mock_job_monitor = self.setup_job_monitor()
    fake_mux = self.FakeSchedulerMux()
    self.setup_mock_scheduler_for_simple_update(mock_api)
    with contextlib.nested(
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('apache.aurora.client.api.updater.JobMonitor', return_value=mock_job_monitor),
        patch('apache.aurora.client.api.updater.QuotaCheck', return_value=mock_quota_check),
        patch('apache.aurora.client.api.updater.SchedulerMux', return_value=fake_mux),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('time.sleep', return_value=None),
        patch('threading._Event.wait')):
      with temporary_file() as fp:
        fp.write(self.get_service_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'update', 'west/bozo/test/hello/1', fp.name])

      mock_scheduler_proxy = mock_api.scheduler_proxy
      assert mock_scheduler_proxy.acquireLock.call_count == 1
      assert mock_scheduler_proxy.addInstances.call_count == 1
      assert mock_scheduler_proxy.killTasks.call_count == 1
      self.assert_correct_status_calls(mock_scheduler_proxy)
      assert mock_scheduler_proxy.releaseLock.call_count == 1

  def test_updater_simple_small_doesnt_warn(self):
    mock_out = IOMock()
    mock_err = IOMock()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    mock_quota_check = self.setup_quota_check()
    mock_job_monitor = self.setup_job_monitor()
    fake_mux = self.FakeSchedulerMux()
    self.setup_mock_scheduler_for_simple_update(mock_api)
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.AuroraCommandContext.print_out',
            side_effect=mock_out.put),
        patch('apache.aurora.client.cli.jobs.AuroraCommandContext.print_err',
            side_effect=mock_err.put),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('apache.aurora.client.api.updater.JobMonitor', return_value=mock_job_monitor),
        patch('apache.aurora.client.api.updater.QuotaCheck', return_value=mock_quota_check),
        patch('apache.aurora.client.api.updater.SchedulerMux', return_value=fake_mux),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('time.sleep', return_value=None),
        patch('threading._Event.wait')):
      with temporary_file() as fp:
        fp.write(self.get_service_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'update', 'west/bozo/test/hello', fp.name])
      assert mock_out.get() == ['Update completed successfully']
      assert mock_err.get() == [CLIENT_UPDATER_DEPRECATION]

  def test_updater_simple_large_does_warn(self):
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    mock_quota_check = self.setup_quota_check()
    mock_job_monitor = self.setup_job_monitor()
    fake_mux = self.FakeSchedulerMux()
    self.setup_mock_scheduler_for_simple_update(mock_api, count=2)
    config = self.get_service_config()
    config = config.replace("instances = 20", "instances = 2")
    with contextlib.nested(
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('apache.aurora.client.api.updater.JobMonitor', return_value=mock_job_monitor),
        patch('apache.aurora.client.api.updater.QuotaCheck', return_value=mock_quota_check),
        patch('apache.aurora.client.api.updater.SchedulerMux', return_value=fake_mux),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('threading._Event.wait')):
      with patch('time.sleep') as sleep:
        with temporary_file() as fp:
          fp.write(config)
          fp.flush()
          cmd = AuroraCommandLine()
          result = cmd.execute(['job', 'update', 'west/bozo/test/hello', fp.name])
          assert result == EXIT_OK
          sleep.assert_called_once_with(5)

  def test_large_with_instances_warn(self):
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    mock_quota_check = self.setup_quota_check()
    mock_job_monitor = self.setup_job_monitor()
    fake_mux = self.FakeSchedulerMux()
    self.setup_mock_scheduler_for_simple_update(mock_api, count=20)

    config = self.get_service_config()
    config = config.replace("instances = 20", "instances = 200")
    with contextlib.nested(
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('apache.aurora.client.api.updater.JobMonitor', return_value=mock_job_monitor),
        patch('apache.aurora.client.api.updater.QuotaCheck', return_value=mock_quota_check),
        patch('apache.aurora.client.api.updater.SchedulerMux', return_value=fake_mux),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('threading._Event.wait')):
      with patch('time.sleep') as sleep:
        with temporary_file() as fp:
          fp.write(config)
          fp.flush()
          cmd = AuroraCommandLine()
          cmd.execute(['job', 'update', 'west/bozo/test/hello/1,3,40-160', fp.name])
          sleep.assert_called_once_with(5)

  def test_large_with_instances_doesnt_warn(self):
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)
    mock_quota_check = self.setup_quota_check()
    mock_job_monitor = self.setup_job_monitor()
    fake_mux = self.FakeSchedulerMux()
    self.setup_mock_scheduler_for_simple_update(mock_api, count=20)
    config = self.get_valid_config()
    config = config.replace("instances = 20", "instances = 200")
    with contextlib.nested(
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.api.instance_watcher.StatusHealthCheck',
            return_value=mock_health_check),
        patch('apache.aurora.client.api.updater.JobMonitor', return_value=mock_job_monitor),
        patch('apache.aurora.client.api.updater.QuotaCheck', return_value=mock_quota_check),
        patch('apache.aurora.client.api.updater.SchedulerMux', return_value=fake_mux),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('threading._Event.wait')):
      with patch('time.sleep') as sleep:
        with temporary_file() as fp:
          fp.write(config)
          fp.flush()
          cmd = AuroraCommandLine()
          cmd.execute(['job', 'update', 'west/bozo/test/hello/1,3', fp.name])
          assert sleep.call_count == 0

  @classmethod
  def assert_correct_addinstance_calls(cls, api):
    assert api.addInstances.call_count == 20
    last_addinst = api.addInstances.call_args
    assert isinstance(last_addinst[0][0], AddInstancesConfig)
    assert last_addinst[0][0].instanceIds == frozenset([19])
    assert last_addinst[0][0].key == JobKey(environment='test', role='bozo', name='hello')

  @classmethod
  def assert_correct_killtask_calls(cls, api):
    assert api.killTasks.call_count == 20
    # Check the last call's parameters.
    api.killTasks.assert_called_with(
        TaskQuery(taskIds=None,
            jobKeys=[JobKey(role='bozo', environment='test', name='hello')],
            instanceIds=frozenset([19]),
            statuses=ACTIVE_STATES),
        Lock(key='foo', token='token'))

  @classmethod
  def assert_correct_status_calls(cls, api):
    # getTasksWithoutConfigs gets called a lot of times. The exact number isn't fixed; it loops
    # over the health checks until all of them pass for a configured period of time.
    # The minumum number of calls is 20: once before the tasks are restarted, and then
    # once for each batch of restarts (Since the batch size is set to 1, and the
    # total number of tasks is 20, that's 20 batches.)
    assert api.getTasksWithoutConfigs.call_count >= 4

    status_calls = api.getTasksWithoutConfigs.call_args_list
    for status_call in status_calls:
      status_call[0][0] == TaskQuery(
        taskIds=None,
        jobKeys=[JobKey(role='bozo', environment='test', name='hello')],
        statuses=set([ScheduleStatus.RUNNING]))

    # getTasksStatus is called only once to build an generate update instructions
    assert api.getTasksStatus.call_count == 1

    api.getTasksStatus.assert_called_once_with(TaskQuery(
      taskIds=None,
      jobKeys=[JobKey(role='bozo', environment='test', name='hello')],
      statuses=ACTIVE_STATES))
