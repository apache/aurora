from copy import deepcopy
from os import environ
from unittest import TestCase

from twitter.aurora.client.api.instance_watcher import InstanceWatcher
from twitter.aurora.client.api.updater import Updater
from twitter.aurora.client.fake_scheduler_proxy import FakeSchedulerProxy

from gen.twitter.aurora.AuroraSchedulerManager import Client as scheduler_client
from gen.twitter.aurora.constants import ACTIVE_STATES
from gen.twitter.aurora.ttypes import (
  AddInstancesConfig,
  AcquireLockResult,
  AssignedTask,
  Constraint,
  ExecutorConfig,
  JobConfiguration,
  JobConfigValidation,
  JobKey,
  Identity,
  Lock,
  LockKey,
  LockValidation,
  Package,
  PopulateJobResult,
  Response,
  ResponseCode,
  Result,
  ScheduleStatusResult,
  ScheduledTask,
  TaskConfig,
  TaskQuery,
)

from mox import MockObject, Replay, Verify
from pytest import raises


# Debug output helper -> enables log.* in source.
if 'UPDATER_DEBUG' in environ:
  from twitter.common import log
  from twitter.common.log.options import LogOptions
  LogOptions.set_disk_log_level('NONE')
  LogOptions.set_stderr_log_level('DEBUG')
  log.init('test_updater')

class FakeConfig(object):
  def __init__(self, role, name, env, update_config):
    self._role = role
    self._env = env
    self._name = name
    self._update_config = update_config
    self.job_config = None

  def role(self):
    return self._role

  def name(self):
    return self._name

  def update_config(self):
    class Anon(object):
      def get(_):
        return self._update_config
    return Anon()

  def has_health_port(self):
    return False

  def cluster(self):
    return 'test'

  def environment(self):
    return self._env

  def job(self):
    return self.job_config

  def instances(self):
    return self.job_config.instanceCount


class UpdaterTest(TestCase):
  UPDATE_CONFIG = {
    'batch_size':                 3,
    'restart_threshold':          50,
    'watch_secs':                 50,
    'max_per_shard_failures':     0,
    'max_total_failures':         0,
  }

  def setUp(self):
    self._role = 'mesos'
    self._name = 'jimbob'
    self._env = 'test'
    self._job_key = JobKey(name=self._name, environment=self._env, role=self._role)
    self._session_key = 'test_session'
    self._lock = 'test_lock'
    self._instance_watcher = MockObject(InstanceWatcher)
    self._scheduler = MockObject(scheduler_client)
    self._scheduler_proxy = FakeSchedulerProxy('test-cluster', self._scheduler, self._session_key)
    self.init_updater(deepcopy(self.UPDATE_CONFIG))

  def replay_mocks(self):
    Replay(self._scheduler)
    Replay(self._instance_watcher)

  def verify_mocks(self):
    Verify(self._scheduler)
    Verify(self._instance_watcher)

  def init_updater(self, update_config):
    self._config = FakeConfig(self._role, self._name, self._env, update_config)
    self._updater = Updater(self._config, 3, self._scheduler_proxy, self._instance_watcher)

  def expect_watch_instances(self, instance_ids, failed_instances=[]):
    self._instance_watcher.watch(instance_ids).AndReturn(set(failed_instances))

  def expect_populate(self, job_config, response_code=None):
    response_code = ResponseCode.OK if response_code is None else response_code
    resp = Response(responseCode=response_code, message='test')
    result = set([deepcopy(job_config.taskConfig)])
    resp.result = Result(populateJobResult=PopulateJobResult(populated=result))
    self._scheduler.populateJobConfig(job_config, JobConfigValidation.RUN_FILTERS).AndReturn(resp)

  def expect_get_tasks(self, tasks, ignore_ids=None, response_code=None):
    response_code = ResponseCode.OK if response_code is None else response_code
    response = Response(responseCode=response_code, message='test')
    scheduled = []
    for index, task in enumerate(tasks):
      if not ignore_ids or index not in ignore_ids:
        scheduled.append(ScheduledTask(assignedTask=AssignedTask(task=task, instanceId=index)))
    response.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=scheduled))
    query = TaskQuery(
        owner=Identity(role=self._job_key.role),
        environment=self._job_key.environment,
        jobName=self._job_key.name,
        statuses=ACTIVE_STATES)
    self._scheduler.getTasksStatus(query).AndReturn(response)

  def expect_cron_replace(self, job_config, response_code=None):
    response_code = ResponseCode.OK if response_code is None else response_code
    resp = Response(responseCode=response_code, message='test')
    self._scheduler.replaceCronTemplate(job_config, self._lock, self._session_key).AndReturn(resp)

  def expect_restart(self, instance_ids, response_code=None):
    response_code = ResponseCode.OK if response_code is None else response_code
    response = Response(responseCode=response_code, message='test')
    self._scheduler.restartShards(
        self._job_key,
        instance_ids,
        self._lock,
        self._session_key).AndReturn(response)

  def expect_kill(self, instance_ids, response_code=None):
    response_code = ResponseCode.OK if response_code is None else response_code
    response = Response(responseCode=response_code, message='test')
    query = TaskQuery(
        owner=Identity(role=self._job_key.role),
        environment=self._job_key.environment,
        jobName=self._job_key.name,
        statuses=ACTIVE_STATES,
        instanceIds=frozenset([int(s) for s in instance_ids]))
    self._scheduler.killTasks(query, self._lock, self._session_key).AndReturn(response)

  def expect_add(self, instance_ids, task_config, response_code=None):
    response_code = ResponseCode.OK if response_code is None else response_code
    response = Response(responseCode=response_code, message='test')
    add_config = AddInstancesConfig(
        key=self._job_key,
        taskConfig=task_config,
        instanceIds=frozenset([int(s) for s in instance_ids]))
    self._scheduler.addInstances(add_config, self._lock, self._session_key).AndReturn(response)

  def expect_start(self, response_code=None):
    response_code = ResponseCode.OK if response_code is None else response_code
    response = Response(responseCode=response_code, message='test')
    response.result = Result(acquireLockResult=AcquireLockResult(lock=self._lock))
    self._scheduler.acquireLock(LockKey(job=self._job_key), self._session_key).AndReturn(response)

  def expect_finish(self, response_code=None):
    response_code = ResponseCode.OK if response_code is None else response_code
    response = Response(responseCode=response_code, message='test')
    self._scheduler.releaseLock(
        self._lock,
        LockValidation.CHECKED,
        self._session_key).AndReturn(response)

  def assert_response_code(self, expected_code, actual_resp):
    assert expected_code == actual_resp.responseCode, (
      'Expected response:%s Actual response:%s' % (expected_code, actual_resp.responseCode))

  def make_task_configs(self, count=1):
    return [TaskConfig(
        owner=Identity(role=self._job_key.role),
        environment=self._job_key.environment,
        jobName=self._job_key.name,
        numCpus=6.0,
        ramMb=1024,
        diskMb=2048,
        priority=0,
        maxTaskFailures=1,
        production=True,
        taskLinks={'task': 'link'},
        contactEmail='foo@bar.com',
        executorConfig=ExecutorConfig(name='test', data='test data')
        # Not setting any set()-related properties as that throws off mox verification.
    )] * count

  def make_job_config(self, task_config, instance_count, cron_schedule=None):
    return JobConfiguration(
        key=self._job_key,
        owner=Identity(role=self._job_key.role),
        cronSchedule=cron_schedule,
        taskConfig=task_config,
        instanceCount=instance_count
    )

  def test_grow(self):
    """Adds instances to the existing job."""
    old_configs = self.make_task_configs(3)
    new_config = old_configs[0]
    job_config = self.make_job_config(new_config, 7)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_add([3, 4, 5], new_config)
    self.expect_watch_instances([3, 4, 5])
    self.expect_add([6], new_config)
    self.expect_watch_instances([6])
    self.expect_finish()
    self.replay_mocks()

    self._updater.update()
    self.verify_mocks()

  def test_shrink(self):
    """Reduces the number of instances of the job."""
    old_configs = self.make_task_configs(10)
    new_config = old_configs[0]
    job_config = self.make_job_config(new_config, 3)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_kill([3, 4, 5])
    self.expect_kill([6, 7, 8])
    self.expect_kill([9])
    self.expect_finish()
    self.replay_mocks()

    self._updater.update()
    self.verify_mocks()

  def test_update_and_grow(self):
    """Updates existing instances and adds new ones."""
    old_configs = self.make_task_configs(3)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 7)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_kill([0, 1, 2])
    self.expect_add([0, 1, 2], new_config)
    self.expect_watch_instances([0, 1, 2])
    self.expect_add([3, 4, 5], new_config)
    self.expect_watch_instances([3, 4, 5])
    self.expect_add([6], new_config)
    self.expect_watch_instances([6])
    self.expect_finish()
    self.replay_mocks()

    self._updater.update()
    self.verify_mocks()

  def test_update_and_shrink(self):
    """Updates some existing instances and reduce the instance count."""
    old_configs = self.make_task_configs(10)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 1)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_kill([0, 1, 2])
    self.expect_add([0], new_config)
    self.expect_watch_instances([0])
    self.expect_kill([3, 4, 5])
    self.expect_kill([6, 7, 8])
    self.expect_kill([9])
    self.expect_finish()
    self.replay_mocks()

    self._updater.update()
    self.verify_mocks()

  def test_update_instances(self):
    """Update existing instances."""
    old_configs = self.make_task_configs(5)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 5)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_kill([0, 1, 2])
    self.expect_add([0, 1, 2], new_config)
    self.expect_watch_instances([0, 1, 2])
    self.expect_kill([3, 4])
    self.expect_add([3, 4], new_config)
    self.expect_watch_instances([3, 4])
    self.expect_finish()
    self.replay_mocks()

    self._updater.update()
    self.verify_mocks()

  def test_grow_with_instance_option(self):
    """Adding instances by providing an optional list of instance IDs."""
    old_configs = self.make_task_configs(3)
    new_config = old_configs[0]
    job_config = self.make_job_config(new_config, 5)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_add([3, 4], new_config)
    self.expect_watch_instances([3, 4])
    self.expect_finish()
    self.replay_mocks()

    self._updater.update([3, 4])
    self.verify_mocks()

  def test_shrink_with_instance_option(self):
    """Reducing instance count by providing an optional list of instance IDs."""
    old_configs = self.make_task_configs(10)
    new_config = old_configs[0]
    job_config = self.make_job_config(new_config, 4)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_kill([4, 5, 6])
    self.expect_kill([7, 8, 9])
    self.expect_finish()
    self.replay_mocks()

    self._updater.update([4, 5, 6, 7, 8, 9])
    self.verify_mocks()

  def test_update_with_instance_option(self):
    """Updating existing instances by providing an optional list of instance IDs."""
    old_configs = self.make_task_configs(10)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 10)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_kill([2, 3, 4])
    self.expect_add([2, 3, 4], new_config)
    self.expect_watch_instances([2, 3, 4])
    self.expect_finish()
    self.replay_mocks()

    self._updater.update([2, 3, 4])
    self.verify_mocks()


  def test_patch_hole_with_instance_option(self):
    """Patching an instance ID gap created by a terminated update."""
    old_configs = self.make_task_configs(8)
    new_config = old_configs[0]
    job_config = self.make_job_config(new_config, 10)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs, [2, 3])
    self.expect_populate(job_config)
    self.expect_add([2, 3], new_config)
    self.expect_watch_instances([2, 3])
    self.expect_finish()
    self.replay_mocks()

    self._updater.update([2, 3])
    self.verify_mocks()

  def test_noop_update(self):
    """No update calls happen if task configs are in sync."""
    old_configs = self.make_task_configs(5)
    new_config = old_configs[0]
    job_config = self.make_job_config(new_config, 5)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_finish()
    self.replay_mocks()

  def test_update_rollback(self):
    """Update process failures exceed total allowable count and update is rolled back."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=2, max_per_shard_failures=1)
    self.init_updater(update_config)

    old_configs = self.make_task_configs(10)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 10)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_kill([0, 1, 2])
    self.expect_add([0, 1, 2], new_config)
    self.expect_watch_instances([0, 1, 2], failed_instances=[0, 1, 2])
    self.expect_restart([0, 1, 2])
    self.expect_watch_instances([0, 1, 2], failed_instances=[0, 1, 2])
    self.expect_kill([2, 1, 0])
    self.expect_add([2, 1, 0], old_configs[0])
    self.expect_watch_instances([2, 1, 0])
    self.expect_finish()
    self.replay_mocks()

    self._updater.update()
    self.verify_mocks()

  def test_update_rollback_sorted(self):
    """Rolling back with a batch of 1 should still be correctly sorted in reverse"""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=0, max_per_shard_failures=1, batch_size=1)
    self.init_updater(update_config)

    old_configs = self.make_task_configs(5)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 5)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_kill([0])
    self.expect_add([0], new_config)
    self.expect_watch_instances([0])
    self.expect_kill([1])
    self.expect_add([1], new_config)
    self.expect_watch_instances([1])
    self.expect_kill([2])
    self.expect_add([2], new_config)
    self.expect_watch_instances([2], failed_instances=[2])
    self.expect_restart([2])
    self.expect_watch_instances([2], failed_instances=[2])
    self.expect_kill([2])
    self.expect_add([2], old_configs[0])
    self.expect_watch_instances([2])
    self.expect_kill([1])
    self.expect_add([1], old_configs[0])
    self.expect_watch_instances([1])
    self.expect_kill([0])
    self.expect_add([0], old_configs[0])
    self.expect_watch_instances([0])
    self.expect_finish()
    self.replay_mocks()

    self._updater.update()
    self.verify_mocks()

  def test_update_after_restart(self):
    """Update succeeds after failed instances are restarted."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=2, max_per_shard_failures=1)
    self.init_updater(update_config)

    old_configs = self.make_task_configs(6)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 6)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_kill([0, 1, 2])
    self.expect_add([0, 1, 2], new_config)
    self.expect_watch_instances([0, 1, 2], failed_instances=[0, 1, 2])
    self.expect_restart([0, 1, 2])
    self.expect_watch_instances([0, 1, 2])
    self.expect_kill([3, 4, 5])
    self.expect_add([3, 4, 5], new_config)
    self.expect_watch_instances([3, 4, 5])
    self.expect_finish()
    self.replay_mocks()

    self._updater.update()
    self.verify_mocks()

  def test_update_cron_job(self):
    """Updating cron job."""
    new_config = self.make_task_configs(1)[0]
    job_config = self.make_job_config(new_config, 1, cron_schedule='cron')
    self._config.job_config = job_config
    self.expect_start()
    self.expect_cron_replace(job_config)
    self.expect_finish()
    self.replay_mocks()

    self._updater.update()
    self.verify_mocks()

  def test_start_invalid_response(self):
    """The acquireLock call fails."""
    self.expect_start(response_code=ResponseCode.INVALID_REQUEST)
    self.replay_mocks()

    resp = self._updater.update()
    self.assert_response_code(ResponseCode.INVALID_REQUEST, resp)
    self.verify_mocks()

  def test_finish_invalid_response(self):
    """The releaseLock call fails."""
    new_config = self.make_task_configs(1)[0]
    job_config = self.make_job_config(new_config, 1, cron_schedule='cron')
    self._config.job_config = job_config
    self.expect_start()
    self.expect_cron_replace(job_config)
    self.expect_finish(response_code=ResponseCode.INVALID_REQUEST)
    self.replay_mocks()

    resp = self._updater.update()
    self.assert_response_code(ResponseCode.INVALID_REQUEST, resp)
    self.verify_mocks()

  def test_invalid_batch_size(self):
    """Test for out of range error for batch size."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(batch_size=0)
    with raises(Updater.InvalidConfigError):
      self.init_updater(update_config)

  def test_invalid_restart_threshold(self):
    """Test for out of range error for restart threshold."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(restart_threshold=0)
    with raises(Updater.InvalidConfigError):
      self.init_updater(update_config)

  def test_invalid_watch_secs(self):
    """Test for out of range error for watch secs."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(watch_secs=0)
    with raises(Updater.InvalidConfigError):
      self.init_updater(update_config)

  def test_update_invalid_response(self):
    """A response code other than success is returned by a scheduler RPC."""
    old_configs = self.make_task_configs(5)
    new_config = old_configs[0]
    job_config = self.make_job_config(new_config, 5)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs, response_code=ResponseCode.INVALID_REQUEST)
    self.replay_mocks()

    self._updater.update()
    self.verify_mocks()

  def test_instances_outside_range(self):
    """Provided optional instance IDs are outside of remote | local scope."""
    old_configs = self.make_task_configs(3)
    new_config = old_configs[0]
    job_config = self.make_job_config(new_config, 3)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.replay_mocks()

    self._updater.update([3, 4])
    self.verify_mocks()

  def test_update_skips_unretryable(self):
    """Update process skips instances exceeding max_per_shard_failures"""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=2, max_per_shard_failures=2)
    self.init_updater(update_config)

    old_configs = self.make_task_configs(10)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 10)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_kill([0, 1, 2])
    self.expect_add([0, 1, 2], new_config)
    self.expect_watch_instances([0, 1, 2], failed_instances=[0])
    self.expect_restart([0])
    self.expect_kill([3, 4])
    self.expect_add([3, 4], new_config)
    self.expect_watch_instances([0, 3, 4], failed_instances=[0])
    self.expect_restart([0])
    self.expect_kill([5, 6])
    self.expect_add([5, 6], new_config)
    self.expect_watch_instances([0, 5, 6], failed_instances=[0])
    self.expect_kill([7, 8, 9])
    self.expect_add([7, 8, 9], new_config)
    self.expect_watch_instances([7, 8, 9])
    self.expect_finish()
    self.replay_mocks()

    self._updater.update()
    self.verify_mocks()

