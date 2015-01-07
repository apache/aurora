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

from copy import deepcopy
from os import environ
from unittest import TestCase

from mox import MockObject, Replay, Verify
from pytest import raises

from apache.aurora.client.api.instance_watcher import InstanceWatcher
from apache.aurora.client.api.job_monitor import JobMonitor
from apache.aurora.client.api.quota_check import CapacityRequest, QuotaCheck
from apache.aurora.client.api.scheduler_mux import SchedulerMux
from apache.aurora.client.api.updater import Updater
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.common.cluster import Cluster

from ..fake_scheduler_proxy import FakeSchedulerProxy

from gen.apache.aurora.api.AuroraSchedulerManager import Client as scheduler_client
from gen.apache.aurora.api.constants import ACTIVE_STATES, THRIFT_API_VERSION
from gen.apache.aurora.api.ttypes import (
    AcquireLockResult,
    AddInstancesConfig,
    AssignedTask,
    Constraint,
    ExecutorConfig,
    Identity,
    JobConfiguration,
    JobKey,
    LimitConstraint,
    LockKey,
    LockValidation,
    Metadata,
    PopulateJobResult,
    ResourceAggregate,
    Response,
    ResponseCode,
    ResponseDetail,
    Result,
    ScheduledTask,
    ScheduleStatusResult,
    ServerInfo,
    TaskConfig,
    TaskConstraint,
    TaskQuery,
    ValueConstraint
)

# Debug output helper -> enables log.* in source.
if 'UPDATER_DEBUG' in environ:
  from twitter.common import log
  from twitter.common.log.options import LogOptions
  LogOptions.set_disk_log_level('NONE')
  LogOptions.set_stderr_log_level('DEBUG')
  log.init('test_updater')

SERVER_INFO = ServerInfo(thriftAPIVersion=THRIFT_API_VERSION)


def make_response(code, msg='test'):
  return Response(
      responseCode=code,
      serverInfo=SERVER_INFO,
      details=[ResponseDetail(message=msg)])


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

  def job_key(self):
    return AuroraJobKey(self.cluster(), self.role(), self.environment(), self.name())

  def instances(self):
    return self.job_config.instanceCount


class FakeSchedulerMux(object):
  def __init__(self):
    self._raise_error = False

  def enqueue_and_wait(self, command, data, timeout=None):
    command([data])
    if self._raise_error:
      raise SchedulerMux.Error("expected")

  def terminate(self):
    pass

  def raise_error(self):
    self._raise_error = True


class UpdaterTest(TestCase):
  UPDATE_CONFIG = {
    'batch_size': 1,
    'restart_threshold': 50,
    'watch_secs': 50,
    'max_per_shard_failures': 0,
    'max_total_failures': 0,
    'rollback_on_failure': True,
    'wait_for_batch_completion': False,
  }

  def setUp(self):
    self._role = 'mesos'
    self._name = 'jimbob'
    self._env = 'test'
    self._job_key = JobKey(name=self._name, environment=self._env, role=self._role)
    self._session_key = 'test_session'
    self._lock = 'test_lock'
    self._instance_watcher = MockObject(InstanceWatcher)
    self._job_monitor = MockObject(JobMonitor)
    self._scheduler_mux = FakeSchedulerMux()
    self._scheduler = MockObject(scheduler_client)
    self._scheduler_proxy = FakeSchedulerProxy(Cluster(name='test-cluster'),
                                               self._scheduler,
                                               self._session_key)
    self._quota_check = MockObject(QuotaCheck)
    self.init_updater(deepcopy(self.UPDATE_CONFIG))
    self._num_cpus = 1.0
    self._num_ram = 1
    self._num_disk = 1

  def replay_mocks(self):
    Replay(self._scheduler)
    Replay(self._instance_watcher)
    Replay(self._quota_check)
    Replay(self._job_monitor)

  def verify_mocks(self):
    Verify(self._scheduler)
    Verify(self._instance_watcher)
    Verify(self._quota_check)
    Verify(self._job_monitor)

  def init_updater(self, update_config):
    self._config = FakeConfig(self._role, self._name, self._env, update_config)
    self._updater = Updater(
        self._config,
        3,
        self._scheduler_proxy,
        self._instance_watcher,
        self._quota_check,
        self._job_monitor,
        self._scheduler_mux)

  def expect_terminate(self):
    self._job_monitor.terminate()
    self._instance_watcher.terminate()

  def expect_watch_instances(self, instance_ids, failed_instances=[]):
    for i in instance_ids:
      failed = [i] if i in failed_instances else []
      self._instance_watcher.watch(instance_ids).InAnyOrder().AndReturn(set(failed))

  def expect_populate(self, job_config, response_code=ResponseCode.OK):
    resp = make_response(response_code)
    config = deepcopy(job_config.taskConfig)
    result = set([config])
    resp.result = Result(populateJobResult=PopulateJobResult(
        populatedDEPRECATED=result,
        taskConfig=config))

    self._scheduler.populateJobConfig(job_config).AndReturn(resp)

  def expect_get_tasks(self, tasks, ignore_ids=None, response_code=ResponseCode.OK):
    scheduled = []
    for index, task in enumerate(tasks):
      if not ignore_ids or index not in ignore_ids:
        scheduled.append(ScheduledTask(assignedTask=AssignedTask(task=task, instanceId=index)))
    response = make_response(response_code)
    response.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=scheduled))
    query = TaskQuery(jobKeys=[self._job_key], statuses=ACTIVE_STATES)
    self._scheduler.getTasksStatus(query).AndReturn(response)

  def expect_cron_replace(self, job_config, response_code=ResponseCode.OK):
    resp = make_response(response_code)
    self._scheduler.replaceCronTemplate(job_config, self._lock, self._session_key).AndReturn(resp)

  def expect_restart(self, instance_ids, response_code=None):
    for i in instance_ids:
      response_code = ResponseCode.OK if response_code is None else response_code
      response = make_response(response_code)
      self._scheduler.restartShards(
          self._job_key,
          [i],
          self._lock,
          self._session_key).AndReturn(response)

  def expect_kill(self,
      instance_ids,
      response_code=ResponseCode.OK,
      monitor_result=True,
      skip_monitor=False):
    for i in instance_ids:
      query = TaskQuery(jobKeys=[self._job_key],
                        statuses=ACTIVE_STATES,
                        instanceIds=frozenset([int(i)]))
      self._scheduler.killTasks(
          query,
          self._lock,
          self._session_key).InAnyOrder().AndReturn(make_response(response_code))

    self.expect_job_monitor(response_code, instance_ids, monitor_result, skip_monitor)

  def expect_job_monitor(self, response_code, instance_ids, monitor_result=True, skip=False):
    if skip or response_code != ResponseCode.OK:
      return

    self._job_monitor.wait_until(
        JobMonitor.terminal,
        instance_ids,
        with_timeout=True).InAnyOrder().AndReturn(monitor_result)

  def expect_add(self, instance_ids, task_config, response_code=ResponseCode.OK):
    for i in instance_ids:
      add_config = AddInstancesConfig(
          key=self._job_key,
          taskConfig=task_config,
          instanceIds=frozenset([int(i)]))
      self._scheduler.addInstances(
          add_config,
          self._lock,
          self._session_key).InAnyOrder().AndReturn(make_response(response_code))

  def expect_update_instances(self, instance_ids, task_config):
    for i in instance_ids:
      self.expect_kill([i])
      self.expect_add([i], task_config)
      self.expect_watch_instances([i])

  def expect_add_instances(self, instance_ids, task_config):
    for i in instance_ids:
      self.expect_add([i], task_config)
      self.expect_watch_instances([i])

  def expect_kill_instances(self, instance_ids):
    for i in instance_ids:
      self.expect_kill([i])

  def expect_start(self, response_code=ResponseCode.OK):
    response = make_response(response_code)
    response.result = Result(acquireLockResult=AcquireLockResult(lock=self._lock))
    self._scheduler.acquireLock(LockKey(job=self._job_key), self._session_key).AndReturn(response)

  def expect_finish(self, response_code=ResponseCode.OK):
    self._scheduler.releaseLock(
        self._lock,
        LockValidation.CHECKED,
        self._session_key).AndReturn(make_response(response_code))

  def expect_quota_check(self,
      num_released,
      num_acquired,
      response_code=ResponseCode.OK,
      prod=True):
    released = CapacityRequest(ResourceAggregate(
        numCpus=num_released * self._num_cpus,
        ramMb=num_released * self._num_ram,
        diskMb=num_released * self._num_disk))
    acquired = CapacityRequest(ResourceAggregate(
      numCpus=num_acquired * self._num_cpus,
      ramMb=num_acquired * self._num_ram,
      diskMb=num_acquired * self._num_disk))

    self._quota_check.validate_quota_from_requested(
        self._job_key, prod, released, acquired).AndReturn(make_response(response_code))

  def make_task_configs(self, count=1, prod=True):
    return [TaskConfig(
        owner=Identity(role=self._job_key.role),
        environment=self._job_key.environment,
        jobName=self._job_key.name,
        numCpus=self._num_cpus,
        ramMb=self._num_ram,
        diskMb=self._num_disk,
        priority=0,
        maxTaskFailures=1,
        production=prod,
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
        taskConfig=deepcopy(task_config),
        instanceCount=instance_count
    )

  def update_and_expect_ok(self, instances=None):
    self.update_and_expect_response(ResponseCode.OK, instances)

  def update_and_expect_response(self, expected_code, instances=None, message=None):
    resp = self._updater.update(instances)
    assert expected_code == resp.responseCode, (
      'Expected response:%s Actual response:%s' % (expected_code, resp.responseCode))

    if message:
      assert len(resp.details) == 1, (
        'Unexpected error count:%s' % len(resp.details))

      assert message in resp.details[0].message, (
        "Expected %s message not found in: %s" % (message, resp.details[0].message))

  def test_grow(self):
    """Adds instances to the existing job."""
    old_configs = self.make_task_configs(3)
    new_config = old_configs[0]
    job_config = self.make_job_config(new_config, 7)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_quota_check(0, 4)
    self.expect_add_instances([3, 4, 5, 6], new_config)
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_ok()
    self.verify_mocks()

  def test_grow_fails_quota_check(self):
    """Adds instances to the existing job fails due to not enough quota."""
    old_configs = self.make_task_configs(3)
    new_config = old_configs[0]
    job_config = self.make_job_config(new_config, 7)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_quota_check(0, 4, response_code=ResponseCode.INVALID_REQUEST)
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_response(expected_code=ResponseCode.ERROR)
    self.verify_mocks()

  def test_non_to_prod_fails_quota_check(self):
    """Update with shrinking with non->prod transition fails quota check."""
    old_configs = self.make_task_configs(4, prod=False)
    new_config = deepcopy(old_configs[0])
    new_config.production = True
    job_config = self.make_job_config(new_config, 2)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_quota_check(0, 2, response_code=ResponseCode.INVALID_REQUEST)
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_response(expected_code=ResponseCode.ERROR)
    self.verify_mocks()

  def test_prod_to_non_always_passes_quota_check(self):
    """Update with growth with prod->non transition always passes."""
    old_configs = self.make_task_configs(1, prod=True)
    new_config = deepcopy(old_configs[0])
    new_config.production = False
    job_config = self.make_job_config(new_config, 3)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_quota_check(1, 0, prod=False)
    self.expect_kill([0])
    self.expect_add_instances([0, 1, 2], new_config)
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_ok()
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
    self.expect_quota_check(7, 0)
    self.expect_kill_instances([3, 4, 5, 6, 7, 8, 9])
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_ok()
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
    self.expect_quota_check(3, 7)
    self.expect_update_instances([0, 1, 2], new_config)
    self.expect_add_instances([3, 4, 5, 6], new_config)
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_ok()
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
    self.expect_quota_check(10, 1)
    self.expect_update_instances([0], new_config)
    self.expect_kill_instances([1, 2, 3, 4, 5, 6, 7, 8, 9])
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_ok()
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
    self.expect_quota_check(5, 5)
    self.expect_update_instances([0, 1, 2, 3, 4], new_config)
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_ok()
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
    self.expect_quota_check(0, 2)
    self.expect_add_instances([3, 4], new_config)
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_ok(instances=[3, 4])
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
    self.expect_quota_check(6, 0)
    self.expect_kill_instances([4, 5, 6, 7, 8, 9])
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_ok(instances=[4, 5, 6, 7, 8, 9])
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
    self.expect_quota_check(3, 3)
    self.expect_update_instances([2, 3, 4], new_config)
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_ok(instances=[2, 3, 4])
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
    self.expect_quota_check(0, 2)
    self.expect_add_instances([2, 3], new_config)
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_ok([2, 3])
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
    self.expect_quota_check(0, 0)
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_ok()
    self.verify_mocks()

  def test_update_rollback(self):
    """Update process failures exceed total allowable count and update is rolled back."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_per_shard_failures=1)
    self.init_updater(update_config)

    old_configs = self.make_task_configs(10)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 10)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_quota_check(10, 10)
    self.expect_update_instances([0, 1], new_config)
    self.expect_kill([2])
    self.expect_add([2], new_config)
    self.expect_watch_instances([2], failed_instances=[2])
    self.expect_restart([2])
    self.expect_watch_instances([2], failed_instances=[2])
    self.expect_update_instances([2, 1, 0], old_configs[0])
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_response(ResponseCode.ERROR)
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
    self.expect_quota_check(6, 6)
    self.expect_update_instances([0, 1], new_config)
    self.expect_kill([2])
    self.expect_add([2], new_config)
    self.expect_watch_instances([2], failed_instances=[2])
    self.expect_restart([2])
    self.expect_watch_instances([2])
    self.expect_update_instances([3, 4, 5], new_config)
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_ok()
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

    self.update_and_expect_ok()
    self.verify_mocks()

  def test_start_invalid_response(self):
    """The acquireLock call fails."""
    self.expect_start(response_code=ResponseCode.INVALID_REQUEST)
    self.replay_mocks()

    self.update_and_expect_response(ResponseCode.INVALID_REQUEST)
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

    self.update_and_expect_response(ResponseCode.INVALID_REQUEST)
    self.verify_mocks()

  def test_invalid_batch_size(self):
    """Test for out of range error for batch size."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(batch_size=0)
    with raises(Updater.Error):
      self.init_updater(update_config)

  def test_invalid_restart_threshold(self):
    """Test for out of range error for restart threshold."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(restart_threshold=0)
    with raises(Updater.Error):
      self.init_updater(update_config)

  def test_invalid_watch_secs(self):
    """Test for out of range error for watch secs."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(watch_secs=0)
    with raises(Updater.Error):
      self.init_updater(update_config)

  def test_update_invalid_response(self):
    """A response code other than success is returned by a scheduler RPC."""
    old_configs = self.make_task_configs(5)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 5)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_quota_check(5, 5)
    self._scheduler_mux.raise_error()
    self.expect_kill([0], skip_monitor=True)
    self.expect_terminate()
    self.replay_mocks()

    self.update_and_expect_response(ResponseCode.ERROR)
    self.verify_mocks()

  def test_update_kill_timeout(self):
    """Test job monitor timeout while waiting for tasks killed."""
    old_configs = self.make_task_configs(5)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 5)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_quota_check(5, 5)
    self.expect_kill([0], monitor_result=False)
    self.expect_terminate()
    self.replay_mocks()

    self.update_and_expect_response(ResponseCode.ERROR)
    self.verify_mocks()

  def test_failed_update_populates_error_details(self):
    """Test failed update populates Response.details."""
    old_configs = self.make_task_configs(5)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 5)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_quota_check(5, 5)
    self.expect_kill([0], monitor_result=False)
    self.expect_terminate()
    self.replay_mocks()

    self.update_and_expect_response(ResponseCode.ERROR, message="Aborting update without rollback")
    self.verify_mocks()

  def test_job_does_not_exist(self):
    """Unable to update a job that does not exist."""
    old_configs = self.make_task_configs(5)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 5)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs, response_code=ResponseCode.INVALID_REQUEST)
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_response(ResponseCode.ERROR)
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
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_response(ResponseCode.ERROR, instances=[3, 4])
    self.verify_mocks()

  def test_update_skips_unretryable(self):
    """Update process skips instances exceeding max_per_shard_failures"""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=1, max_per_shard_failures=2)
    self.init_updater(update_config)

    old_configs = self.make_task_configs(10)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 10)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_quota_check(10, 10)
    self.expect_update_instances([0, 1], new_config)
    self.expect_kill([2])
    self.expect_add([2], new_config)
    self.expect_watch_instances([2], failed_instances=[2])
    self.expect_restart([2])
    self.expect_watch_instances([2], failed_instances=[2])
    self.expect_restart([2])
    self.expect_watch_instances([2], failed_instances=[2])
    self.expect_update_instances([3, 4, 5, 6, 7, 8, 9], new_config)
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_ok()
    self.verify_mocks()

  def test_diff_unordered_configs(self):
    """Diff between two config objects with different repr but identical content works ok."""
    from_config = self.make_task_configs()[0]
    from_config.constraints = set([
        Constraint(name='value', constraint=ValueConstraint(values=set(['1', '2']))),
        Constraint(name='limit', constraint=TaskConstraint(limit=LimitConstraint(limit=int(10))))])
    from_config.taskLinks = {'task1': 'link1', 'task2': 'link2'}
    from_config.metadata = set([
      Metadata(key='k2', value='v2'),
      Metadata(key='k1', value='v1')])
    from_config.executorConfig = ExecutorConfig(name='test', data='test data')
    from_config.requestedPorts = set(['3424', '142', '45235'])

    # Deepcopy() almost guarantees from_config != to_config due to a different sequence of
    # dict insertions. That in turn generates unequal json objects. The ideal here would be to
    # assert to_config != from_config but that would produce a flaky test as I have observed
    # the opposite on rare occasions as the ordering is not stable between test runs.
    to_config = deepcopy(from_config)

    diff_result = self._updater._diff_configs(from_config, to_config)
    assert diff_result == "", (
      'diff result must be empty but was: %s' % diff_result)

  def test_update_no_rollback(self):
    """Update process failures exceed total allowable count and update is not rolled back."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=1, max_per_shard_failures=1, rollback_on_failure=False)
    self.init_updater(update_config)

    old_configs = self.make_task_configs(10)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 10)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_quota_check(10, 10)
    self.expect_kill([0])
    self.expect_add([0], new_config)
    self.expect_watch_instances([0], failed_instances=[0])
    self.expect_restart([0])
    self.expect_watch_instances([0], failed_instances=[0])
    self.expect_kill([1])
    self.expect_add([1], new_config)
    self.expect_watch_instances([1], failed_instances=[1])
    self.expect_restart([1])
    self.expect_watch_instances([1], failed_instances=[1])
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_response(ResponseCode.ERROR)
    self.verify_mocks()

  def test_update_instances_wait_for_batch_completion_filled_batch(self):
    """Update existing instances with wait_for_batch_completion flag set."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(wait_for_batch_completion=True, batch_size=2)
    self.init_updater(update_config)

    old_configs = self.make_task_configs(6)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 6)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_quota_check(6, 6)
    self.expect_update_instances([0, 1, 2, 3, 4, 5], new_config)
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_ok()
    self.verify_mocks()

  def test_update_instances_wait_for_batch_completion_partially_filled_batch(self):
    """Update existing instances with wait_for_batch_completion flag set."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(wait_for_batch_completion=True, batch_size=3)
    self.init_updater(update_config)

    old_configs = self.make_task_configs(5)
    new_config = deepcopy(old_configs[0])
    new_config.priority = 5
    job_config = self.make_job_config(new_config, 5)
    self._config.job_config = job_config
    self.expect_start()
    self.expect_get_tasks(old_configs)
    self.expect_populate(job_config)
    self.expect_quota_check(5, 5)
    self.expect_update_instances([0, 1, 2, 3, 4], new_config)
    self.expect_finish()
    self.replay_mocks()

    self.update_and_expect_ok()
    self.verify_mocks()
