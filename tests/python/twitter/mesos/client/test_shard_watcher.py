from collections import deque, defaultdict
import copy
from math import ceil
import unittest

from twitter.mesos.client.scheduler_client import SchedulerProxy
from twitter.mesos.client.shard_watcher import ShardWatcher
from twitter.mesos.common.http_signaler import HttpSignaler as HealthChecker

from gen.twitter.mesos.constants import DEFAULT_ENVIRONMENT
from gen.twitter.mesos.ttypes import *
from gen.twitter.mesos.MesosSchedulerManager import Client as scheduler_client

import mox
import pytest

RUNNING    = ScheduleStatus.RUNNING
FAILED     = ScheduleStatus.FAILED
PENDING    = ScheduleStatus.PENDING


def find_expected_status_calls(watch_secs, sleep_secs):
  return ceil(watch_secs / sleep_secs) + 1


class Clock(object):
  """Simulates time for test cases."""
  def __init__(self):
    """Initialize the current time to 0.0"""
    self._current_time = 0.0

  def time(self):
    """Returns the current time."""
    return self._current_time

  def sleep(self, secs):
    """Simulate sleep by adding the required sleep time in secs."""
    self._current_time += secs


class FakeSchedulerProxy(SchedulerProxy):
  def __init__(self, cluster, scheduler, session_key):
    self._cluster = cluster
    self._scheduler = scheduler
    self._session_key = session_key

  def client(self):
    return self._scheduler

  def session_key(self):
    return self._session_key


class HealthCheckHandler(object):
  """Performs the functions of a health checker for testing"""
  ENABLED = False
  HEALTH_CHECKERS = defaultdict(lambda: mox.MockObject(HealthChecker))

  def __init__(self, enabled):
    HealthCheckHandler.ENABLED = enabled

  @classmethod
  def handle_health_checker_creation(cls, host, port):
    # ShardId is used as the host for testing
    if cls.ENABLED:
      return cls.HEALTH_CHECKERS[host]

  def expect_health_check(self, shard, status):
    health_checker = self.HEALTH_CHECKERS[str(shard)]
    health_checker.health().AndReturn((status, ''))

  def replay(self):
    for health_checker in self.HEALTH_CHECKERS.values():
      mox.Replay(health_checker)

  def verify(self):
    for health_checker in self.HEALTH_CHECKERS.values():
      mox.Verify(health_checker)
    self.HEALTH_CHECKERS.clear()


class TestShardWatcher(ShardWatcher):
  # Avoid proxy for unit tests.
  def maybe_setup_proxy(self, cluster):
    pass

  def create_health_checker(self, host, port):
    return HealthCheckHandler.handle_health_checker_creation(host, port)


class ShardWatcherTest(unittest.TestCase):
  RESTART_THRESHOLD = HEALTHY_THRESHOLD = WATCH_SECS = 50
  EXPECTED_CYCLES = find_expected_status_calls(WATCH_SECS, 3.0)

  def setUp(self):
    self._role = 'mesos'
    self._name = 'jimbob'
    self._job = JobKey(name=self._name, environment=DEFAULT_ENVIRONMENT, role=self._role)
    self._session_key = 'test_session'
    self._clock = Clock()
    self._scheduler = mox.MockObject(scheduler_client)
    self._scheduler_proxy = FakeSchedulerProxy('cluster', self._scheduler, self._session_key)
    self.init_shard_watcher()

  def init_shard_watcher(self, health_check=False):
    self._watcher = TestShardWatcher(self._scheduler, self._job, 'cluster', self.RESTART_THRESHOLD,
        self.HEALTHY_THRESHOLD, self.WATCH_SECS, health_check, self._clock)
    self.init_health_check_handler(health_check)

  def init_health_check_handler(self, health_check):
    self._health_check_handler = HealthCheckHandler(health_check)

  def get_tasks_status_query(self, shard_ids):
    query = TaskQuery()
    query.owner = Identity(role=self._role)
    query.jobName = self._name
    query.statuses = set([ScheduleStatus.RUNNING])
    query.shardIds = shard_ids
    return query

  def expect_get_statuses(self, statuses, num_calls=EXPECTED_CYCLES, missing_shards=[]):
    response = ScheduleStatusResponse(responseCode=ResponseCode.OK, message='test', tasks=[])
    for shard in statuses:
      task = TwitterTaskInfo(shardId = shard)
      ports = {}
      if self._health_check_handler.ENABLED:
        ports['health'] = 33333
      # Using shardId as the slaveHost for testing.
      assigned_task = AssignedTask(task = task, slaveHost = str(shard), assignedPorts = ports)
      response.tasks += [ScheduledTask(assignedTask = assigned_task, status = statuses[shard])]
    query = self.get_tasks_status_query(set(statuses).union(missing_shards))
    for x in range(int(num_calls)):
      self._scheduler.getTasksStatus(query).AndReturn(response)

  def expect_health_check(self, shard, status, num_calls=EXPECTED_CYCLES):
    for x in range(int(num_calls)):
      self._health_check_handler.expect_health_check(shard, status)

  def assert_watch_result(self, expected_failed_shards, shards_to_watch=range(3)):
    shards_returned = self._watcher.watch(shards_to_watch)
    assert expected_failed_shards == shards_returned, (
        'Expected shards (%s) : Returned shards (%s)' % (expected_failed_shards, shards_returned))

  def replay_mocks(self):
    mox.Replay(self._scheduler)
    self._health_check_handler.replay()

  def verify_mocks(self):
    mox.Verify(self._scheduler)
    self._health_check_handler.verify()

  def test_successful_watch(self):
    """Shards move into RUNNING immediately."""
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING})
    self.replay_mocks()
    self.assert_watch_result([])
    mox.Verify(self._scheduler)

  def test_restart_threshold_failure(self):
    """Shards fail to move to RUNNING before restart threshold."""
    self.expect_get_statuses({0: PENDING, 1: PENDING, 2: PENDING})
    self.replay_mocks()
    self.assert_watch_result([0, 1, 2])
    self.verify_mocks()

  def test_watch_period_failure(self):
    """Shards fail to remain RUNNING before watch seconds expires."""
    self.expect_get_statuses(
        {0: RUNNING, 1: RUNNING, 2: RUNNING}, num_calls=self.EXPECTED_CYCLES - 1)
    self.expect_get_statuses({0: FAILED, 1: FAILED, 2: FAILED}, num_calls=1)
    self.replay_mocks()
    self.assert_watch_result([0, 1, 2])
    self.verify_mocks()

  def test_extended_time_to_run(self):
    """Shards transition into RUNNING just before the restart threshold"""
    self.expect_get_statuses(
        {0: PENDING, 1: PENDING, 2: PENDING}, num_calls=self.EXPECTED_CYCLES - 1)
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING})
    self.replay_mocks()
    self.assert_watch_result([])
    self.verify_mocks()

  def test_missing_shards(self):
    """A shard goes missing after transitioning into RUNNING"""
    self.expect_get_statuses(
        {0: RUNNING, 1: RUNNING, 2: RUNNING}, num_calls=self.EXPECTED_CYCLES - 1)
    self.expect_get_statuses({1:RUNNING, 2:RUNNING}, num_calls=1, missing_shards=[0])
    self.replay_mocks()
    self.assert_watch_result([0])
    self.verify_mocks()

  def test_single_failed_shard(self):
    """A shard goes fails after transitioning into RUNNING"""
    self.expect_get_statuses(
        {0: RUNNING, 1: RUNNING, 2: RUNNING}, num_calls=self.EXPECTED_CYCLES - 1)
    self.expect_get_statuses({0:FAILED, 1:RUNNING, 2:RUNNING}, num_calls=1)
    self.replay_mocks()
    self.assert_watch_result([0])
    self.verify_mocks()

  def test_clean_health_checks(self):
    """All shards return ok to health checks immediately."""
    self.init_shard_watcher(health_check=True)
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2:RUNNING})
    self.expect_health_check(0, True)
    self.expect_health_check(1, True)
    self.expect_health_check(2, True)
    self.replay_mocks()
    self.assert_watch_result([])
    self.verify_mocks()

  def test_warmup_period_for_health(self):
    """Shard returns ok to health check after an initial delay"""
    self.init_shard_watcher(health_check=True)
    FAILED_HEALTH_CHECKS = 6
    self.expect_get_statuses(
        {0: RUNNING}, num_calls=self.EXPECTED_CYCLES + FAILED_HEALTH_CHECKS)
    self.expect_health_check(0, False, num_calls=FAILED_HEALTH_CHECKS)
    self.expect_health_check(0, True)
    self.replay_mocks()
    self.assert_watch_result([], shards_to_watch=range(1))
    self.verify_mocks()

  def test_failed_health_check(self):
    """Shard fails to return ok to health check within the healthy threshold"""
    self.init_shard_watcher(health_check=True)
    self.expect_get_statuses({0: RUNNING})
    self.expect_health_check(0, False)
    self.replay_mocks()
    self.assert_watch_result([0], shards_to_watch=range(1))
    self.verify_mocks()

  def test_delayed_task_failure(self):
    """Shard fails to return ok during the watch period"""
    self.init_shard_watcher(health_check=True)
    self.expect_get_statuses({0: RUNNING}, num_calls=self.EXPECTED_CYCLES - 1)
    self.expect_health_check(0, True, num_calls=self.EXPECTED_CYCLES - 1)
    self.expect_get_statuses({0: FAILED}, num_calls=1)
    self.replay_mocks()
    self.assert_watch_result([0], shards_to_watch=range(1))
    self.verify_mocks()
