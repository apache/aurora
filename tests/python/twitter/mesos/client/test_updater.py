from collections import deque
import copy
from math import ceil
import unittest

from twitter.mesos.client.scheduler_client import SchedulerProxy
from twitter.mesos.client.updater import Updater

from gen.twitter.mesos.constants import DEFAULT_ENVIRONMENT
from gen.twitter.mesos.ttypes import *
from gen.twitter.mesos.MesosSchedulerManager import Client as scheduler_client

import mox
import pytest


RUNNING    = ScheduleStatus.RUNNING
PENDING    = ScheduleStatus.PENDING
ADDED      = ShardUpdateResult.ADDED
RESTARTING = ShardUpdateResult.RESTARTING
UNCHANGED  = ShardUpdateResult.UNCHANGED


def find_expected_status_calls(watch_secs, sleep_secs):
  return ceil(watch_secs / sleep_secs)


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


class FakeConfig(object):
  def __init__(self, role, name, update_config):
    self._role = role
    self._name = name
    self._update_config = update_config

  def role(self):
    return self._role

  def name(self):
    return self._name

  def update_config(self):
    class Anon(object):
      def get(_):
        return self._update_config
    return Anon()


class FakeSchedulerProxy(SchedulerProxy):
  def __init__(self, cluster, scheduler, session_key):
    self._cluster = cluster
    self._scheduler = scheduler
    self._session_key = session_key

  def client(self):
    return self._scheduler

  def session_key(self):
    return self._session_key

class UpdaterTest(unittest.TestCase):
  UPDATE_CONFIG = {
    'batch_size':             3,
    'restart_threshold':     50,
    'watch_secs':            50,
    'max_per_shard_failures': 0,
    'max_total_failures':     0,
  }
  EXPECTED_GET_STATUS_CALLS = find_expected_status_calls(UPDATE_CONFIG['watch_secs'], 3.0) + 1

  def setUp(self):
    self._role = 'mesos'
    self._name = 'jimbob'
    self._job_key = JobKey(name=self._name, environment=DEFAULT_ENVIRONMENT, role=self._role)
    self._session_key = 'test_session'
    self._update_token = 'test_update'
    self._clock = Clock()
    self._scheduler = mox.MockObject(scheduler_client)
    self._scheduler_proxy = FakeSchedulerProxy('test-cluster', self._scheduler, self._session_key)
    self._initial_shards = range(10)
    self.init_updater(copy.deepcopy(self.UPDATE_CONFIG))

  def init_updater(self, update_config):
    config = FakeConfig(self._role, self._name, update_config)
    self._updater = Updater(config, self._scheduler_proxy, self._clock)
    self._updater._update_token = self._update_token

  def get_tasks_status_query(self, shard_ids):
    query = TaskQuery()
    query.owner = Identity(role=self._role)
    query.jobName = self._name
    query.statuses = set([ScheduleStatus.RUNNING])
    query.shardIds = shard_ids
    return query

  def expect_get_statuses(self, statuses, num_calls=EXPECTED_GET_STATUS_CALLS):
    response = ScheduleStatusResponse(responseCode=ResponseCode.OK, message='test', tasks=[])
    for shard in statuses:
      response.tasks += [ScheduledTask(assignedTask=AssignedTask(
          task=TwitterTaskInfo(shardId=shard)), status=statuses[shard])]
    query = self.get_tasks_status_query(set(statuses))
    for x in range(int(num_calls)):
      self._scheduler.getTasksStatus(query).AndReturn(response)

  def expect_update(self, shard_ids, shard_results=None):
    response = UpdateShardsResponse(responseCode=UpdateResponseCode.OK, message='test')
    response.shards = shard_results
    self._scheduler.updateShards(
        self._role, self._name, self._job_key, shard_ids,
        self._update_token, self._session_key).AndReturn(response)

  def expect_restart(self, shard_ids):
    response = RestartShardsResponse(responseCode=ResponseCode.OK, message='test')
    self._scheduler.restartShards(
        self._role, self._name, self._job_key, shard_ids, self._session_key).AndReturn(response)

  def expect_rollback(self, shard_ids, shard_results=None):
    response = RollbackShardsResponse(responseCode=UpdateResponseCode.OK, message='test')
    response.shards = shard_results
    self._scheduler.rollbackShards(
        self._role, self._name, self._job_key, shard_ids,
        self._update_token, self._session_key).AndReturn(response)

  def assert_update_result(self, expected_failed_shards, update_shards=None):
    update_shards = update_shards or self._initial_shards
    shards_returned = self._updater.update(update_shards)
    assert expected_failed_shards == shards_returned, (
        'Expected shards (%s) : Returned shards (%s)' % (expected_failed_shards, shards_returned))

  def test_case_pass(self):
    """All tasks complete and update succeeds"""
    self.expect_update([0, 1, 2])
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING})
    self.expect_update([3, 4, 5])
    self.expect_get_statuses({3: RUNNING, 4: RUNNING, 5: RUNNING})
    self.expect_update([6, 7, 8])
    self.expect_get_statuses({6: RUNNING, 7: RUNNING, 8: RUNNING})
    self.expect_update([9])
    self.expect_get_statuses({9: RUNNING})
    mox.Replay(self._scheduler)
    self.assert_update_result([])
    mox.Verify(self._scheduler)

  def test_noop_update(self):
    """No config changes made in any tasks."""
    def expect_noop_restart(shards):
      self.expect_update(shards, dict([(s, UNCHANGED) for s in shards]))
    expect_noop_restart([0, 1, 2])
    expect_noop_restart([3, 4, 5])
    expect_noop_restart([6, 7, 8])
    expect_noop_restart([9])
    mox.Replay(self._scheduler)
    self.assert_update_result([])
    mox.Replay(self._scheduler)

  def test_mixed_update(self):
    """Update that adds tasks and modifies some."""
    self.expect_update([0, 1, 2], {0: RESTARTING, 1: UNCHANGED, 2: RESTARTING})
    self.expect_get_statuses({0: RUNNING, 2: RUNNING})
    self.expect_update([3, 4, 5], {3: UNCHANGED, 4: RESTARTING, 5: RESTARTING})
    self.expect_get_statuses({4: RUNNING, 5: RUNNING})
    self.expect_update([6, 7, 8], {3: UNCHANGED, 4: UNCHANGED, 5: UNCHANGED})
    self.expect_update([9, 10, 11], {9: UNCHANGED, 10: ADDED, 11: ADDED})
    self.expect_get_statuses({10: RUNNING, 11: RUNNING})
    mox.Replay(self._scheduler)
    self.assert_update_result([], range(12))
    mox.Verify(self._scheduler)

  def test_tasks_stuck_in_starting(self):
    """Tasks 1, 2, 3 fail to move into RUNNING when restarted - Complete rollback performed."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=2, max_per_shard_failures=1)
    self.init_updater(update_config)
    self.expect_update([0, 1, 2])
    self.expect_get_statuses({0: PENDING, 1: PENDING, 2: PENDING})
    self.expect_restart([0, 1, 2])
    self.expect_get_statuses({0: PENDING, 1: PENDING, 2: PENDING})
    self.expect_rollback([0, 1, 2])
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING})
    mox.Replay(self._scheduler)
    self.assert_update_result([0, 1, 2])
    mox.Verify(self._scheduler)

  def test_all_failed_shards(self):
    """Tests if appropriate calls are made if all shards in an update fail."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=2, max_per_shard_failures=1)
    self.init_updater(update_config)
    self.expect_update([0, 1, 2])
    self.expect_get_statuses({0: PENDING, 1: PENDING, 2: PENDING})
    self.expect_restart([0, 1, 2])
    self.expect_get_statuses({0: PENDING, 1: PENDING, 2: PENDING})
    self.expect_rollback([0, 1, 2])
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING})
    mox.Replay(self._scheduler)
    self.assert_update_result([0, 1, 2], range(3))
    mox.Verify(self._scheduler)

  def test_successful_shard_restart(self):
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=2, max_per_shard_failures=1)
    self.init_updater(update_config)
    self.expect_update([0])
    self.expect_get_statuses({0: PENDING})
    self.expect_restart([0])
    self.expect_get_statuses({0: RUNNING})
    mox.Replay(self._scheduler)
    self.assert_update_result([], range(1))
    mox.Verify(self._scheduler)

  def test_single_failed_shard(self):
    """All tasks fail to move into running state when re-started - Complete rollback performed."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=0, max_per_shard_failures=2)
    self.init_updater(update_config)
    self.expect_update([0, 1, 2])
    self.expect_get_statuses({0:PENDING, 1: RUNNING, 2: RUNNING})
    self.expect_restart([0])
    self.expect_update([3, 4])
    self.expect_get_statuses({0:PENDING, 3: RUNNING, 4: RUNNING})
    self.expect_restart([0])
    self.expect_update([5, 6])
    self.expect_get_statuses({0:PENDING, 5: RUNNING, 6: RUNNING})
    self.expect_rollback([0, 1, 2])
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING})
    self.expect_rollback([3, 4, 5])
    self.expect_get_statuses({3: RUNNING, 4: RUNNING, 5: RUNNING})
    self.expect_rollback([6])
    self.expect_get_statuses({6: RUNNING})
    mox.Replay(self._scheduler)
    self.assert_update_result([0])
    mox.Verify(self._scheduler)

  def test_shard_state_transition(self):
    """All tasks move into running state at the end of restart threshold."""
    self.expect_update([0, 1, 2])
    self.expect_get_statuses(
        {0: PENDING, 1: PENDING, 2: PENDING}, num_calls=(self.EXPECTED_GET_STATUS_CALLS - 1))
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING})
    self.expect_update([3, 4, 5])
    self.expect_get_statuses(
        {3: PENDING, 4: PENDING, 5: PENDING}, self.EXPECTED_GET_STATUS_CALLS - 1)
    self.expect_get_statuses({3: RUNNING, 4: RUNNING, 5: RUNNING})
    self.expect_update([6, 7, 8])
    self.expect_get_statuses(
        {6: PENDING, 7: PENDING, 8: PENDING}, self.EXPECTED_GET_STATUS_CALLS - 1)
    self.expect_get_statuses({6: RUNNING, 7: RUNNING, 8: RUNNING})
    self.expect_update([9])
    self.expect_get_statuses({9: PENDING}, self.EXPECTED_GET_STATUS_CALLS - 1)
    self.expect_get_statuses({9: RUNNING})
    mox.Replay(self._scheduler)
    self.assert_update_result([])
    mox.Verify(self._scheduler)

  def test_case_unknown_state(self):
    """All tasks move into an unexpected state - Complete rollback performed."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=2, max_per_shard_failures=1)
    self.init_updater(update_config)
    self.expect_update([0, 1, 2])
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING}, num_calls=1)
    self.expect_get_statuses({0: PENDING, 1: PENDING, 2: PENDING}, num_calls=1)
    self.expect_restart([0, 1, 2])
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING}, num_calls=1)
    self.expect_get_statuses({0: PENDING, 1: PENDING, 2: PENDING}, num_calls=1)
    self.expect_rollback([0, 1, 2])
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING})
    mox.Replay(self._scheduler)
    self.assert_update_result([0, 1, 2])
    mox.Verify(self._scheduler)

  def test_invalid_batch_size(self):
    """Test for out of range error for batch size"""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(batch_size=0)
    with pytest.raises(Updater.InvalidConfigError):
      self.init_updater(update_config)

  def test_invalid_restart_threshold(self):
    """Test for out of range error for restart threshold"""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(restart_threshold=0)
    with pytest.raises(Updater.InvalidConfigError):
      self.init_updater(update_config)

  def test_invalid_watch_secs(self):
    """Test for out of range error for watch secs"""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(watch_secs=0)
    with pytest.raises(Updater.InvalidConfigError):
      self.init_updater(update_config)
