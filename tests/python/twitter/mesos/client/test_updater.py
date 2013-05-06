import copy
import unittest

from twitter.mesos.client.scheduler_client import SchedulerProxy
from twitter.mesos.client.updater import Updater
from twitter.mesos.client.shard_watcher import ShardWatcher

from gen.twitter.mesos.ttypes import *
from gen.twitter.mesos.MesosSchedulerManager import Client as scheduler_client

import mox
import pytest

ADDED      = ShardUpdateResult.ADDED
RESTARTING = ShardUpdateResult.RESTARTING
UNCHANGED  = ShardUpdateResult.UNCHANGED


class FakeConfig(object):
  def __init__(self, role, name, env, update_config):
    self._role = role
    self._env = env
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

  def has_health_port(self):
    return False

  def cluster(self):
    return 'test'

  def environment(self):
    return self._env


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

  def setUp(self):
    self._role = 'mesos'
    self._name = 'jimbob'
    self._env = 'test'
    self._job_key = JobKey(name=self._name, environment=self._env, role=self._role)
    self._session_key = 'test_session'
    self._update_token = 'test_update'
    self._shard_watcher = mox.MockObject(ShardWatcher)
    self._scheduler = mox.MockObject(scheduler_client)
    self._scheduler_proxy = FakeSchedulerProxy('test-cluster', self._scheduler, self._session_key)
    self._initial_shards = range(10)
    self.init_updater(copy.deepcopy(self.UPDATE_CONFIG))

  def init_updater(self, update_config):
    config = FakeConfig(self._role, self._name, self._env, update_config)
    self._updater = Updater(config, self._scheduler_proxy)
    self._updater._update_token = self._update_token

  def expect_update(self, shard_ids, shard_results=None):
    response = UpdateShardsResponse(responseCode=UpdateResponseCode.OK, message='test')
    response.shards = shard_results
    # TODO(ksweeney): Remove Nones after JobKey migration is complete
    self._scheduler.updateShards(
        None, None, self._job_key, shard_ids,
        self._update_token, self._session_key).AndReturn(response)

  def expect_restart(self, shard_ids):
    response = RestartShardsResponse(responseCode=ResponseCode.OK, message='test')
    # TODO(ksweeney): Remove Nones after JobKey migration is complete
    self._scheduler.restartShards(
        None, None, self._job_key, shard_ids, self._session_key).AndReturn(response)

  def expect_rollback(self, shard_ids, shard_results=None):
    response = RollbackShardsResponse(responseCode=UpdateResponseCode.OK, message='test')
    response.shards = shard_results
    # TODO(ksweeney): Remove Nones after JobKey migration is complete
    self._scheduler.rollbackShards(
        None, None, self._job_key, shard_ids,
        self._update_token, self._session_key).AndReturn(response)

  def expect_watch_shards(self, shard_ids, failed_shards=[]):
    self._shard_watcher.watch(shard_ids).AndReturn(set(failed_shards))

  def assert_update_result(self, expected_failed_shards, update_shards=None):
    update_shards = update_shards or self._initial_shards
    health_check_interval_seconds = 3
    shards_returned = self._updater.update(
        update_shards, health_check_interval_seconds, self._shard_watcher)
    assert set(expected_failed_shards) == shards_returned, (
        'Expected shards (%s) : Returned shards (%s)' % (expected_failed_shards, shards_returned))

  def replay_mocks(self):
    mox.Replay(self._scheduler)
    mox.Replay(self._shard_watcher)

  def verify_mocks(self):
    mox.Verify(self._scheduler)
    mox.Verify(self._shard_watcher)

  def test_case_pass(self):
    """All tasks complete and update succeeds"""
    self.expect_update([0, 1, 2])
    self.expect_watch_shards([0, 1, 2])
    self.expect_update([3, 4, 5])
    self.expect_watch_shards([3, 4, 5])
    self.expect_update([6, 7, 8])
    self.expect_watch_shards([6, 7, 8])
    self.expect_update([9])
    self.expect_watch_shards([9])
    self.replay_mocks()
    self.assert_update_result([])
    self.verify_mocks()

  def test_noop_update(self):
    """No config changes made in any tasks."""
    def expect_noop_update(shards):
      self.expect_update(shards, dict([(s, UNCHANGED) for s in shards]))
    expect_noop_update([0, 1, 2])
    expect_noop_update([3, 4, 5])
    expect_noop_update([6, 7, 8])
    expect_noop_update([9])
    self.replay_mocks()
    self.assert_update_result([])
    self.verify_mocks()

  def test_mixed_update(self):
    """Update that adds tasks and modifies some."""
    self.expect_update([0, 1, 2], {0: RESTARTING, 1: UNCHANGED, 2: RESTARTING})
    self.expect_watch_shards([0, 2])
    self.expect_update([3, 4, 5], {3: UNCHANGED, 4: RESTARTING, 5: RESTARTING})
    self.expect_watch_shards([4, 5])
    self.expect_update([6, 7, 8], {3: UNCHANGED, 4: UNCHANGED, 5: UNCHANGED})
    self.expect_update([9, 10, 11], {9: UNCHANGED, 10: ADDED, 11: ADDED})
    self.expect_watch_shards([10, 11])
    self.replay_mocks()
    self.assert_update_result([], range(12))
    self.verify_mocks()

  def test_initial_failed_shards(self):
    """Tasks 1, 2, 3 fail to move into RUNNING when restarted - Complete rollback performed."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=2, max_per_shard_failures=1)
    self.init_updater(update_config)
    self.expect_update([0, 1, 2])
    self.expect_watch_shards([0, 1, 2], failed_shards=[0, 1, 2])
    self.expect_restart([0, 1, 2])
    self.expect_watch_shards([0, 1, 2], failed_shards=[0, 1, 2])
    self.expect_rollback([0, 1, 2])
    self.expect_watch_shards([0, 1, 2])
    self.replay_mocks()
    self.assert_update_result([0, 1, 2])
    self.verify_mocks()

  def test_all_failed_shards(self):
    """Tests if appropriate calls are made if all shards in an update fail."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=2, max_per_shard_failures=1)
    self.init_updater(update_config)
    self.expect_update([0, 1, 2])
    self.expect_watch_shards([0, 1, 2], failed_shards=[0, 1, 2])
    self.expect_restart([0, 1, 2])
    self.expect_watch_shards([0, 1, 2], failed_shards=[0, 1, 2])
    self.expect_rollback([0, 1, 2])
    self.expect_watch_shards([0, 1, 2])
    self.replay_mocks()
    self.assert_update_result([0, 1, 2], range(3))
    self.verify_mocks()

  def test_successful_shard_restart(self):
    """A shard fails during an initial update but succeeds after a successive restart"""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=2, max_per_shard_failures=1)
    self.init_updater(update_config)
    self.expect_update([0])
    self.expect_watch_shards([0], failed_shards=[0])
    self.expect_restart([0])
    self.expect_watch_shards([0])
    self.replay_mocks()
    self.assert_update_result([], range(1))
    self.verify_mocks()

  def test_single_failed_shard(self):
    """All tasks fail to move into running state when re-started - Complete rollback performed."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=0, max_per_shard_failures=2)
    self.init_updater(update_config)
    self.expect_update([0, 1, 2])
    self.expect_watch_shards([0, 1, 2], failed_shards=[0])
    self.expect_restart([0])
    self.expect_update([3, 4])
    self.expect_watch_shards([0, 3, 4], failed_shards=[0])
    self.expect_restart([0])
    self.expect_update([5, 6])
    self.expect_watch_shards([0, 5, 6], failed_shards=[0])
    self.expect_rollback([0, 1, 2])
    self.expect_watch_shards([0, 1, 2])
    self.expect_rollback([3, 4, 5])
    self.expect_watch_shards([3, 4, 5])
    self.expect_rollback([6])
    self.expect_watch_shards([6])
    self.replay_mocks()
    self.assert_update_result([0])
    self.verify_mocks()

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
