import copy
import unittest

from twitter.aurora.client.api.instance_watcher import InstanceWatcher
from twitter.aurora.client.api.updater import Updater

from gen.twitter.aurora.ttypes import *
from gen.twitter.aurora.AuroraSchedulerManager import Client as scheduler_client

# test space
from twitter.aurora.client.fake_scheduler_proxy import FakeSchedulerProxy

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


class UpdaterTest(unittest.TestCase):
  UPDATE_CONFIG = {
    'batch_size':                 3,
    'restart_threshold':          50,
    'watch_secs':                 50,
    'max_per_instance_failures':  0,
    'max_total_failures':         0,
  }

  def setUp(self):
    self._role = 'mesos'
    self._name = 'jimbob'
    self._env = 'test'
    self._job_key = JobKey(name=self._name, environment=self._env, role=self._role)
    self._session_key = 'test_session'
    self._update_token = 'test_update'
    self._instance_watcher = mox.MockObject(InstanceWatcher)
    self._scheduler = mox.MockObject(scheduler_client)
    self._scheduler_proxy = FakeSchedulerProxy('test-cluster', self._scheduler, self._session_key)
    self._initial_instances = range(10)
    self.init_updater(copy.deepcopy(self.UPDATE_CONFIG))

  def init_updater(self, update_config):
    config = FakeConfig(self._role, self._name, self._env, update_config)
    self._updater = Updater(config, self._scheduler_proxy)
    self._updater._update_token = self._update_token

  def expect_update(self, instance_ids, instance_results=None):
    response = Response(responseCode=ResponseCode.OK, message='test')
    result = UpdateShardsResult(shards=instance_results)
    response.result = Result(updateShardsResult=result)
    self._scheduler.updateShards(self._job_key, instance_ids, self._update_token, self._session_key
        ).AndReturn(response)

  def expect_invalid_update(self):
    response = Response(responseCode=ResponseCode.INVALID_REQUEST)
    self._scheduler.updateShards(self._job_key, mox.IgnoreArg(), mox.IgnoreArg(), self._session_key
        ).AndReturn(response)


  def expect_restart(self, instance_ids):
    response = Response(responseCode=ResponseCode.OK, message='test')
    self._scheduler.restartShards(self._job_key, instance_ids, self._session_key).AndReturn(response)

  def expect_rollback(self, instance_ids, instance_results=None):
    response = Response(responseCode=ResponseCode.OK, message='test')
    result = RollbackShardsResult(shards=instance_results)
    response.result = Result(rollbackShardsResult=result)
    self._scheduler.rollbackShards(self._job_key, instance_ids, self._update_token, self._session_key
        ).AndReturn(response)

  def expect_watch_instances(self, instance_ids, failed_instances=[]):
    self._instance_watcher.watch(instance_ids).AndReturn(set(failed_instances))

  def assert_update_result(self, expected_failed_instances, update_instances=None):
    update_instances = update_instances or self._initial_instances
    health_check_interval_seconds = 3
    instances_returned = self._updater.update(
      update_instances, health_check_interval_seconds, self._instance_watcher)
    assert set(expected_failed_instances) == instances_returned, (
        'Expected instances (%s) : Returned instances (%s)' % (expected_failed_instances, instances_returned))

  def replay_mocks(self):
    mox.Replay(self._scheduler)
    mox.Replay(self._instance_watcher)

  def verify_mocks(self):
    mox.Verify(self._scheduler)
    mox.Verify(self._instance_watcher)

  def test_case_pass(self):
    """All tasks complete and update succeeds"""
    self.expect_update([0, 1, 2])
    self.expect_watch_instances([0, 1, 2])
    self.expect_update([3, 4, 5])
    self.expect_watch_instances([3, 4, 5])
    self.expect_update([6, 7, 8])
    self.expect_watch_instances([6, 7, 8])
    self.expect_update([9])
    self.expect_watch_instances([9])
    self.replay_mocks()
    self.assert_update_result([])
    self.verify_mocks()

  def test_noop_update(self):
    """No config changes made in any tasks."""
    def expect_noop_update(instances):
      self.expect_update(instances, dict([(s, UNCHANGED) for s in instances]))
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
    self.expect_watch_instances([0, 2])
    self.expect_update([3, 4, 5], {3: UNCHANGED, 4: RESTARTING, 5: RESTARTING})
    self.expect_watch_instances([4, 5])
    self.expect_update([6, 7, 8], {3: UNCHANGED, 4: UNCHANGED, 5: UNCHANGED})
    self.expect_update([9, 10, 11], {9: UNCHANGED, 10: ADDED, 11: ADDED})
    self.expect_watch_instances([10, 11])
    self.replay_mocks()
    self.assert_update_result([], range(12))
    self.verify_mocks()

  def test_initial_failed_instances(self):
    """Tasks 1, 2, 3 fail to move into RUNNING when restarted - Complete rollback performed."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=2, max_per_instance_failures=1)
    self.init_updater(update_config)
    self.expect_update([0, 1, 2])
    self.expect_watch_instances([0, 1, 2], failed_instances=[0, 1, 2])
    self.expect_restart([0, 1, 2])
    self.expect_watch_instances([0, 1, 2], failed_instances=[0, 1, 2])
    self.expect_rollback([0, 1, 2])
    self.expect_watch_instances([0, 1, 2])
    self.replay_mocks()
    self.assert_update_result([0, 1, 2])
    self.verify_mocks()

  def test_all_failed_instances(self):
    """Tests if appropriate calls are made if all instances in an update fail."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=2, max_per_instance_failures=1)
    self.init_updater(update_config)
    self.expect_update([0, 1, 2])
    self.expect_watch_instances([0, 1, 2], failed_instances=[0, 1, 2])
    self.expect_restart([0, 1, 2])
    self.expect_watch_instances([0, 1, 2], failed_instances=[0, 1, 2])
    self.expect_rollback([0, 1, 2])
    self.expect_watch_instances([0, 1, 2])
    self.replay_mocks()
    self.assert_update_result([0, 1, 2], range(3))
    self.verify_mocks()

  def test_successful_instance_restart(self):
    """An instance fails during an initial update but succeeds after a successive restart"""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=2, max_per_instance_failures=1)
    self.init_updater(update_config)
    self.expect_update([0])
    self.expect_watch_instances([0], failed_instances=[0])
    self.expect_restart([0])
    self.expect_watch_instances([0])
    self.replay_mocks()
    self.assert_update_result([], range(1))
    self.verify_mocks()

  def test_single_failed_instance(self):
    """All tasks fail to move into running state when re-started - Complete rollback performed."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures=0, max_per_instance_failures=2)
    self.init_updater(update_config)
    self.expect_update([0, 1, 2])
    self.expect_watch_instances([0, 1, 2], failed_instances=[0])
    self.expect_restart([0])
    self.expect_update([3, 4])
    self.expect_watch_instances([0, 3, 4], failed_instances=[0])
    self.expect_restart([0])
    self.expect_update([5, 6])
    self.expect_watch_instances([0, 5, 6], failed_instances=[0])
    self.expect_rollback([0, 1, 2])
    self.expect_watch_instances([0, 1, 2])
    self.expect_rollback([3, 4, 5])
    self.expect_watch_instances([3, 4, 5])
    self.expect_rollback([6])
    self.expect_watch_instances([6])
    self.replay_mocks()
    self.assert_update_result([0])
    self.verify_mocks()

  def test_invalid_response(self):
    """A response code other than success is returned by a scheduler RPC."""
    update_config = self.UPDATE_CONFIG.copy()
    self.init_updater(update_config)
    self.expect_invalid_update()

    self.replay_mocks()
    with pytest.raises(Updater.Error):
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
