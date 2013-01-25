from math import ceil
import copy
import unittest
import pytest

from gen.twitter.mesos.ttypes import *
from twitter.mesos.updater import Updater

from fake_scheduler import *

RUNNING = ScheduleStatus.RUNNING


ADDED      = ShardUpdateResult.ADDED
RESTARTING = ShardUpdateResult.RESTARTING
UNCHANGED  = ShardUpdateResult.UNCHANGED


def find_expected_status_calls(watch_secs, sleep_secs):
  return ceil(watch_secs / sleep_secs)

class UpdaterTest(unittest.TestCase):
  UPDATE_CONFIG = {
    'batchSize':           3,
    'restartThreshold':   50,
    'watchSecs':          50,
    'maxPerShardFailures': 0,
    'maxTotalFailures':    0,
  }
  EXPECTED_GET_STATUS_CALLS = (find_expected_status_calls(UPDATE_CONFIG['watchSecs'], 3.0) + 1)

  def setUp(self):
    self._clock = Clock()
    self._scheduler = FakeScheduler()
    self._updater = Updater('mesos', 'jimbob', self._scheduler, self._clock, 'test_update')
    self._update_config = copy.deepcopy(UpdaterTest.UPDATE_CONFIG)
    self._initial_shards = range(10)

  def expect_restart(self, shard_ids, shard_results=None):
    self._scheduler.expect_updateShards('mesos',
                                        'jimbob',
                                        shard_ids,
                                        'test_update',
                                        shard_results)

  def expect_rollback(self, shard_ids, shard_results=None):
    self._scheduler.expect_rollbackShards('mesos',
                                          'jimbob',
                                          shard_ids,
                                          'test_update',
                                          shard_results)

  def expect_get_statuses(self, statuses, num_calls=EXPECTED_GET_STATUS_CALLS):
    for x in range(int(num_calls)):
      self._scheduler.expect_getTasksStatus(statuses)

  def assert_update_result(self, expected_failed_shards, update_shards=None):
    update_shards = update_shards or self._initial_shards
    shards_returned = self._updater.update(self._update_config, update_shards)
    assert expected_failed_shards == shards_returned, (
        'Expected shards (%s) : Returned shards (%s)' % (expected_failed_shards, shards_returned))
    self.verify()

  def verify(self):
    self._scheduler.verify()

  def test_case_pass(self):
    """All tasks complete and update succeeds"""
    self.expect_restart([0, 1, 2])
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING})
    self.expect_restart([3, 4, 5])
    self.expect_get_statuses({3: RUNNING, 4: RUNNING, 5: RUNNING})
    self.expect_restart([6, 7, 8])
    self.expect_get_statuses({6: RUNNING, 7: RUNNING, 8: RUNNING})
    self.expect_restart([9])
    self.expect_get_statuses({9: RUNNING})
    self.assert_update_result([])

  def test_noop_update(self):
    """No config changes made in any tasks."""
    def expect_noop_restart(shards):
      self.expect_restart(shards, dict([(s, UNCHANGED) for s in shards]))
    expect_noop_restart([0, 1, 2])
    expect_noop_restart([3, 4, 5])
    expect_noop_restart([6, 7, 8])
    expect_noop_restart([9])
    self.assert_update_result([])

  def test_mixed_update(self):
    """Update that adds tasks and modifies some."""
    self.expect_restart([0, 1, 2], {0: RESTARTING, 1: UNCHANGED, 2: RESTARTING})
    self.expect_get_statuses({0: RUNNING, 2: RUNNING})
    self.expect_restart([3, 4, 5], {3: UNCHANGED, 4: RESTARTING, 5: RESTARTING})
    self.expect_get_statuses({4: RUNNING, 5: RUNNING})
    self.expect_restart([6, 7, 8], {3: UNCHANGED, 4: UNCHANGED, 5: UNCHANGED})
    self.expect_restart([9, 10, 11], {9: UNCHANGED, 10: ADDED, 11: ADDED})
    self.expect_get_statuses({10: RUNNING, 11: RUNNING})
    self.assert_update_result([], range(12))

  def test_tasks_stuck_in_starting(self):
    """Tasks 1, 2, 3 fail to move into RUNNING when restarted - Complete rollback performed."""
    self._update_config['maxTotalFailures'] = 2
    self._update_config['maxPerShardFailures'] = 1
    self.expect_restart([0, 1, 2])
    self.expect_get_statuses({})
    self.expect_restart([0, 1, 2])
    self.expect_get_statuses({})
    self.expect_rollback([0, 1, 2])
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING})
    self.assert_update_result([0, 1, 2])

  def test_single_failed_shard(self):
    """All tasks fail to move into running state when re-started - Complete rollback performed."""
    self._update_config['maxTotalFailures'] = 0
    self._update_config['maxPerShardFailures'] = 2
    self.expect_restart([0, 1, 2])
    self.expect_get_statuses({1: RUNNING, 2: RUNNING})
    self.expect_restart([0, 3, 4])
    self.expect_get_statuses({3: RUNNING, 4: RUNNING})
    self.expect_restart([0, 5, 6])
    self.expect_get_statuses({5: RUNNING, 6: RUNNING})
    self.expect_rollback([0, 1, 2])
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING})
    self.expect_rollback([3, 4, 5])
    self.expect_get_statuses({3: RUNNING, 4: RUNNING, 5: RUNNING})
    self.expect_rollback([6])
    self.expect_get_statuses({6: RUNNING})
    self.assert_update_result([0])

  def test_shard_state_transition(self):
    """All tasks move into running state at the end of restart threshold."""
    self.expect_restart([0, 1, 2])
    self.expect_get_statuses({}, num_calls=(self.EXPECTED_GET_STATUS_CALLS - 1))
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING})
    self.expect_restart([3, 4, 5])
    self.expect_get_statuses({}, self.EXPECTED_GET_STATUS_CALLS - 1)
    self.expect_get_statuses({3: RUNNING, 4: RUNNING, 5: RUNNING})
    self.expect_restart([6, 7, 8])
    self.expect_get_statuses({}, self.EXPECTED_GET_STATUS_CALLS - 1)
    self.expect_get_statuses({6: RUNNING, 7: RUNNING, 8: RUNNING})
    self.expect_restart([9])
    self.expect_get_statuses({}, self.EXPECTED_GET_STATUS_CALLS - 1)
    self.expect_get_statuses({9: RUNNING})
    self.assert_update_result([])

  def test_case_unknown_state(self):
    """All tasks move into an unexpected state - Complete rollback performed."""
    self._update_config['maxTotalFailures'] = 2
    self._update_config['maxPerShardFailures'] = 1
    self.expect_restart([0, 1, 2])
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING}, num_calls=1)
    self.expect_get_statuses({}, num_calls=1)
    self.expect_restart([0, 1, 2])
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING}, num_calls=1)
    self.expect_get_statuses({}, num_calls=1)
    self.expect_rollback([0, 1, 2])
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING})
    self.assert_update_result([0, 1, 2])

  def test_invalid_batch_size(self):
    """Test for out of range error for batch size"""
    self._update_config['batchSize'] = 0
    with pytest.raises(Updater.InvalidConfigError):
      self._updater.update(self._update_config, self._initial_shards)
    self.verify()

  def test_invalid_restart_threshold(self):
    """Test for out of range error for restart threshold"""
    self._update_config['restartThreshold'] = 0
    with pytest.raises(Updater.InvalidConfigError):
      self._updater.update(self._update_config, self._initial_shards)
    self.verify()

  def test_invalid_watch_secs(self):
    """Test for out of range error for watch secs"""
    self._update_config['watchSecs'] = 0
    with pytest.raises(Updater.InvalidConfigError):
      self._updater.update(self._update_config, self._initial_shards)
    self.verify()
