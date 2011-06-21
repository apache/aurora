import unittest
import pytest
from fake_scheduler import *
from twitter.mesos.mesos.update import *
from twitter.common import options
from mesos_twitter.ttypes import *
from mesos_twitter.ttypes import ScheduleStatus
import twitter.common.log

class UpdaterTest(unittest.TestCase):
  BATCH_SIZE = 3
  RESTART_THRESHOLD = 50
  WATCH_SECS = 50
  EXPECTED_CALLS_NORMAL_CASE = 18
  EXPECTED_CALLS_EXTREME_CASE = 17
  EXPECTED_CALLS_UNKNOWN_CASE = 1

  @classmethod
  def setUpClass(cls):
    options.parse([])
    twitter.common.log.init('Update_test')

  def setUp(self):
    self._clock = Clock()
    self._scheduler = FakeScheduler()
    self._updater = Updater('mesos', 'sathya', self._scheduler, self._clock)
    self._update_config = UpdateConfig()
    self._update_config.batchSize = UpdaterTest.BATCH_SIZE
    self._update_config.restartThreshold = UpdaterTest.RESTART_THRESHOLD
    self._update_config.watchSecs = UpdaterTest.WATCH_SECS

  def expect_restart(self, shard_ids):
    task_ids = {}
    for shard_id in shard_ids:
      task_ids[shard_id] = 'task_%s' % shard_id
    self._scheduler.expect_restart_tasks('mesos', 'sathya', set(shard_ids), task_ids)

  def expect_get_statuses(self, num_calls, statuses):
    for x in range(num_calls):
      self._scheduler.expect_get_statuses(set(statuses.keys()), statuses)

  def test_case_pass(self):
    """All tasks complete and update succeeds"""
    self._scheduler.expect_get_shards('mesos', 'sathya', set(range(10)))
    self.expect_restart(set([0, 1, 2]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {0: ScheduleStatus.RUNNING, 1: ScheduleStatus.RUNNING, 2: ScheduleStatus.RUNNING})
    self.expect_restart(set([3, 4, 5]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {3: ScheduleStatus.RUNNING, 4: ScheduleStatus.RUNNING, 5: ScheduleStatus.RUNNING})
    self.expect_restart(set([6, 7, 8]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {6: ScheduleStatus.RUNNING, 7: ScheduleStatus.RUNNING, 8: ScheduleStatus.RUNNING})
    self.expect_restart(set([9]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {9: ScheduleStatus.RUNNING})
    tasks_expected = set()
    tasks_returned = self._updater.update(self._update_config)
    assert tasks_expected == tasks_returned, ('Expected tasks (%s) : Returned Tasks (%s)' %
      (tasks_expected, tasks_returned))

  def test_tasks_stuck_in_starting(self):
    """Tasks 1, 2, 3 fail to move into running state when restarted"""
    self._scheduler.expect_get_shards('mesos', 'sathya', set(range(10)))
    self.expect_restart(set([0, 1, 2]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {0: ScheduleStatus.STARTING, 1: ScheduleStatus.STARTING, 2: ScheduleStatus.STARTING})
    self.expect_restart(set([3, 4, 5]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {3: ScheduleStatus.RUNNING, 4: ScheduleStatus.RUNNING, 5: ScheduleStatus.RUNNING})
    self.expect_restart(set([6, 7, 8]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {6: ScheduleStatus.RUNNING, 7: ScheduleStatus.RUNNING, 8: ScheduleStatus.RUNNING})
    self.expect_restart(set([9]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE, {9: ScheduleStatus.RUNNING})
    tasks_expected = set([0, 1, 2])
    tasks_returned = self._updater.update(self._update_config)
    assert tasks_expected == tasks_returned, ('Expected tasks (%s) : Returned Tasks (%s)' %
      (tasks_expected, tasks_returned))

  def test_case_all_failed_tasks(self):
    """All tasks fail to move into running state when re-started"""
    self._scheduler.expect_get_shards('mesos', 'sathya', set(range(10)))
    self.expect_restart(set([0, 1, 2]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {0: ScheduleStatus.STARTING, 1: ScheduleStatus.STARTING, 2: ScheduleStatus.STARTING})
    self.expect_restart(set([3, 4, 5]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {3: ScheduleStatus.STARTING, 4: ScheduleStatus.STARTING, 5: ScheduleStatus.STARTING})
    self.expect_restart(set([6, 7, 8]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {6: ScheduleStatus.STARTING, 7: ScheduleStatus.STARTING, 8: ScheduleStatus.STARTING})
    self.expect_restart(set([9]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE, {9: ScheduleStatus.STARTING})
    tasks_expected = set(range(10))
    tasks_returned = self._updater.update(self._update_config)
    assert tasks_expected == tasks_returned, ('Expected tasks (%s) : Returned Tasks (%s)' %
      (tasks_expected, tasks_returned))

  def test_case_extreme_pass(self):
    """All tasks move into running state at the end of restart threshold"""
    self._scheduler.expect_get_shards('mesos', 'sathya', set(range(10)))
    self.expect_restart(set([0, 1, 2]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_EXTREME_CASE,
      {0: ScheduleStatus.STARTING, 1: ScheduleStatus.STARTING, 2: ScheduleStatus.STARTING})
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {0: ScheduleStatus.RUNNING, 1: ScheduleStatus.RUNNING, 2: ScheduleStatus.RUNNING})
    self.expect_restart(set([3, 4, 5]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_EXTREME_CASE,
      {3: ScheduleStatus.STARTING, 4: ScheduleStatus.STARTING, 5: ScheduleStatus.STARTING})
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {3: ScheduleStatus.RUNNING, 4: ScheduleStatus.RUNNING, 5: ScheduleStatus.RUNNING})
    self.expect_restart(set([6, 7, 8]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_EXTREME_CASE,
      {6: ScheduleStatus.STARTING, 7: ScheduleStatus.STARTING, 8: ScheduleStatus.STARTING})
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {6: ScheduleStatus.RUNNING, 7: ScheduleStatus.RUNNING, 8: ScheduleStatus.RUNNING})
    self.expect_restart(set([9]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_EXTREME_CASE, {9: ScheduleStatus.STARTING})
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE, {9: ScheduleStatus.RUNNING})
    tasks_expected = set()
    tasks_returned = self._updater.update(self._update_config)
    assert tasks_expected == tasks_returned, ('Expected tasks (%s) : Returned Tasks (%s)' %
      (tasks_expected, tasks_returned))

  def test_case_unknown_state(self):
    """All tasks move into an unexpected state"""
    self._scheduler.expect_get_shards('mesos', 'sathya', set(range(10)))
    self.expect_restart(set([0, 1, 2]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_UNKNOWN_CASE,
      {0: ScheduleStatus.FINISHED, 1: ScheduleStatus.FINISHED, 2: ScheduleStatus.FINISHED})
    self.expect_restart(set([3, 4, 5]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_UNKNOWN_CASE,
      {3: ScheduleStatus.FINISHED, 4: ScheduleStatus.FINISHED, 5: ScheduleStatus.FINISHED})
    self.expect_restart(set([6, 7, 8]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_UNKNOWN_CASE,
      {6: ScheduleStatus.FINISHED, 7: ScheduleStatus.FINISHED, 8: ScheduleStatus.FINISHED})
    self.expect_restart(set([9]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_UNKNOWN_CASE, {9: ScheduleStatus.FINISHED})
    tasks_expected = set(range(10))
    tasks_returned = self._updater.update(self._update_config)
    assert tasks_expected == tasks_returned, ('Expected tasks (%s) : Returned Tasks (%s)' %
      (tasks_expected, tasks_returned))

  def test_invalid_batch_size(self):
    """Test for out of range error for batch size"""
    self._update_config.batchSize = 0
    self._scheduler.expect_get_shards('mesos', 'sathya', set(range(10)))
    with pytest.raises(InvalidUpdaterConfigException):
      self._updater.update(self._update_config)

  def test_invalid_restart_threshold(self):
    """Test for out of range error for restart threshold"""
    self._update_config.restartThreshold = 0
    self._scheduler.expect_get_shards('mesos', 'sathya', set(range(10)))
    with pytest.raises(InvalidUpdaterConfigException):
      self._updater.update(self._update_config)

  def test_invalid_watch_secs(self):
    """Test for out of range error for watch secs"""
    self._update_config.watchSecs = 0
    self._scheduler.expect_get_shards('mesos', 'sathya', set(range(10)))
    with pytest.raises(InvalidUpdaterConfigException):
      self._updater.update(self._update_config)
