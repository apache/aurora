import unittest
from math import ceil
from fake_scheduler import *
from update import *
from twitter.common import options

PENDING  = 0
ASSIGNED = 1
STARTING = 2
RUNNING  = 3
FINISHED = 4
FAILED = 5

class UpdaterTest(unittest.TestCase):
  BATCH_SIZE = 3
  RESTART_THRESHOLD = 50
  WATCH_SECS = 50
  EXPECTED_CALLS_NORMAL_CASE = 18
  EXPECTED_CALLS_EXTREME_CASE = 17
  EXPECTED_CALLS_UNKNOWN_CASE = 1
  @classmethod
  def setup_class(self):
    options.parse()

  def setup_method(self, method):
    self._clock = Clock()
    self._scheduler = FakeScheduler()
    self._updater = Updater('mesos', 'sathya', self._scheduler, self._clock)

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
      {0: RUNNING, 1: RUNNING, 2: RUNNING})
    self.expect_restart(set([3, 4, 5]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {3: RUNNING, 4: RUNNING, 5: RUNNING})
    self.expect_restart(set([6, 7, 8]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {6: RUNNING, 7: RUNNING, 8: RUNNING})
    self.expect_restart(set([9]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {9: RUNNING})
    tasks_expected = set()
    tasks_returned = self._updater.update(UpdaterTest.BATCH_SIZE, UpdaterTest.RESTART_THRESHOLD,
      UpdaterTest.WATCH_SECS)
    assert tasks_expected == tasks_returned, ('Expected tasks (%s) : Returned Tasks (%s)' %
      (tasks_expected, tasks_returned))

  def test_tasks_stuck_in_starting(self):
    """Tasks 1, 2, 3 fail to move into running state when restarted"""
    self._scheduler.expect_get_shards('mesos', 'sathya', set(range(10)))
    self.expect_restart(set([0, 1, 2]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {0: STARTING, 1: STARTING, 2: STARTING})
    self.expect_restart(set([3, 4, 5]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {3: RUNNING, 4: RUNNING, 5: RUNNING})
    self.expect_restart(set([6, 7, 8]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {6: RUNNING, 7: RUNNING, 8: RUNNING})
    self.expect_restart(set([9]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE, {9: RUNNING})
    tasks_expected = set([0, 1, 2])
    tasks_returned = self._updater.update(UpdaterTest.BATCH_SIZE, UpdaterTest.RESTART_THRESHOLD,
      UpdaterTest.WATCH_SECS)
    assert tasks_expected == tasks_returned, ('Expected tasks (%s) : Returned Tasks (%s)' %
      (tasks_expected, tasks_returned))

  def test_case_all_failed_tasks(self):
    """All tasks fail to move into running state when re-started"""
    self._scheduler.expect_get_shards('mesos', 'sathya', set(range(10)))
    self.expect_restart(set([0, 1, 2]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {0: STARTING, 1: STARTING, 2: STARTING})
    self.expect_restart(set([3, 4, 5]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {3: STARTING, 4: STARTING, 5: STARTING})
    self.expect_restart(set([6, 7, 8]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {6: STARTING, 7: STARTING, 8: STARTING})
    self.expect_restart(set([9]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE, {9: STARTING})
    tasks_expected = set(range(10))
    tasks_returned = self._updater.update(UpdaterTest.BATCH_SIZE, UpdaterTest.RESTART_THRESHOLD,
      UpdaterTest.WATCH_SECS)
    assert tasks_expected == tasks_returned, ('Expected tasks (%s) : Returned Tasks (%s)' %
      (tasks_expected, tasks_returned))

  def test_case_extreme_pass(self):
    """All tasks move into running state at the end of restart threshold"""
    self._scheduler.expect_get_shards('mesos', 'sathya', set(range(10)))
    self.expect_restart(set([0, 1, 2]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_EXTREME_CASE,
      {0: STARTING, 1: STARTING, 2: STARTING})
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {0: RUNNING, 1: RUNNING, 2: RUNNING})
    self.expect_restart(set([3, 4, 5]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_EXTREME_CASE,
      {3: STARTING, 4: STARTING, 5: STARTING})
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {3: RUNNING, 4: RUNNING, 5: RUNNING})
    self.expect_restart(set([6, 7, 8]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_EXTREME_CASE,
      {6: STARTING, 7: STARTING, 8: STARTING})
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE,
      {6: RUNNING, 7: RUNNING, 8: RUNNING})
    self.expect_restart(set([9]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_EXTREME_CASE, {9: STARTING})
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_NORMAL_CASE, {9: RUNNING})
    tasks_expected = set()
    tasks_returned = self._updater.update(UpdaterTest.BATCH_SIZE, UpdaterTest.RESTART_THRESHOLD,
      UpdaterTest.WATCH_SECS)
    assert tasks_expected == tasks_returned, ('Expected tasks (%s) : Returned Tasks (%s)' %
      (tasks_expected, tasks_returned))

  def test_case_unknown_state(self):
    """All tasks move into an unexpected state"""
    self._scheduler.expect_get_shards('mesos', 'sathya', set(range(10)))
    self.expect_restart(set([0, 1, 2]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_UNKNOWN_CASE,
      {0: FINISHED, 1: FINISHED, 2: FINISHED})
    self.expect_restart(set([3, 4, 5]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_UNKNOWN_CASE,
      {3: FINISHED, 4: FINISHED, 5: FINISHED})
    self.expect_restart(set([6, 7, 8]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_UNKNOWN_CASE,
      {6: FINISHED, 7: FINISHED, 8: FINISHED})
    self.expect_restart(set([9]))
    self.expect_get_statuses(UpdaterTest.EXPECTED_CALLS_UNKNOWN_CASE, {9: FINISHED})
    tasks_expected = set(range(10))
    tasks_returned = self._updater.update(UpdaterTest.BATCH_SIZE, UpdaterTest.RESTART_THRESHOLD,
      UpdaterTest.WATCH_SECS)
    assert tasks_expected == tasks_returned, ('Expected tasks (%s) : Returned Tasks (%s)' %
      (tasks_expected, tasks_returned))
