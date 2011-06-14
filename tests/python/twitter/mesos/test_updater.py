import unittest
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
  @classmethod
  def setup_class(self):
    options.parse()
    self._clock = Clock()
    self._scheduler = FakeScheduler()
    self._updater = Updater('mesos', 'sathya', self._scheduler, self._clock)

  def expect_restart(self,shard_ids):
    task_ids = {}
    for shard_id in shard_ids:
      task_ids[shard_id] = 'task_%s' % shard_id
    self._scheduler.expect_restart_tasks('mesos', 'sathya', set(shard_ids), task_ids)

  def expect_get_statuses(self,statuses):
    self._scheduler.expect_get_statuses(set(statuses.keys()), statuses)

  def clear_scheduler_state(self):
    """Clears the data structures of the fake scheduler"""
    self._scheduler.clear_state()

  def test_case_pass(self):
    self._scheduler.expect_get_shards('mesos', 'sathya', set(range(10)))
    self.expect_restart(set([0, 1, 2]))
    for x in range(18):
      self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING})
    self.expect_restart(set([3, 4, 5]))
    for x in range(18):
      self.expect_get_statuses({3: RUNNING, 4: RUNNING, 5: RUNNING})
    self.expect_restart(set([6, 7, 8]))
    for x in range(18):
      self.expect_get_statuses({6: RUNNING, 7: RUNNING, 8: RUNNING})
    self.expect_restart(set([9]))
    for x in range(18):
       self.expect_get_statuses({9: RUNNING})
    self._updater.update(3, 50, 50)