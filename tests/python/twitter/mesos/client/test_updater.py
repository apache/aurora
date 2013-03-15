from collections import deque
import copy
from math import ceil
import unittest

from twitter.mesos.client.updater import Updater

from gen.twitter.mesos.constants import DEFAULT_ENVIRONMENT
from gen.twitter.mesos.ttypes import *

import pytest


RUNNING    = ScheduleStatus.RUNNING
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


class FakeScheduler(object):
  """Performs the functions of a mesos scheduler for testing."""

  def __init__(self):
    self._status_calls = deque()
    self._restart_calls = deque()
    self._rollback_calls = deque()

  def verify(self):
    assert not self._status_calls, 'Expected status calls not made: %s' % self._status_calls
    assert not self._restart_calls, 'Expected restart calls not made: %s' % self._restart_calls
    assert not self._rollback_calls, 'Exepcted rollback calls not made: %s' % self._rollback_calls

  def getTasksStatus(self, query):
    """Check input paramters with expected paramters queued by expect_get_statuses

    Arguments:
    query -- query object.

    Returns a map of the current status of the shards.
    """
    assert self._status_calls, 'Unexpected call to get_statuses(%s)' % query.jobName
    statuses = self._status_calls.popleft()
    response = ScheduleStatusResponse(responseCode = ResponseCode.OK, message = 'test', tasks = [])
    for shard in statuses:
      response.tasks += [ScheduledTask(assignedTask = AssignedTask(
          task = TwitterTaskInfo(shardId = shard)), status = statuses[shard])]
    return response

  def expect_getTasksStatus(self, statuses):
    """Sets up an expectation for a get_statuses call to be made.

    Arguments:
    statuses -- map of shards to current status.
    """
    self._status_calls.append(statuses)

  def _handle_update(self, name, call_queue, actual_args):
    assert call_queue, 'Unexpected call to %s(%s)' % (name, str(actual_args))
    response = call_queue.popleft()
    expected = response[:-1]
    assert expected == actual_args, ('Call to %s(%s), expected %s(%s)'
                                     % (name, str(actual_args), name, str(expected)))
    resp = UpdateShardsResponse(responseCode=UpdateResponseCode.OK, message='test')
    resp.shards = response[-1]
    return resp

  def updateShards(self, role, job_name, job, shard_ids, update_token):
    """Check input paramters with expected paramters queued by expect_restart_tasks.

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    shard_ids -- set of shards to verify the validity of the call.
    update_token -- unique token identifying the current update.

    Returns an UpdateResponseCode OK
    """
    return self._handle_update('updateShards',
                               self._restart_calls,
                               (role, job_name, job, shard_ids, update_token))

  def expect_updateShards(self, role, job_name, job, shard_ids, update_token, shard_results):
    """Sets up an expectation for a restart_tasks call to be made.

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    shard_ids -- set of shards.
    update_token -- unique token identifying the current update.
    """
    self._restart_calls.append((role, job_name, job, shard_ids, update_token, shard_results))

  def rollbackShards(self, role, job_name, job, shard_ids, update_token):
    """Check input paramters with expected paramters queued by expect_rollback_tasks.

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    shard_ids -- set of shards to verify the validity of the call.
    update_token -- unique token identifying the current update.

    Returns an UpdateResponseCode OK
    """
    return self._handle_update('rollbackShards',
                               self._rollback_calls,
                               (role, job_name, job, shard_ids, update_token))

  def expect_rollbackShards(self, role, job_name, job, shard_ids, update_token, shard_results):
    """Sets up an expectation for a rollback_tasks call to be made.

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    shard_ids -- set of shards.
    update_token -- unique token identifying the current update.
    """
    self._rollback_calls.append((role, job_name, job, shard_ids, update_token, shard_results))


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
    self._clock = Clock()
    self._scheduler = FakeScheduler()
    self._initial_shards = range(10)
    self.init_updater(copy.deepcopy(self.UPDATE_CONFIG))

  def init_updater(self, update_config):
    config = FakeConfig('mesos', 'jimbob', update_config)
    self._updater = Updater(config, self._scheduler, self._clock)
    self._updater._update_token = 'test_update'

  def expect_restart(self, shard_ids, shard_results=None):
    self._scheduler.expect_updateShards('mesos', 'jimbob',
      JobKey(role='mesos', environment=DEFAULT_ENVIRONMENT, name='jimbob'),
      shard_ids, 'test_update', shard_results)

  def expect_rollback(self, shard_ids, shard_results=None):
    self._scheduler.expect_rollbackShards(
      'mesos', 'jimbob',
      JobKey(role='mesos', environment=DEFAULT_ENVIRONMENT, name='jimbob'),
      shard_ids, 'test_update', shard_results)

  def expect_get_statuses(self, statuses, num_calls=EXPECTED_GET_STATUS_CALLS):
    for x in range(int(num_calls)):
      self._scheduler.expect_getTasksStatus(statuses)

  def assert_update_result(self, expected_failed_shards, update_shards=None):
    update_shards = update_shards or self._initial_shards
    shards_returned = self._updater.update(update_shards)
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
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures = 2, max_per_shard_failures = 1)
    self.init_updater(update_config)
    self.expect_restart([0, 1, 2])
    self.expect_get_statuses({})
    self.expect_restart([0, 1, 2])
    self.expect_get_statuses({})
    self.expect_rollback([0, 1, 2])
    self.expect_get_statuses({0: RUNNING, 1: RUNNING, 2: RUNNING})
    self.assert_update_result([0, 1, 2])

  def test_single_failed_shard(self):
    """All tasks fail to move into running state when re-started - Complete rollback performed."""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures = 0, max_per_shard_failures = 2)
    self.init_updater(update_config)
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
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(max_total_failures = 2, max_per_shard_failures = 1)
    self.init_updater(update_config)
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
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(batch_size = 0)
    with pytest.raises(Updater.InvalidConfigError):
      self.init_updater(update_config)
    self.verify()

  def test_invalid_restart_threshold(self):
    """Test for out of range error for restart threshold"""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(restart_threshold = 0)
    with pytest.raises(Updater.InvalidConfigError):
      self.init_updater(update_config)
    self.verify()

  def test_invalid_watch_secs(self):
    """Test for out of range error for watch secs"""
    update_config = self.UPDATE_CONFIG.copy()
    update_config.update(watch_secs = 0)
    with pytest.raises(Updater.InvalidConfigError):
      self.init_updater(update_config)
    self.verify()
