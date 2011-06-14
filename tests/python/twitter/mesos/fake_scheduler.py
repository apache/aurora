from collections import deque

class Clock:
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

class FakeScheduler:
  """Performs the functions of a mesos scheduler for testing"""

  def __init__(self):
    self._shard_calls = deque()
    self._status_calls = deque()
    self._restart_calls = deque()

  def get_shards(self, role, job):
    """Returns the shards as queued by the test script.

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    """
    assert self._shard_calls, 'Unexpected call to get_shards(%s, %s)' % (role, job)
    response = self._shard_calls.popleft()
    args = response[0]
    assert role == args[0] and job == args[1], ('Call to get_shards(%s, %s),'
        ' expected get_shards(%s, %s)' % (role, job, args[0], args[1]))
    return response[1]

  def expect_get_shards(self, role, job, shards):
    """Called by the test script to queue the scheduler with the shards for role and job.

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    shards -- a set of shards to queue.
    """
    self._shard_calls.append(((role, job), shards))

  def get_statuses(self, task_ids):
    """Returns a map of the current status of the shards.

    Arguments:
    task_ids -- set of shards to verify validity of the call.
    """
    assert self._status_calls, 'Unexpected call to get_statuses(%s)' % task_ids
    response = self._status_calls.popleft()
    args = response[0]
    assert task_ids == args, ('Call to get_statuses(%s), expected get_statuses(%s)'
        % (task_ids, args))
    return response[1]

  def expect_get_statuses(self, task_ids, statuses):
    """Called by the test script to queue the status of shards for the next call.

    Arguments:
    task_ids -- set of shards.
    statuses -- map of shards to current status.
    """
    self._status_calls.append((task_ids, statuses))

  def restart_tasks(self, role, job, shard_ids):
    """Returns a map of the tasks restarted

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    shard_ids -- set of shards to verify the validity of the call.
    """
    assert self._restart_calls, ('Unexpected call to restart_tasks(%s, %s, %s)'
        % (role, job, shard_ids))
    response = self._restart_calls.popleft()
    args = response[0]
    assert role == args[0] and job == args[1] and shard_ids == args[2], (
        'Call to restart_tasks(%s, %s, %s), expected restart_tasks(%s, %s, %s)' %
        (role, job, shard_ids, args[0], args[1], args[2]))
    return response[1]

  def expect_restart_tasks(self, role, job, shard_ids, restarted_task_map):
    """Called by the test script to queue the next restart call.

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    shard_ids -- set of shards.
    restarted_task_map -- a map from task name to shard id.
    """
    self._restart_calls.append(((role, job, shard_ids), restarted_task_map))

  def clear_state(self):
    """Clear the data structures of the scheduler for a new test run"""
    self._shard_calls.clear()
    self._status_calls.clear()
    self._restart_calls.clear()
