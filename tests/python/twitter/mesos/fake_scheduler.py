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
  """Performs the functions of a mesos scheduler for testing."""

  def __init__(self):
    self._shard_calls = deque()
    self._status_calls = deque()
    self._restart_calls = deque()
    self._rollback_calls = deque()

  def get_shards(self, role, job):
    """Check input paramters with expected paramters queued by expect_get_shards

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.

    Returns a set of shards.
    """
    assert self._shard_calls, 'Unexpected call to get_shards(%s, %s)' % (role, job)
    response = self._shard_calls.popleft()
    args = response[0]
    assert role == args[0] and job == args[1], ('Call to get_shards(%s, %s),'
        ' expected get_shards(%s, %s)' % (role, job, args[0], args[1]))
    return response[1]

  def expect_get_shards(self, role, job, shards):
    """Sets up an expectation for a get_shards call to be made.

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    shards -- a set of shards to queue.
    """
    self._shard_calls.append(((role, job), shards))

  def get_statuses(self, task_ids):
    """Check input paramters with expected paramters queued by expect_get_statuses

    Arguments:
    task_ids -- set of shards to verify validity of the call.

    Returns a map of the current status of the shards.
    """
    assert self._status_calls, 'Unexpected call to get_statuses(%s)' % task_ids
    response = self._status_calls.popleft()
    args = response[0]
    task_ids.sort()
    args.sort()
    assert task_ids == args, ('Call to get_statuses(%s), expected get_statuses(%s)'
        % (task_ids, args))
    return response[1]

  def expect_get_statuses(self, task_ids, statuses):
    """Sets up an expectation for a get_statuses call to be made.

    Arguments:
    task_ids -- set of shards.
    statuses -- map of shards to current status.
    """
    self._status_calls.append((task_ids, statuses))

  def restart_tasks(self, role, job, shard_ids, update_config):
    """Check input paramters with expected paramters queued by expect_restart_tasks.

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    shard_ids -- set of shards to verify the validity of the call.
    update_config -- update configuration object that describes how the restart is performed.

    Returns a set of restarted shards.
    """
    assert self._restart_calls, ('Unexpected call to restart_tasks(%s, %s, %s)'
        % (role, job, shard_ids))
    response = self._restart_calls.popleft()
    assert role == response[0] and job == response[1] and shard_ids == response[2], (
        'Call to restart_tasks(%s, %s, %s), expected restart_tasks(%s, %s, %s)' %
            (role, job, shard_ids, response[0], response[1], response[2]))
    return response[2]

  def expect_restart_tasks(self, role, job, shard_ids):
    """Sets up an expectation for a restart_tasks call to be made.

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    shard_ids -- set of shards.
    restarted_task_map -- a map from task name to shard id.
    """
    self._restart_calls.append((role, job, shard_ids))

  def rollback_tasks(self, role, job, shard_ids, update_config):
    """Check input paramters with expected paramters queued by expect_rollback_tasks.

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    shard_ids -- set of shards to verify the validity of the call.
    update_config -- update configuration object that describes how the restart is performed.
    """
    assert self._rollback_calls, ('Unexpected call to rollback_tasks(%s, %s, %s)'
        % (role, job, shard_ids))
    response = self._rollback_calls.popleft()
    assert role == response[0] and job == response[1] and shard_ids == response[2], (
        'Call to rollback_tasks(%s, %s, %s), expected rollback_tasks(%s, %s, %s)' %
            (role, job, shard_ids, response[0], response[1], response[2]))

  def expect_rollback_tasks(self, role, job, shard_ids):
    """Sets up an expectation for a rollback_tasks call to be made.

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    shard_ids -- set of shards.
    """
    self._rollback_calls.append((role, job, shard_ids))