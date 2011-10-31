from collections import deque
from gen.twitter.mesos.ttypes import *

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

  def updateShards(self, role, job, shard_ids, update_token, session):
    """Check input paramters with expected paramters queued by expect_restart_tasks.

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    shard_ids -- set of shards to verify the validity of the call.
    update_token -- unique token identifying the current update.

    Returns an UpdateResponseCode OK
    """
    assert self._restart_calls, ('Unexpected call to restart_tasks(%s, %s, %s)'
        % (role, job, shard_ids))
    response = self._restart_calls.popleft()
    assert role == response[0] and job == response[1] and shard_ids == response[2], (
        'Call to restart_tasks(%s, %s, %s), expected restart_tasks(%s, %s, %s)' %
            (role, job, shard_ids, response[0], response[1], response[2]))
    resp = UpdateShardsResponse(responseCode = UpdateResponseCode.OK, message = 'test')
    return resp

  def expect_updateShards(self, role, job, shard_ids, update_token):
    """Sets up an expectation for a restart_tasks call to be made.

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    shard_ids -- set of shards.
    update_token -- unique token identifying the current update.
    """
    self._restart_calls.append((role, job, shard_ids))

  def rollbackShards(self, role, job, shard_ids, update_token, session):
    """Check input paramters with expected paramters queued by expect_rollback_tasks.

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    shard_ids -- set of shards to verify the validity of the call.
    update_token -- unique token identifying the current update.

    Returns an UpdateResponseCode OK
    """
    assert self._rollback_calls, ('Unexpected call to rollback_tasks(%s, %s, %s)'
        % (role, job, shard_ids))
    response = self._rollback_calls.popleft()
    assert role == response[0] and job == response[1] and shard_ids == response[2], (
        'Call to rollback_tasks(%s, %s, %s), expected rollback_tasks(%s, %s, %s)' %
            (role, job, shard_ids, response[0], response[1], response[2]))
    resp = UpdateShardsResponse(responseCode = UpdateResponseCode.OK, message = 'test')
    return resp

  def expect_rollbackShards(self, role, job, shard_ids, update_token):
    """Sets up an expectation for a rollback_tasks call to be made.

    Arguments:
    role -- string specifying the role.
    job -- string specifying the job.
    shard_ids -- set of shards.
    update_token -- unique token identifying the current update.
    """
    self._rollback_calls.append((role, job, shard_ids))
