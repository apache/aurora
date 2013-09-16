import time

from twitter.common.quantity import Amount, Time

from gen.twitter.aurora.constants import (
    LIVE_STATES,
    TERMINAL_STATES
)
from gen.twitter.aurora.ttypes import (
    Identity,
    TaskQuery
)

from thrift.transport import TTransport


class JobMonitor(object):
  MIN_POLL_INTERVAL = Amount(10, Time.SECONDS)
  MAX_POLL_INTERVAL = Amount(2, Time.MINUTES)

  @classmethod
  def running_or_finished(cls, status):
    return status in (LIVE_STATES | TERMINAL_STATES)

  @classmethod
  def terminal(cls, status):
    return status in TERMINAL_STATES

  # TODO(ksweeney): Make this use the AuroraJobKey
  def __init__(self, client, role, env, jobname):
    self._client = client
    self._query = TaskQuery(owner=Identity(role=role), environment=env, jobName=jobname)
    self._initial_tasks = set()
    self._initial_tasks = set(task.assignedTask.taskId for task in self.iter_query())

  def iter_query(self):
    try:
      res = self._client.scheduler.getTasksStatus(self._query)
    except TTransport.TTransportException as e:
      print('Failed to query slaves from scheduler: %s' % e)
      return
    if res is None or res.result is None:
      return
    for task in res.result.scheduleStatusResult.tasks:
      if task.assignedTask.taskId not in self._initial_tasks:
        yield task

  def states(self):
    states = {}
    for task in self.iter_query():
      status, shard_id = task.status, task.assignedTask.task.shardId
      first_timestamp = task.taskEvents[0].timestamp
      if shard_id not in states or first_timestamp > states[shard_id][0]:
        states[shard_id] = (first_timestamp, status)
    return dict((shard_id, status[1]) for (shard_id, status) in states.items())

  def wait_until(self, predicate):
    """Given a predicate (from ScheduleStatus => Boolean), return once all tasks
       return true for that predicate."""
    poll_interval = self.MIN_POLL_INTERVAL
    while not all(predicate(state) for state in self.states().values()):
      time.sleep(poll_interval.as_(Time.SECONDS))
      poll_interval = min(self.MAX_POLL_INTERVAL, 2 * poll_interval)
