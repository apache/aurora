from abc import abstractmethod

from twitter.common import log
from twitter.common.lang import Interface
from twitter.mesos.common.http_signaler import HttpSignaler

from gen.twitter.mesos.ttypes import ScheduleStatus


class HealthCheck(Interface):
  @abstractmethod
  def health(self, task):
    """Checks health of the task"""


class StatusHealthCheck(HealthCheck):
  """Verifies the health of a task based on the task status. A task is healthy iff,
    1. A task is in state RUNNING
    2. A task that satisfies (1) and is already known has the same task id.
  """
  def __init__(self):
    self._task_ids = {}

  def health(self, task):
    task_id = task.assignedTask.taskId
    shard_id = task.assignedTask.task.shardId
    status = task.status

    if status == ScheduleStatus.RUNNING:
      if shard_id in self._task_ids:
        return task_id == self._task_ids.get(shard_id)
      else:
        log.info('Detected RUNNING shard %s' % shard_id)
        self._task_ids[shard_id] = task_id
        return True
    else:
      return False


class HttpHealthCheck(HealthCheck):
  """Verifies the health of a task based on http health checks. A new http signaler is created for a
  task iff,
    1. The shard id of the task is unknown.
    2. The shard id is known but the (host, port) is different for the task.
  """
  def __init__(self, http_signaler_factory=HttpSignaler):
    self._http_signalers = {}
    self._http_signaler_factory = http_signaler_factory

  def health(self, task):
    assigned_task = task.assignedTask
    shard_id = assigned_task.task.shardId
    host_port = (assigned_task.slaveHost, assigned_task.assignedPorts['health'])
    http_signaler = None
    if shard_id in self._http_signalers:
      checker_host_port, signaler = self._http_signalers.get(shard_id)
      # Only reuse the health checker if it is for the same destination.
      if checker_host_port == host_port:
        http_signaler = signaler
    if not http_signaler:
      http_signaler = self._http_signaler_factory(host_port[1], host_port[0])
      self._http_signalers[shard_id] = (host_port, http_signaler)
    return http_signaler.health()[0]


class ChainedHealthCheck(HealthCheck):
  """Delegates health checks to configured health checkers."""
  def __init__(self, *health_checkers):
    self._health_checkers = health_checkers

  def health(self, task):
    return all(checker.health(task) for checker in self._health_checkers)


class HealthCheckFactory(object):
  def __init__(self, cluster, has_health_port):
    self._enable_http = has_health_port

  def get(self):
    check = StatusHealthCheck()
    if self._enable_http:
      check = ChainedHealthCheck(check, HttpHealthCheck())
    return check
