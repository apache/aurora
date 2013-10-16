import time

from twitter.common import log
from .health_check import ShardWatcherHealthCheck

from gen.twitter.aurora.ttypes import (
  Identity,
  ResponseCode,
  ScheduleStatus,
  TaskQuery,
)

class Shard(object):
  def __init__(self, birthday=None, finished=False):
    self.birthday = birthday
    self.finished = finished
    self.healthy = False

  def set_healthy(self, value):
    self.healthy = value
    self.finished = True

  def __str__(self):
    return ('[birthday=%s, healthy=%s, finished=%s]' % (self.birthday, self.healthy, self.finished))


class ShardWatcher(object):
  def __init__(self,
               scheduler,
               job_key,
               restart_threshold,
               watch_secs,
               health_check_interval_seconds,
               clock=time):

    self._scheduler = scheduler
    self._job_key = job_key
    self._restart_threshold = restart_threshold
    self._watch_secs = watch_secs
    self._health_check_interval_seconds = health_check_interval_seconds
    self._clock = clock

  def watch(self, shard_ids, health_check=None):
    """Watches a set of shards and detects failures based on a delegated health check.

    Arguments:
    shard_ids -- set of shards to watch.

    Returns a set of shards that are considered failed.
    """
    log.info('Watching shards: %s' % shard_ids)
    shard_ids = set(shard_ids)
    health_check = health_check or ShardWatcherHealthCheck()
    now = self._clock.time()
    expected_healthy_by = now + self._restart_threshold
    max_time = now + self._restart_threshold + self._watch_secs

    shard_states = {}

    def finished_shards():
      return dict((s_id, s) for s_id, s in shard_states.items() if s.finished)

    def set_shard_healthy(shard_id, now):
      if shard_id not in shard_states:
        shard_states[shard_id] = Shard(now)
      shard = shard_states.get(shard_id)
      if now > (shard.birthday + self._watch_secs):
        log.info('Shard %s has been up and healthy for at least %d seconds' % (
            shard_id, self._watch_secs))
        shard.set_healthy(True)

    def maybe_set_shard_unhealthy(shard_id, retriable):
      # A shard that was previously healthy and currently unhealthy has failed.
      if shard_id in shard_states:
        log.info('Shard %s is unhealthy' % shard_id)
        shard_states[shard_id].set_healthy(False)
      # If the restart threshold has expired or if the shard cannot be retried it is unhealthy.
      elif now > expected_healthy_by or not retriable:
        log.info('Shard %s was not reported healthy within %d seconds' % (
            shard_id, self._restart_threshold))
        shard_states[shard_id] = Shard(finished=True)

    while True:
      running_tasks = self._get_tasks_by_shard_id(shard_ids)
      now = self._clock.time()
      tasks_by_shard = dict((task.assignedTask.task.instanceIdDEPRECATED, task)
                            for task in running_tasks)
      for shard_id in shard_ids:
        if shard_id not in finished_shards():
          running_task = tasks_by_shard.get(shard_id)
          if running_task is not None:
            task_healthy, retriable = health_check.health(running_task)
            if task_healthy:
              set_shard_healthy(shard_id, now)
            else:
              maybe_set_shard_unhealthy(shard_id, retriable)
          else:
            # Set retriable=True since a shard should be retried if it has not been healthy.
            maybe_set_shard_unhealthy(shard_id, retriable=True)

      log.debug('Shards health: %s' % ['%s: %s' % val for val in shard_states.items()])

      # Return if all tasks are finished.
      if set(finished_shards().keys()) == shard_ids:
        return set([s_id for s_id, s in shard_states.items() if not s.healthy])

      # Return if time is up.
      if now > max_time:
        return set([s_id for s_id in shard_ids if s_id not in shard_states
                                             or not shard_states[s_id].healthy])

      self._clock.sleep(self._health_check_interval_seconds)

  def _get_tasks_by_shard_id(self, instance_ids):
    log.debug('Querying shard statuses.')
    query = TaskQuery()
    query.owner = Identity(role=self._job_key.role)
    query.environment = self._job_key.environment
    query.jobName = self._job_key.name
    query.statuses = set([ScheduleStatus.RUNNING])
    query.instanceIds = instance_ids
    resp = self._scheduler.getTasksStatus(query)

    tasks = []
    if resp.responseCode == ResponseCode.OK:
      tasks = resp.result.scheduleStatusResult.tasks

    log.debug('Response from scheduler: %s (message: %s)'
        % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
    return tasks
