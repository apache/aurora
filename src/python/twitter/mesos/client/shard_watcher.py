import time

from twitter.common import app, log
from twitter.mesos.common.http_signaler import HttpSignaler as HealthChecker

from gen.twitter.mesos.ttypes import (
  Identity,
  ResponseCode,
  ScheduleStatus,
  TaskQuery,
)

app.add_option('--aurora_updater_status_check_interval',
  dest='aurora_updater_status_check_interval',
  default=3,
  type='int',
  help='How often Mesos runs the update loop.')


class Shard(object):
  def __init__(self, task_id=None, birthday=None, health_checker=None, finished=False):
    self.task_id = task_id
    self.birthday = birthday
    self.health_checker = health_checker
    self.healthy_timestamp = None
    self.finished = finished
    self.healthy = False

  def set_healthy(self, value):
    self.healthy = value
    self.finished = True

  def __str__(self):
    return ('[id=%s, birthday=%s, healthy=%s, healthy_timestamp=%s, finished=%s]'
            % (self.task_id, self.birthday, self.healthy, self.healthy_timestamp, self.finished))


class ShardWatcher(object):
  def __init__(self, scheduler, job, cluster, restart_threshold, healthy_threshold, watch_secs,
               health_checker_enabled, clock=time):
    self._scheduler = scheduler
    self._job_name = job.name
    self._role = job.role
    self._job = job
    self._restart_threshold = restart_threshold
    self._healthy_threshold = healthy_threshold
    self._watch_secs = watch_secs
    self._health_checker_enabled = health_checker_enabled
    self._clock = clock
    self._cluster = cluster
    self._is_proxy_setup = False

  def maybe_setup_proxy(self, cluster):
    if self._health_checker_enabled and not self._is_proxy_setup:
      self._is_proxy_setup = True
      HealthChecker.maybe_setup_proxy(cluster)

  def watch(self, shard_ids):
    """Monitors a set of shards.

    Arguments:
    shard_ids -- set of shards to watch.

    Returns a set of shards that meet the following criteria,
    1. Failed to move to RUNNING state before restart_threshold from the time of restart.
    2. Failed to respond to a health check before healthy_threshold after moving into RUNNING.
    3. Failed to respond to a health check or failed to stay in the RUNNING state before watch_secs.
    """
    self.maybe_setup_proxy(self._cluster)
    log.info('Watching shards: %s' % shard_ids)
    shard_ids = set(shard_ids)
    start_time = self._clock.time()
    expected_running_by = start_time + self._restart_threshold
    max_time = start_time + self._restart_threshold + self._watch_secs
    if self._health_checker_enabled:
      max_time += self._healthy_threshold

    shards_info = {}
    def shards(finished):
      return dict((s_id, s) for (s_id, s) in shards_info.items() if s.finished == finished)

    while True:
      tasks = self._get_tasks_by_shard_id(shard_ids)
      now = self._clock.time()
      shards_info = self._update_shards_info(shards_info, tasks, now)

      # Shards not reported when queried for status are unhealthy.
      shards_returned = [task.assignedTask.task.shardId for task in tasks]
      for shard_id in set(shards(finished=False)) - set(shards_returned):
        log.error('Shard %s is no longer RUNNING' % shard_id)
        shards_info[shard_id].set_healthy(False)

      # Shards that do not transition to RUNNING before a restart threshold are unhealthy.
      if now > expected_running_by:
        for shard_id in (shard_ids - set(shards_info)):
          shards_info[shard_id] = Shard(finished=True)
          log.error('Shard %s did not transition to RUNNING' % shard_id)

      if self._health_checker_enabled:
        unfinished_shards = self._update_health_info(shards(finished=False), now) 
        shards_info.update(unfinished_shards)
      else:
        for shard_id, shard in shards(finished=False).items():
          if now > (shard.birthday + self._watch_secs):
            shard.set_healthy(True)
            log.info('Shard %s is up and healthy' % shard_id)

      log.debug('Shards health: {%s}' % ['%s: %s' % val for val in shards_info.items()])

      # Return if all tasks are finished.
      if set(shards(finished=True)) == shard_ids:
        return [s_id for (s_id, s) in shards_info.items() if not s.healthy]

      # Return if time is up.
      if now > max_time:
        return [s_id for s_id in shard_ids if s_id not in shards_info 
                                             or not shards_info[s_id].healthy]

      self._clock.sleep(app.get_options().aurora_updater_status_check_interval)

  def _get_tasks_by_shard_id(self, shard_ids):
    log.debug('Querying shard statuses.')
    # TODO(ksweeney): Change to use just job after JobKey refactor.
    query = TaskQuery()
    query.owner = Identity(role=self._role)
    query.jobName = self._job_name
    query.statuses = set([ScheduleStatus.RUNNING])
    query.shardIds = shard_ids
    resp = self._scheduler.getTasksStatus(query)
    resp.tasks = resp.tasks or []
    log.debug('Response from scheduler: %s (message: %s)'
        % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
    return resp.tasks

  def _update_shards_info(self, shards_info, tasks, now):
    for task in tasks:
      assigned_task = task.assignedTask
      task_id = assigned_task.taskId
      shard_id = assigned_task.task.shardId
      status = task.status

      if shard_id in shards_info:
        shard = shards_info[shard_id]
        if not shard.finished and status != ScheduleStatus.RUNNING or task_id != shard.task_id:
          log.error('Detected failure in shard %s' % shard_id)
          log.error('  task id %s' % task_id)
          shard.set_healthy(False)
      elif status == ScheduleStatus.RUNNING:
        log.debug('Detected RUNNING shard %s' % shard_id)
        log.debug('  task id %s' % task_id)
        if self._health_checker_enabled:
          health_port = assigned_task.assignedPorts['health']
          health_checker = self.create_health_checker(assigned_task.slaveHost, health_port)
        else:
          health_checker = None
        shards_info[shard_id] = Shard(task_id, now, health_checker)
    return shards_info

  def create_health_checker(self, host, port):
    return HealthChecker(port, host)

  def _update_health_info(self, shards_info, now):
    for shard_id, shard in shards_info.items():
      # Shards that do not respond to health checks before the healthy threhsold are unhealthy.
      if shard.healthy_timestamp is None and now > (shard.birthday + self._healthy_threshold):
        shard.set_healthy(False)
        log.info('Shard %s did not respond "OK" to health checks' % shard_id)

      is_healthy, reason = shard.health_checker.health()
      if is_healthy:
        if shard.healthy_timestamp is None:
          log.info('Shard %s responded ok to health check' % shard_id)
          shard.healthy_timestamp = now
        if now > (shard.healthy_timestamp + self._watch_secs):
          shard.set_healthy(True)
          log.info('Shard %s is up and healthy' % shard_id)
      else:
        log.debug('Shard %s did not respond ok to health check. Reason: %s' % (shard_id, reason))
        shard.healthy_timestamp = None
    return shards_info
