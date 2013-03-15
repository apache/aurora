import collections
import time

from twitter.common import app, log

from gen.twitter.mesos.constants import DEFAULT_ENVIRONMENT
from gen.twitter.mesos.ttypes import (
  Identity,
  JobKey,
  ResponseCode,
  ScheduleStatus,
  ShardUpdateResult,
  TaskQuery,
  UpdateResponseCode,
  UpdateResult,
)

from .scheduler_client import SchedulerProxy


app.add_option('--mesos_updater_status_check_interval',
  dest='mesos_updater_status_check_interval',
  default=3,
  type='int',
  help='How often Mesos runs update loop.')


def debug_if(condition):
  return log.DEBUG if condition else log.ERROR


class Shard(object):
  def __init__(self, task_id=None, birthday=None, finished=False):
    self.task_id = task_id
    self.birthday = birthday
    self.finished = finished
    self.healthy = False

  def set_healthy(self, healthy):
    self.healthy = healthy
    self.finished = True

  def __str__(self):
    return ('[id=%s, birthday=%s, healthy=%s, finished=%s'
            % (self.task_id, self.birthday, self.healthy, self.finished))


class UpdaterConfig(object):
  def __init__(self, batch_size, restart_threshold, watch_secs, max_per_shard_failures,
               max_total_failures):
    if batch_size <= 0:
      raise ValueError('Batch size should be greater than 0')
    if restart_threshold <= 0:
      raise ValueError('Restart Threshold should be greater than 0')
    if watch_secs <= 0:
      raise ValueError('Watch seconds should be greater than 0')
    self.batch_size = batch_size
    self.restart_threshold = restart_threshold
    self.watch_secs = watch_secs
    self.max_total_failures = max_total_failures
    self.max_per_shard_failures = max_per_shard_failures


class Updater(object):
  """Update the shards of a job in batches."""

  class Error(Exception): pass
  class InvalidConfigError(Error): pass
  class UpdateInProgressError(Error): pass

  def __init__(self, config, scheduler=None, clock=time):
    self._role = config.role()
    self._job_name = config.name()
    self._scheduler = scheduler or SchedulerProxy(config.cluster())
    self._clock = clock
    self._failures_by_shard = collections.defaultdict(int)
    self._config = config
    try:
      self._update_config = UpdaterConfig(**config.update_config().get())
    except ValueError as e:
      raise self.InvalidConfigError(str(e))
    self._update_token = None

    # TODO(ksweeney): Get this from config and remove job_name and role fields.
    self._job = JobKey(name=self._job_name, environment=DEFAULT_ENVIRONMENT, role=self._role)

  def update_failure_counts(self, failed_shards):
    """Update the failure counts metrics based upon a batch of failed shards."""
    for shard in failed_shards:
      self._failures_by_shard[shard] += 1

  def exceeded_shard_fail_count(self):
    """Checks if the per shard failure is greater than a threshold."""
    return sum(count > self._update_config.max_per_shard_failures
               for count in self._failures_by_shard.values())

  def start(self):
    """Start an update.
    
       Returns:
         True if an update is necessary
         False if an update is not necessary
         
       UpdateInProgressError exception is thrown if an update is already in progress.
    """
    resp = self._scheduler.startUpdate(self._config.job())
    if resp.responseCode == ResponseCode.OK and resp.rollingUpdateRequired:
      self._update_token = resp.updateToken
    return resp

  def finish(self, rollback=False):
    """Finish an update.  If you seek a rollback on finish, set rollback=True.
    
       Returns:
         True if update succeeded
         False if update failed
    """

    # TODO(ksweeney): Change to use just job after JobKey refactor.
    resp = self._scheduler.finishUpdate(
        self._role, self._job_name, self._job,
        UpdateResult.FAILED if rollback else UpdateResult.SUCCESS, self._update_token)
    if resp.responseCode == ResponseCode.OK:
      resp._update_token = None
    return resp
  
  def cancel(self):
    return self.cancel_update(self._scheduler, self._role, self._job_name, self._update_token)

  @classmethod
  def cancel_update(cls, scheduler, role, jobname, token=None):
    job = JobKey(role=role, environment=DEFAULT_ENVIRONMENT, name=jobname)

    # TODO(ksweeney): Change to use just job after JobKey refactor.
    return scheduler.finishUpdate(role, jobname, job, UpdateResult.TERMINATE, token)

  def is_failed_update(self):
    total_failed_shards = self.exceeded_shard_fail_count()
    is_failed = total_failed_shards > self._update_config.max_total_failures

    if is_failed:
      log.error('%s failed shards observed, maximum allowed is %s' % (total_failed_shards,
          self._update_config.max_total_failures))
      for shard, failure_count in self._failures_by_shard.items():
        if failure_count > self._update_config.max_per_shard_failures:
          log.error('%s shard failures for shard %s, maximum allowed is %s' %
              (failure_count, shard, self._update_config.max_per_shard_failures))
    return is_failed

  def _maybe_watch_shards(self, shard_states, batch_shards):
    if shard_states:
      no_watch_states = (ShardUpdateResult.UNCHANGED, )
      watch_states = (ShardUpdateResult.RESTARTING, ShardUpdateResult.ADDED)
      unchanged_shards = dict(filter(lambda i: i[1] in no_watch_states, shard_states.iteritems()))
      if unchanged_shards:
        log.info('Not watching unchanged shards %s' % unchanged_shards.keys())
      watch_shards = dict(filter(lambda i: i[1] in watch_states, shard_states.iteritems()))
    else:
      log.error('No shard actions returned by scheduler, assuming all shards restarted.')
      watch_shards = batch_shards
    if watch_shards:
      return self.watch_shards(watch_shards)
    else:
      return []

  def update(self, initial_shards):
    """Performs the job update, blocking until it completes.
    A rollback will be performed if the update was considered a failure based on the
    update configuration.

    Arguments:
    update_config -- job update configuration object.
    initial_shards -- a list of shards to update.

    Returns the set of shards that failed to update.
    """
    failed_shards = []
    remaining_shards = initial_shards[:]

    log.info('Starting job update.')
    while remaining_shards and not self.is_failed_update():
      batch_shards = remaining_shards[0 : self._update_config.batch_size]
      remaining_shards = list(set(remaining_shards) - set(batch_shards))
      resp = self.restart_shards(batch_shards)
      log.log(debug_if(resp.responseCode == UpdateResponseCode.OK),
        'Response from scheduler: %s (message: %s)' % (
          UpdateResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
      failed_shards = self._maybe_watch_shards(resp.shards, batch_shards)
      log.log(debug_if(not failed_shards), 'Failed shards: %s' % failed_shards)
      remaining_shards += failed_shards
      remaining_shards.sort()
      self.update_failure_counts(failed_shards)

    if failed_shards:
      shards_to_rollback = [shard for shard in set(initial_shards).difference(remaining_shards)] + (
          failed_shards)
      self.rollback(shards_to_rollback)

    return failed_shards

  def rollback(self, shards_to_rollback):
    """Performs the job rollback.

    Arguments:
    update_config -- update configuration object that describes how the rollback is performed.
    shards_to_rollback -- shard ids.
    """
    log.info('Reverting update for %s' % shards_to_rollback)
    shards_to_rollback.sort()
    failed_shards = []
    while shards_to_rollback:
      batch_shards = shards_to_rollback[0 : self._update_config.batch_size]
      shards_to_rollback = list(set(shards_to_rollback) - set(batch_shards))

      # TODO(ksweeney): Change to use just job after JobKey refactor.
      resp = self._scheduler.rollbackShards(self._role, self._job_name, self._job, batch_shards,
          self._update_token)
      log.log(debug_if(resp.responseCode == UpdateResponseCode.OK),
        'Response from scheduler: %s (message: %s)'
          % (UpdateResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
      failed_shards += self._maybe_watch_shards(resp.shards, batch_shards)

    if failed_shards:
      log.error('Rollback failed for shards: %s' % failed_shards)

  def restart_shards(self, shard_ids):
    """Performs a scheduler call for restart.

    Arguments:
    shard_ids -- set of shards to be restarted by the scheduler.

    Returns a map of the current status of the restarted shards as returned by the scheduler.
    """
    log.info('Restarting shards: %s' % shard_ids)

    # TODO(ksweeney): Change to use just job after JobKey refactor.
    return self._scheduler.updateShards(self._role, self._job_name, self._job, shard_ids,
        self._update_token)

  def watch_shards(self, shard_ids):
    """Monitors the restarted shards.

    Arguments:
    shard_ids -- set of shards to watch.

    Returns a set of tasks that failed to meet the following criteria,
    1. Failed to move to RUNNING state before restart_threshold from the time of restart.
    2. Failed to stay in the RUNNING state before watch_secs expire.
    """
    log.info('Watching shards for %s seconds: %s' % (self._update_config.watch_secs, shard_ids))
    shard_ids = set(shard_ids)
    start_time = self._clock.time()
    expected_running_by = start_time + self._update_config.restart_threshold

    shards_health = {}
    def shards(finished):
      return dict([(s_id, s) for (s_id, s) in shards_health.items() if s.finished == finished])

    while True:
      log.debug('Querying shard statuses.')
      query = TaskQuery()
      query.owner = Identity(role=self._role)
      query.jobName = self._job_name
      query.statuses = set([ScheduleStatus.RUNNING])
      query.shardIds = shard_ids
      resp = self._scheduler.getTasksStatus(query)
      resp.tasks = resp.tasks or []
      log.debug('Response from scheduler: %s (message: %s)'
          % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))

      now = self._clock.time()
      for task in resp.tasks:
        task_id = task.assignedTask.taskId
        shard_id = task.assignedTask.task.shardId
        status = task.status
        if shard_id in shards_health:
          shard = shards_health[shard_id]
          if not shard.finished and status != ScheduleStatus.RUNNING or task_id != shard.task_id:
            log.error('Detected failure in shard %s' % shard_id)
            log.debug('  task id %s' % task_id)
            shard.set_healthy(False)
        elif status == ScheduleStatus.RUNNING:
          log.debug('Detected RUNNING shard %s' % shard_id)
          log.debug('  task id %s' % task_id)
          shards_health[shard_id] = Shard(task_id, now)
      shards_returned = [task.assignedTask.task.shardId for task in resp.tasks]
      for shard_id in set(shards(finished=False)) - set(shards_returned):
        log.error('Shard %s is no longer RUNNING' % shard_id)
        shards_health[shard_id].set_healthy(False)

      if now > expected_running_by:
        for shard_id in shard_ids - set(shards_health):
          shards_health[shard_id] = Shard(finished=True)
          log.error('Shard %s did not transition to RUNNING' % shard_id)

      for shard_id, shard in shards(finished=False).items():
        if now > (shard.birthday + self._update_config.watch_secs):
          shard.set_healthy(True)
          log.info('Shard %s is up and healthy' % shard_id)

      log.debug('Shards health: {%s}' % ['%s: %s' % val for val in shards_health.items()])

      # Return if all tasks are finished.
      if set(shards(finished=True)) == shard_ids:
        return [s_id for (s_id, s) in shards_health.items() if not s.healthy]
      # Return if time is up.
      if now > (start_time + self._update_config.restart_threshold +
          self._update_config.watch_secs):
        return [s_id for s_id in shard_ids if s_id not in shards_health
                                           or not shards_health[s_id].healthy]
      self._clock.sleep(app.get_options().mesos_updater_status_check_interval)
