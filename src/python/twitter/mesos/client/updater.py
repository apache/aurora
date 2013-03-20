import collections

from twitter.common import app, log

from gen.twitter.mesos.constants import DEFAULT_ENVIRONMENT
from gen.twitter.mesos.ttypes import (
  JobKey,
  ResponseCode,
  ShardUpdateResult,
  UpdateResponseCode,
  UpdateResult,
)

from .shard_watcher import ShardWatcher
from .scheduler_client import SchedulerProxy


def debug_if(condition):
  return log.DEBUG if condition else log.ERROR


class UpdaterConfig(object):
  """
  For updates involving a health check,

  UPDATE SHARD          RUNNING                    HEALTHY           RUNNING and HEALTHY
  ------------------------|--------------------------|-----------------------|
  \----------------------/ \------------------------/ \----------------------/
     restart_thresold           healthy_threshold            watch_secs

  For updates without a health check,

  UPDATE SHARD          RUNNING               REMAINS RUNNING
  ------------------------|--------------------------|
  \----------------------/ \------------------------/
     restart_thresold            watch_secs

  When an update is initiated, a shard is expected to move into RUNNING before restart_threshold.
  Once RUNNING, if health checks are enabled, the health checker requires the shard to be in a 
  "healthy" state within healthy_threshold. The shard is then expected to remain RUNNING and report
  "healthy", if health checks are enabled, for atleast watch_secs. If any of these conditions are 
  not satisfied, the shard is deemed unhealthy.
  """
  def __init__(self, batch_size, restart_threshold, healthy_threshold, watch_secs,
               max_per_shard_failures, max_total_failures):
    if batch_size <= 0:
      raise ValueError('Batch size should be greater than 0')
    if restart_threshold <= 0:
      raise ValueError('Restart Threshold should be greater than 0')
    if watch_secs <= 0:
      raise ValueError('Watch seconds should be greater than 0')
    if healthy_threshold <= 0:
      raise ValueError('Healthy threshold should be greater than 0')
    self.batch_size = batch_size
    self.restart_threshold = restart_threshold
    self.healthy_threshold = healthy_threshold
    self.watch_secs = watch_secs
    self.max_total_failures = max_total_failures
    self.max_per_shard_failures = max_per_shard_failures


class Updater(object):
  """Update the shards of a job in batches."""

  class Error(Exception): pass
  class InvalidConfigError(Error): pass
  class UpdateInProgressError(Error): pass

  def __init__(self, config, scheduler=None, shard_watcher=None):
    self._config = config
    self._role = config.role()
    self._job_name = config.name()
    self._cluster = config.cluster()
    self._scheduler = scheduler or SchedulerProxy(self._cluster)
    self._failures_by_shard = collections.defaultdict(int)
    try:
      self._update_config = UpdaterConfig(**config.update_config().get())
    except ValueError as e:
      raise self.InvalidConfigError(str(e))
    self._update_token = None

    # TODO(ksweeney): Get this from config and remove job_name and role fields.
    self._job = JobKey(name=self._job_name, environment=DEFAULT_ENVIRONMENT, role=self._role)
    if shard_watcher:
      self._shard_watcher = shard_watcher
    else:
      self._shard_watcher = ShardWatcher(
          scheduler, self._job, self._cluster, self._update_config.restart_threshold,
          self._update_config.healthy_threshold, self._update_config.watch_secs,
          config.has_health_port())

  def _update_failure_counts(self, failed_shards):
    """Update the failure counts metrics based upon a batch of failed shards."""
    for shard in failed_shards:
      self._failures_by_shard[shard] += 1

  def _exceeded_shard_fail_count(self):
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

  def _is_failed_update(self):
    total_failed_shards = self._exceeded_shard_fail_count()
    is_failed = total_failed_shards > self._update_config.max_total_failures

    if is_failed:
      log.error('%s failed shards observed, maximum allowed is %s' % (total_failed_shards,
          self._update_config.max_total_failures))
      for shard, failure_count in self._failures_by_shard.items():
        if failure_count > self._update_config.max_per_shard_failures:
          log.error('%s shard failures for shard %s, maximum allowed is %s' %
              (failure_count, shard, self._update_config.max_per_shard_failures))
    return is_failed

  def _get_shards_to_watch(self, shard_states, batch_shards):
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
    return watch_shards

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
    ShardState = collections.namedtuple('ShardState', ['shard_id', 'is_updated'])
    remaining_shards = [ShardState(shard_id, is_updated=False) for shard_id in initial_shards]

    log.info('Starting job update.')
    while remaining_shards and not self._is_failed_update():
      batch_shards = remaining_shards[0 : self._update_config.batch_size]
      remaining_shards = list(set(remaining_shards) - set(batch_shards))
      shards_to_restart = [s.shard_id for s in batch_shards if s.is_updated]
      shards_to_update = [s.shard_id for s in batch_shards if not s.is_updated]

      shards_to_watch = []
      if shards_to_restart:
        self._restart_shards(shards_to_restart)
        shards_to_watch += shards_to_restart

      if shards_to_update:
        shard_states = self._update_shards(shards_to_update)
        shards_to_watch += self._get_shards_to_watch(shard_states, shards_to_update)

      failed_shards = self._shard_watcher.watch(shards_to_watch) if shards_to_watch else []

      log.log(debug_if(not failed_shards), 'Failed shards: %s' % failed_shards)
      remaining_shards += [ShardState(shard_id, is_updated=True) for shard_id in failed_shards]
      remaining_shards.sort(key=lambda tup: tup.shard_id)
      self._update_failure_counts(failed_shards)

    if failed_shards:
      untouched_shards = [s.shard_id for s in remaining_shards if not s.is_updated]
      shards_to_rollback = list(set(initial_shards) - set(untouched_shards))
      self._rollback(shards_to_rollback)

    return failed_shards

  def _rollback(self, shards_to_rollback):
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
      shards_to_watch = self._get_shards_to_watch(resp.shards, batch_shards)
      failed_shards += self._shard_watcher.watch(shards_to_watch)

    if failed_shards:
      log.error('Rollback failed for shards: %s' % failed_shards)

  def _update_shards(self, shard_ids):
    """Instructs the scheduler to update shards.

    Arguments:
    shard_ids -- set of shards to be updated by the scheduler.

    Returns a map of the current status of the updated shards as returned by the scheduler.
    """
    log.info('Updating shards: %s' % shard_ids)
    # TODO(ksweeney): Change to use just job after JobKey refactor.
    resp = self._scheduler.updateShards(
        self._role, self._job_name, self._job, shard_ids, self._update_token)
    log.log(debug_if(resp.responseCode == UpdateResponseCode.OK),
        'Response from scheduler: %s (message: %s)' % (
          UpdateResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
    return resp.shards

  def _restart_shards(self, shard_ids):
    """Instructs the scheduler to restart shards.

    Arguments:
    shard_ids -- set of shards to be restarted by the scheduler.
    """
    log.info('Restarting shards: %s' % shard_ids)
    resp = self._scheduler.restartShards(self._role, self._job_name, self._job, shard_ids)
    log.log(debug_if(resp.responseCode == ResponseCode.OK),
        'Response from scheduler: %s (message: %s)' % (
          ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
