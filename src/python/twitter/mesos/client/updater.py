import collections

from twitter.common import log

from gen.twitter.mesos.constants import DEFAULT_ENVIRONMENT
from gen.twitter.mesos.ttypes import (
  JobKey,
  ResponseCode,
  ShardUpdateResult,
  UpdateResponseCode,
  UpdateResult,
)
from .updater_util import FailureThreshold, UpdaterConfig
from .shard_watcher import ShardWatcher
from .scheduler_client import SchedulerProxy


def debug_if(condition):
  return log.DEBUG if condition else log.ERROR


class Updater(object):
  """Update the shards of a job in batches."""

  class Error(Exception): pass
  class InvalidConfigError(Error): pass
  class UpdateInProgressError(Error): pass

  def __init__(self, config, scheduler=None):
    self._config = config
    self._job_key = JobKey(role=config.role(), environment=config.environment(), name=config.name())
    self._scheduler = scheduler or SchedulerProxy(config.cluster())
    try:
      self._update_config = UpdaterConfig(**config.update_config().get())
    except ValueError as e:
      raise self.InvalidConfigError(str(e))
    self._update_token = None

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

    resp = self._scheduler.finishUpdate(self._job_key,
        UpdateResult.FAILED if rollback else UpdateResult.SUCCESS,
        self._update_token)

    if resp.responseCode == ResponseCode.OK:
      resp._update_token = None
    return resp

  def cancel(self):
    return self.cancel_update(self._scheduler, self._job_key.role, self._job_key.environment,
        self._job_key.jobname, self._update_token)

  @classmethod
  def cancel_update(cls, scheduler, role, env, jobname, token=None):
    job_key = JobKey(role=role, environment=env, name=jobname)

    return scheduler.finishUpdate(job_key, UpdateResult.TERMINATE, token)

  def _get_shards_to_watch(self, shard_states, batch_shards):
    if shard_states:
      no_watch_states = (ShardUpdateResult.UNCHANGED, )
      watch_states = (ShardUpdateResult.RESTARTING, ShardUpdateResult.ADDED)
      unchanged_shards = [shard for (shard, state) in shard_states.items() if state in no_watch_states]
      if unchanged_shards:
        log.info('Not watching unchanged shards %s' % unchanged_shards)
      watch_shards = [shard for (shard, state) in shard_states.items() if state in watch_states]
    else:
      log.error('No shard actions returned by scheduler, assuming all shards restarted.')
      watch_shards = batch_shards
    return watch_shards

  def update(self, initial_shards, health_check_interval_seconds, shard_watcher=None):
    """Performs the job update, blocking until it completes.
    A rollback will be performed if the update was considered a failure based on the
    update configuration.

    Arguments:
    initial_shards -- a list of shards to update.
    health_check_interval_seconds -- Time to wait between consecutive status checks.

    Returns the set of shards that failed to update.
    """
    self._shard_watcher = shard_watcher or ShardWatcher(
        self._scheduler,
        self._job_key,
        self._update_config.restart_threshold,
        self._update_config.watch_secs,
        health_check_interval_seconds)

    failure_threshold = FailureThreshold(
        self._update_config.max_per_shard_failures,
        self._update_config.max_total_failures
    )
    failed_shards = set()
    ShardState = collections.namedtuple('ShardState', ['shard_id', 'is_updated'])
    remaining_shards = [ShardState(shard_id, is_updated=False) for shard_id in initial_shards]

    log.info('Starting job update.')
    while remaining_shards and not failure_threshold.is_failed_update():
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

      failed_shards = self._shard_watcher.watch(shards_to_watch) if shards_to_watch else set()

      log.log(debug_if(not failed_shards), 'Failed shards: %s' % failed_shards)
      remaining_shards += [ShardState(shard_id, is_updated=True) for shard_id in failed_shards]
      remaining_shards.sort(key=lambda tup: tup.shard_id)
      failure_threshold.update_failure_counts(failed_shards)

    if failed_shards:
      untouched_shards = [s.shard_id for s in remaining_shards if not s.is_updated]
      shards_to_rollback = list(set(initial_shards) - set(untouched_shards))
      self._rollback(shards_to_rollback)

    return failed_shards

  def _rollback(self, shards_to_rollback):
    """Performs the job rollback.

    Arguments:
    shards_to_rollback -- shard ids.
    """
    log.info('Reverting update for %s' % shards_to_rollback)
    shards_to_rollback.sort()
    failed_shards = []
    while shards_to_rollback:
      batch_shards = shards_to_rollback[0 : self._update_config.batch_size]
      shards_to_rollback = list(set(shards_to_rollback) - set(batch_shards))

      resp = self._scheduler.rollbackShards(self._job_key, batch_shards, self._update_token)
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
    resp = self._scheduler.updateShards(self._job_key, shard_ids, self._update_token)
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
    resp = self._scheduler.restartShards(self._job_key, shard_ids)
    log.log(debug_if(resp.responseCode == ResponseCode.OK),
        'Response from scheduler: %s (message: %s)' % (
          ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
