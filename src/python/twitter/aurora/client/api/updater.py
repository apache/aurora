import collections

from twitter.common import log

from gen.twitter.aurora.ttypes import (
  JobKey,
  Response,
  ResponseCode,
  ShardUpdateResult,
  UpdateResult,
)
from .updater_util import FailureThreshold, UpdaterConfig
from .instance_watcher import InstanceWatcher
from .scheduler_client import SchedulerProxy


class Updater(object):
  """Update the instances of a job in batches."""
  UPDATE_FAILURE_WARNING = """
Note: if the scheduler detects that an update is in progress (or was not
properly completed) it will reject subsequent updates.  This is because your
job is likely in a partially-updated state.  You should only begin another
update if you are confident that no other team members are updating this
job, and that the job is in a state suitable for an update.

After checking on the above, you may release the update lock on the job by
invoking cancel_update.
"""
  class Error(Exception): pass
  class InvalidConfigError(Error): pass

  def __init__(self, config, health_check_interval_seconds, scheduler=None, instance_watcher=None):
    self._config = config
    self._job_key = JobKey(role=config.role(), environment=config.environment(), name=config.name())
    self._health_check_interval_seconds = health_check_interval_seconds
    self._scheduler = scheduler or SchedulerProxy(config.cluster())
    try:
      self._update_config = UpdaterConfig(**config.update_config().get())
    except ValueError as e:
      raise self.InvalidConfigError(str(e))
    self._update_token = None
    self._instance_watcher = instance_watcher or InstanceWatcher(
        self._scheduler,
        self._job_key,
        self._update_config.restart_threshold,
        self._update_config.watch_secs,
        self._health_check_interval_seconds)

  def _start(self):
    """Start an update.

       Returns:
         Response instance from the scheduler call.
    """
    resp = self._scheduler.startUpdate(self._config.job())
    if resp.responseCode == ResponseCode.OK and resp.result.startUpdateResult.rollingUpdateRequired:
      self._update_token = resp.result.startUpdateResult.updateToken
    return resp

  def _finish(self, rollback=False):
    """Finish an update.  If you seek a rollback on finish, set rollback=True.

       Returns:
         Response instance from the scheduler call.
    """
    resp = self._scheduler.finishUpdate(self._job_key,
        UpdateResult.FAILED if rollback else UpdateResult.SUCCESS,
        self._update_token)

    if resp.responseCode == ResponseCode.OK:
      resp._update_token = None
    return resp

  def _get_instances_to_watch(self, instance_states, batch_instances):
    if instance_states:
      no_watch_states = (ShardUpdateResult.UNCHANGED, )
      watch_states = (ShardUpdateResult.RESTARTING, ShardUpdateResult.ADDED)
      unchanged_instances = [instance for (instance, state) in instance_states.items() if state in no_watch_states]
      if unchanged_instances:
        log.info('Not watching unchanged instances %s' % unchanged_instances)
      watch_instances = [instance for (instance, state) in instance_states.items() if state in watch_states]
    else:
      log.error('No instance actions returned by scheduler, assuming all instances restarted.')
      watch_instances = batch_instances
    return watch_instances

  def _update(self, initial_instances):
    """Drives execution of the update logic. Performs a batched update/rollback for all instances
    affected by the current update request.

    Arguments:
    initial_instances -- a list of instances to update.

    Returns the set of instances that failed to update.
    """
    failure_threshold = FailureThreshold(
        self._update_config.max_per_instance_failures,
        self._update_config.max_total_failures
    )
    failed_instances = set()
    InstanceState = collections.namedtuple('InstanceState', ['instance_id', 'is_updated'])
    remaining_instances = [InstanceState(instance_id, is_updated=False) for instance_id in initial_instances]

    log.info('Starting job update.')
    while remaining_instances and not failure_threshold.is_failed_update():
      batch_instances = remaining_instances[0 : self._update_config.batch_size]
      remaining_instances = list(set(remaining_instances) - set(batch_instances))
      instances_to_restart = [s.instance_id for s in batch_instances if s.is_updated]
      instances_to_update = [s.instance_id for s in batch_instances if not s.is_updated]

      instances_to_watch = []
      if instances_to_restart:
        self._restart_instances(instances_to_restart)
        instances_to_watch += instances_to_restart

      if instances_to_update:
        instance_states = self._update_instances(instances_to_update)
        instances_to_watch += self._get_instances_to_watch(instance_states, instances_to_update)

      failed_instances = self._instance_watcher.watch(instances_to_watch) if instances_to_watch else set()

      if failed_instances:
        log.error('Failed instances: %s' % failed_instances)
      remaining_instances += [InstanceState(instance_id, is_updated=True) for instance_id in failed_instances]
      remaining_instances.sort(key=lambda tup: tup.instance_id)
      failure_threshold.update_failure_counts(failed_instances)

    if failed_instances:
      untouched_instances = [s.instance_id for s in remaining_instances if not s.is_updated]
      instances_to_rollback = list(set(initial_instances) - set(untouched_instances))
      self._rollback(instances_to_rollback)

    return failed_instances

  def _rollback(self, instances_to_rollback):
    """Performs the job rollback.

    Arguments:
    instances_to_rollback -- instance ids.
    """
    log.info('Reverting update for %s' % instances_to_rollback)
    instances_to_rollback.sort()
    failed_instances = []
    while instances_to_rollback:
      batch_instances = instances_to_rollback[0 : self._update_config.batch_size]
      instances_to_rollback = list(set(instances_to_rollback) - set(batch_instances))

      resp = self._scheduler.rollbackShards(self._job_key, batch_instances, self._update_token)
      self._check_and_log_update_response(resp)
      instances = resp.result.rollbackShardsResult.shards
      instances_to_watch = self._get_instances_to_watch(instances, batch_instances)
      failed_instances += self._instance_watcher.watch(instances_to_watch)

    if failed_instances:
      log.error('Rollback failed for instances: %s' % failed_instances)

  def _update_instances(self, instance_ids):
    """Instructs the scheduler to update instances.

    Arguments:
    instance_ids -- set of instances to be updated by the scheduler.

    Returns a map of the current status of the updated instances as returned by the scheduler.
    """
    log.info('Updating instances: %s' % instance_ids)
    resp = self._scheduler.updateShards(self._job_key, instance_ids, self._update_token)
    self._check_and_log_update_response(resp)
    return resp.result.updateShardsResult.shards

  def _restart_instances(self, instance_ids):
    """Instructs the scheduler to restart instances.

    Arguments:
    instance_ids -- set of instances to be restarted by the scheduler.
    """
    log.info('Restarting instances: %s' % instance_ids)
    resp = self._scheduler.restartShards(self._job_key, instance_ids)
    self._check_and_log_response(resp)

  def update(self, instances=None):
    """Performs the job update, blocking until it completes.
    A rollback will be performed if the update was considered a failure based on the
    update configuration.

    Arguments:
    instances -- (optional) instances to update. If not specified, all instances will be updated.

    Returns a response object with update result status.
    """
    resp = self._start()
    update_resp = Response()
    update_resp.responseCode = resp.responseCode
    update_resp.message = resp.message

    if resp.responseCode != ResponseCode.OK:
      log.error("Error starting update: %s" % resp.message)
      log.error(self.UPDATE_FAILURE_WARNING)
      return update_resp
    elif not resp.result.startUpdateResult.rollingUpdateRequired:
      log.info('Update successful: %s' % resp.message)
      return update_resp

    failed_instances = self._update(instances or list(range(self._config.instances())))

    if failed_instances:
      log.error('Update reverted, failures detected on instances %s' % failed_instances)
    else:
      log.info('Update successful')

    resp = self._finish(failed_instances)
    if resp.responseCode != ResponseCode.OK:
      log.error('There was an error finalizing the update: %s' % resp.message)

    return resp

  # TODO(maximk): Re-evaluate to see if it makes sense as an instance method.
  @classmethod
  def cancel_update(cls, scheduler, job_key, token=None):
    return scheduler.finishUpdate(job_key.to_thrift(), UpdateResult.TERMINATE, token)

  @classmethod
  def _handle_unexpected_response(cls, name, message):
    e = cls.Error('Unexpected response from scheduler: %s (message: %s)' % (name, message))
    log.error(e)
    log.error('Aborting update without rollback!!!')
    raise e

  @classmethod
  def _check_and_log_response(cls, resp):
    name, message = ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message
    if resp.responseCode == ResponseCode.OK:
      log.debug('Response from scheduler: %s (message: %s)' % (name, message))
    else:
      cls._handle_unexpected_response(name, message)

  @classmethod
  def _check_and_log_update_response(cls, resp):
    name, message = ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message
    if resp.responseCode == ResponseCode.OK:
      log.debug('Response from scheduler: %s (message: %s)' % (name, message))
    else:
      cls._handle_unexpected_response(name, message)
