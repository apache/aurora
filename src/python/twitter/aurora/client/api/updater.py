from collections import namedtuple
from difflib import unified_diff

from twitter.common import log

from gen.twitter.aurora.constants import ACTIVE_STATES
from gen.twitter.aurora.ttypes import (
    AddInstancesConfig,
    JobConfigValidation,
    JobKey,
    Identity,
    Lock,
    LockKey,
    LockValidation,
    Response,
    ResponseCode,
    ShardUpdateResult,
    TaskQuery,
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
  class InvalidStateError(Error): pass

  InstanceState = namedtuple('InstanceState', ['instance_id', 'is_updated'])
  OperationConfigs = namedtuple('OperationConfigs', ['from_config', 'to_config'])
  InstanceConfigs = namedtuple(
      'InstanceConfigs',
      ['remote_config_map', 'local_config_map', 'instances_to_process']
  )

  def __init__(self, config, health_check_interval_seconds, scheduler=None, instance_watcher=None):
    self._config = config
    self._job_key = JobKey(role=config.role(), environment=config.environment(), name=config.name())
    self._health_check_interval_seconds = health_check_interval_seconds
    self._scheduler = scheduler or SchedulerProxy(config.cluster())
    try:
      self._update_config = UpdaterConfig(**config.update_config().get())
    except ValueError as e:
      raise self.InvalidConfigError(str(e))
    self._lock = None
    self._watcher = instance_watcher or InstanceWatcher(
        self._scheduler,
        self._job_key,
        self._update_config.restart_threshold,
        self._update_config.watch_secs,
        self._health_check_interval_seconds)

  def _start(self):
    """Starts an update by applying an exclusive lock on a job being updated.

       Returns:
         Response instance from the scheduler call.
    """
    resp = self._scheduler.acquireLock(LockKey(job=self._job_key))
    if resp.responseCode == ResponseCode.OK:
      self._lock = resp.result.acquireLockResult.lock
    else:
      log.error('Error starting update: %s' % resp.message)
      log.error(self.UPDATE_FAILURE_WARNING)
    return resp

  def _finish(self):
    """Finishes an update by removing an exclusive lock on an updated job.

       Returns:
         Response instance from the scheduler call.
    """
    resp = self._scheduler.releaseLock(self._lock, LockValidation.CHECKED)

    if resp.responseCode == ResponseCode.OK:
      self._lock = None
    else:
      log.error('There was an error finalizing the update: %s' % resp.message)
    return resp

  def _update(self, instances=None):
    """Drives execution of the update logic. Performs a batched update/rollback for all instances
    affected by the current update request.

    Arguments:
    instances -- (optional) set of instances to update.

    Returns the set of instances that failed to update.
    """
    failure_threshold = FailureThreshold(
        self._update_config.max_per_instance_failures,
        self._update_config.max_total_failures
    )

    instance_configs = self._get_update_instructions(instances)

    instance_operation = self.OperationConfigs(
      from_config=instance_configs.remote_config_map,
      to_config=instance_configs.local_config_map
    )

    remaining_instances = [
        self.InstanceState(instance_id, is_updated=False)
        for instance_id in instance_configs.instances_to_process
    ]

    log.info('Starting job update.')
    failed_instances = set()
    while remaining_instances and not failure_threshold.is_failed_update():
      batch_instances = remaining_instances[0 : self._update_config.batch_size]
      remaining_instances = list(set(remaining_instances) - set(batch_instances))
      instances_to_restart = [s.instance_id for s in batch_instances if s.is_updated]
      instances_to_update = [s.instance_id for s in batch_instances if not s.is_updated]

      instances_to_watch = []
      if instances_to_restart:
        instances_to_watch += self._restart_instances(instances_to_restart)

      if instances_to_update:
        instances_to_watch += self._update_instances(instances_to_update, instance_operation)

      failed_instances = self._watcher.watch(instances_to_watch) if instances_to_watch else set()

      if failed_instances:
        log.error('Failed instances: %s' % failed_instances)
      remaining_instances += [
          self.InstanceState(instance_id, is_updated=True) for instance_id in failed_instances
      ]
      remaining_instances.sort(key=lambda tup: tup.instance_id)
      failure_threshold.update_failure_counts(failed_instances)

    if failed_instances:
      untouched_instances = [s.instance_id for s in remaining_instances if not s.is_updated]
      instances_to_rollback = list(set(instance_configs.instances_to_process) - set(untouched_instances))
      self._rollback(instances_to_rollback, instance_configs)

    return failed_instances

  def _rollback(self, instances_to_rollback, instance_configs):
    """Performs a rollback operation for the failed instances.

    Arguments:
    instances_to_rollback -- instance ids to rollback.
    instance_configs -- instance configuration to use for rollback.
    """
    log.info('Reverting update for %s' % instances_to_rollback)
    instance_operation = self.OperationConfigs(
        from_config=instance_configs.local_config_map,
        to_config=instance_configs.remote_config_map
    )
    instances_to_rollback.sort(reverse=True)
    failed_instances = []
    while instances_to_rollback:
      batch_instances = instances_to_rollback[0 : self._update_config.batch_size]
      instances_to_rollback = list(set(instances_to_rollback) - set(batch_instances))
      instances_to_rollback.sort(reverse=True)
      instances_to_watch = self._update_instances(batch_instances, instance_operation)
      failed_instances += self._watcher.watch(instances_to_watch)

    if failed_instances:
      log.error('Rollback failed for instances: %s' % failed_instances)

  def _create_kill_add_lists(self, instance_ids, operation_configs):
    """Determines a particular action (kill or add) to use for every instance in instance_ids.

    Arguments:
    instance_ids -- current batch of IDs to process.
    operation_configs -- OperationConfigs with update details.

    Returns lists of instances to kill and to add.
    """
    to_kill = []
    to_add = []
    for instance_id in instance_ids:
      from_config = operation_configs.from_config.get(instance_id)
      to_config = operation_configs.to_config.get(instance_id)

      if from_config and to_config:
        # Sort internal dicts before comparing to rule out differences due to hashing.
        diff_output = ''.join(unified_diff(
          str(sorted(from_config.__dict__.items(), key=lambda x: x[0])),
          str(sorted(to_config.__dict__.items(), key=lambda x: x[0]))))
        if diff_output:
          log.debug('Task configuration changed for instance [%s]:\n%s' % (instance_id, diff_output))
          to_kill.append(instance_id)
          to_add.append(instance_id)
      elif from_config and not to_config:
        to_kill.append(instance_id)
      elif not from_config and to_config:
        to_add.append(instance_id)
      else:
        raise self.InvalidStateError('Instance %s is outside of supported range' % instance_id)

    return to_kill, to_add

  def _update_instances(self, instance_ids, operation_configs):
    """Applies kill/add actions for the specified batch instances.

    Arguments:
    instance_ids -- current batch of IDs to process.
    operation_configs -- OperationConfigs with update details.

    Returns a list of added instances.
    """
    log.info('Examining instances: %s' % instance_ids)

    to_kill, to_add = self._create_kill_add_lists(instance_ids, operation_configs)

    unchanged = list(set(instance_ids) - set(to_kill + to_add))
    if unchanged:
      log.info('Skipping unchanged instances: %s' % unchanged)

    # Kill is a blocking call in scheduler -> no need to watch it yet.
    self._kill_instances(to_kill)
    self._add_instances(to_add, operation_configs.to_config)
    return to_add

  def _kill_instances(self, instance_ids):
    """Instructs the scheduler to kill instances and waits for completion.

    Arguments:
    instance_ids -- list of IDs to kill.
    """
    if instance_ids:
      log.info('Killing instances: %s' % instance_ids)
      query = self._create_task_query(instanceIds=frozenset(int(s) for s in instance_ids))
      self._check_and_log_response(self._scheduler.killTasks(query, self._lock))
      log.info('Instances killed')

  def _add_instances(self, instance_ids, to_config):
    """Instructs the scheduler to add instances.

    Arguments:
    instance_ids -- list of IDs to add.
    to_config -- OperationConfigs with update details.
    """
    if instance_ids:
      log.info('Adding instances: %s' % instance_ids)
      add_config = AddInstancesConfig(
          key=self._job_key,
          taskConfig=to_config[instance_ids[0]],  # instance_ids will always have at least 1 item.
          instanceIds=frozenset(int(s) for s in instance_ids))
      self._check_and_log_response(self._scheduler.addInstances(add_config, self._lock))
      log.info('Instances added')

  def _restart_instances(self, instance_ids):
    """Instructs the scheduler to restart instances.

    Arguments:
    instance_ids -- set of instances to be restarted by the scheduler.
    """
    log.info('Restarting instances: %s' % instance_ids)
    resp = self._scheduler.restartShards(self._job_key, instance_ids, self._lock)
    self._check_and_log_response(resp)
    return instance_ids

  def _get_update_instructions(self, instances=None):
    """Loads, validates and populates update working set.

    Arguments:
    instances -- (optional) set of instances to update.

    Returns:
    InstanceConfigs with the following data:
      remote_config_map -- dictionary of {key:instance_id, value:task_config} from scheduler.
      local_config_map  -- dictionary of {key:instance_id, value:task_config} with local
                           task configs validated and populated with default values.
      instances_to_process -- list of instance IDs to go over in update.
    """
    # Load existing tasks and populate remote config map and instance list.
    assigned_tasks = self._get_existing_tasks()
    remote_config_map = {}
    remote_instances = []
    for assigned_task in assigned_tasks:
      remote_config_map[assigned_task.instanceId] = assigned_task.task
      remote_instances.append(assigned_task.instanceId)

    # Validate local job config and populate local task config.
    local_task_config = self._validate_and_populate_local_config()

    # Union of local and remote instance IDs.
    job_config_instances = list(range(self._config.instances()))
    instance_superset = sorted(list(set(remote_instances) | set(job_config_instances)))

    # Calculate the update working set.
    if instances is None:
      # Full job update -> union of remote and local instances
      instances_to_process = instance_superset
    else:
      # Partial job update -> validate all instances are recognized
      instances_to_process = instances
      unrecognized = list(set(instances) - set(instance_superset))
      if unrecognized:
        raise self.InvalidConfigError('Instances %s are outside of supported range' % unrecognized)

    # Populate local config map
    local_config_map = dict.fromkeys(job_config_instances, local_task_config)

    return self.InstanceConfigs(remote_config_map, local_config_map, instances_to_process)

  def _get_existing_tasks(self):
    """Loads all existing tasks from the scheduler.

    Returns a list of AssignedTasks.
    """
    resp = self._scheduler.getTasksStatus(self._create_task_query())
    self._check_and_log_response(resp)
    return [t.assignedTask for t in resp.result.scheduleStatusResult.tasks]

  def _validate_and_populate_local_config(self):
    """Validates local job configuration and populates local task config with default values.

    Returns a TaskConfig populated with default values.
    """
    resp = self._scheduler.populateJobConfig(self._config.job(), JobConfigValidation.RUN_FILTERS)
    self._check_and_log_response(resp)

    # Safe to take the first element as Scheduler would throw in case zero instances provided.
    return list(resp.result.populateJobResult.populated)[0]

  def _replace_template_if_cron(self):
    """Checks if the provided job config represents a cron job and if so, replaces it.

    Returns True if job is cron and False otherwise.
    """
    if self._config.job().cronSchedule:
      resp = self._scheduler.replaceCronTemplate(self._config.job(), self._lock)
      self._check_and_log_response(resp)
      return True
    else:
      return False

  def _create_task_query(self, instanceIds=None):
    return TaskQuery(
        owner=Identity(role=self._job_key.role),
        environment=self._job_key.environment,
        jobName=self._job_key.name,
        statuses=ACTIVE_STATES,
        instanceIds=instanceIds)

  def update(self, instances=None):
    """Performs the job update, blocking until it completes.
    A rollback will be performed if the update was considered a failure based on the
    update configuration.

    Arguments:
    instances -- (optional) instances to update. If not specified, all instances will be updated.

    Returns a response object with update result status.
    """
    resp = self._start()
    if resp.responseCode != ResponseCode.OK:
      return resp

    try:
      # Handle cron jobs separately from other jobs.
      if self._replace_template_if_cron():
        log.info('Cron template updated, next run will reflect changes')
      else:
        failed_instances = self._update(instances)
        if failed_instances:
          log.error('Update reverted, failures detected on instances %s' % failed_instances)
        else:
          log.info('Update successful')
      resp = self._finish()
    except (self.InvalidConfigError, self.InvalidStateError) as e:
      log.error(e)
      log.error('Aborting update without rollback!')
    except self.Error:
      pass  # Error is already logged in _check_and_log_response()

    return resp

  @classmethod
  def cancel_update(cls, scheduler, job_key):
    """Cancels an update process by removing an exclusive lock on a provided job.

    Arguments:
    scheduler -- scheduler instance to use.
    job_key -- job key to cancel update for.

    Returns a response object with cancel update result status.
    """
    return scheduler.releaseLock(
        Lock(key=LockKey(job=job_key.to_thrift())),
        LockValidation.UNCHECKED)

  @classmethod
  def _check_and_log_response(cls, resp):
    """Checks scheduler return status, logs and raises Error in case of unexpected response.

    Arguments:
    resp -- scheduler response object.

    Raises Error in case of unexpected response status.
    """
    name, message = ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message
    if resp.responseCode == ResponseCode.OK:
      log.debug('Response from scheduler: %s (message: %s)' % (name, message))
    else:
      e = cls.Error('Unexpected response from scheduler: %s (message: %s)' % (name, message))
      log.error(e)
      log.error('Aborting update without rollback!!!')
      raise e
