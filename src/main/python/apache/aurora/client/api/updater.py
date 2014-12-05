#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import signal
from collections import namedtuple
from difflib import unified_diff
from threading import Lock as threading_lock
from threading import Event

from thrift.protocol import TJSONProtocol
from thrift.TSerialization import serialize
from twitter.common import log
from twitter.common.quantity import Amount, Time

from apache.aurora.client.base import combine_messages, format_response

from .error_handling_thread import ExecutionError, spawn_worker
from .instance_watcher import InstanceWatcher
from .job_monitor import JobMonitor
from .quota_check import CapacityRequest, QuotaCheck
from .scheduler_client import SchedulerProxy
from .scheduler_mux import SchedulerMux
from .updater_util import FailureThreshold, UpdaterConfig

from gen.apache.aurora.api.constants import ACTIVE_STATES
from gen.apache.aurora.api.ttypes import (
    AddInstancesConfig,
    JobKey,
    Lock,
    LockKey,
    LockValidation,
    Response,
    ResponseCode,
    ResponseDetail,
    TaskQuery
)

try:
  from Queue import Queue, Empty
except ImportError:
  from queue import Queue, Empty


class Updater(object):
  """Performs an update command using a collection of parallel threads.
  The number of parallel threads used is determined by the UpdateConfig.batch_size."""

  class Error(Exception):
    """Updater error wrapper."""
    pass

  RPC_COMPLETION_TIMEOUT_SECS = Amount(120, Time.SECONDS)

  OPERATION_CONFIGS = namedtuple('OperationConfigs', ['from_config', 'to_config'])
  INSTANCE_CONFIGS = namedtuple(
      'InstanceConfigs',
      ['remote_config_map', 'local_config_map', 'instances_to_process']
  )

  INSTANCE_DATA = namedtuple('InstanceData', ['instance_id', 'operation_configs'])

  def __init__(self,
               config,
               health_check_interval_seconds,
               scheduler=None,
               instance_watcher=None,
               quota_check=None,
               job_monitor=None,
               scheduler_mux=None,
               rpc_completion_timeout=RPC_COMPLETION_TIMEOUT_SECS):
    self._config = config
    self._job_key = JobKey(role=config.role(), environment=config.environment(), name=config.name())
    self._health_check_interval_seconds = health_check_interval_seconds
    self._scheduler = scheduler or SchedulerProxy(config.cluster())
    self._quota_check = quota_check or QuotaCheck(self._scheduler)
    self._scheduler_mux = scheduler_mux or SchedulerMux()
    self._job_monitor = job_monitor or JobMonitor(
        self._scheduler,
        self._config.job_key(),
        scheduler_mux=self._scheduler_mux)
    self._rpc_completion_timeout = rpc_completion_timeout
    try:
      self._update_config = UpdaterConfig(**config.update_config().get())
    except ValueError as e:
      raise self.Error(str(e))
    self._lock = None
    self._thread_lock = threading_lock()
    self._batch_wait_event = Event()
    self._batch_completion_queue = Queue()
    self.failure_threshold = FailureThreshold(
        self._update_config.max_per_instance_failures,
        self._update_config.max_total_failures
    )
    self._watcher = instance_watcher or InstanceWatcher(
        self._scheduler,
        self._job_key,
        self._update_config.restart_threshold,
        self._update_config.watch_secs,
        self._health_check_interval_seconds,
        scheduler_mux=self._scheduler_mux)
    self._terminating = False

  def _start(self):
    """Starts an update by applying an exclusive lock on a job being updated.

    Returns Response instance from the scheduler call.
    """
    resp = self._scheduler.acquireLock(LockKey(job=self._job_key))
    if resp.responseCode == ResponseCode.OK:
      self._lock = resp.result.acquireLockResult.lock
    return resp

  def _finish(self):
    """Finishes an update by removing an exclusive lock on an updated job.

    Returns Response instance from the scheduler call.
    """
    resp = self._scheduler.releaseLock(self._lock, LockValidation.CHECKED)

    if resp.responseCode == ResponseCode.OK:
      self._lock = None
    else:
      log.error('There was an error finalizing the update: %s' % combine_messages(resp))
    return resp

  def int_handler(self, *args):
    """Ensures keyboard interrupt exception is raised on a main thread."""
    raise KeyboardInterrupt()

  def _update(self, instance_configs):
    """Drives execution of the update logic.

    Performs instance updates in parallel using a number of threads bound by
    the batch_size config option.

    Arguments:
    instance_configs -- list of instance update configurations to go through.

    Returns the set of instances that failed to update.
    """
    # Register signal handler to ensure KeyboardInterrupt is received by a main thread.
    signal.signal(signal.SIGINT, self.int_handler)

    instances_to_update = [
      self.INSTANCE_DATA(
        instance_id,
        self.OPERATION_CONFIGS(
          from_config=instance_configs.remote_config_map,
          to_config=instance_configs.local_config_map))
      for instance_id in instance_configs.instances_to_process
    ]

    log.info('Instances to update: %s' % instance_configs.instances_to_process)
    update_queue = self._update_instances_in_parallel(self._update_instance, instances_to_update)

    if self._is_failed_update(quiet=False):
      if not self._update_config.rollback_on_failure:
        log.info('Rollback on failure is disabled in config. Aborting rollback')
        return

      rollback_ids = self._get_rollback_ids(instance_configs.instances_to_process, update_queue)
      instances_to_revert = [
          self.INSTANCE_DATA(
              instance_id,
              self.OPERATION_CONFIGS(
                  from_config=instance_configs.local_config_map,
                  to_config=instance_configs.remote_config_map))
          for instance_id in rollback_ids
      ]

      log.info('Reverting update for: %s' % rollback_ids)
      self._update_instances_in_parallel(self._revert_instance, instances_to_revert)

    return not self._is_failed_update()

  def _update_instances_in_parallel(self, target, instances_to_update):
    """Processes instance updates in parallel and waits for completion.

    Arguments:
    target -- target method to handle instance update.
    instances_to_update -- list of InstanceData with update details.

    Returns Queue with non-updated instance data.
    """
    log.info('Processing in parallel with %s worker thread(s)' % self._update_config.batch_size)
    instance_queue = Queue()
    for instance_to_update in instances_to_update:
      instance_queue.put(instance_to_update)

    try:
      threads = []
      for _ in range(self._update_config.batch_size):
        threads.append(spawn_worker(target, kwargs={'instance_queue': instance_queue}))

      for thread in threads:
        thread.join_and_raise()
    except Exception as e:
      log.debug('Caught unhandled exception: %s' % e)
      self._terminate()
      raise

    return instance_queue

  def _try_reset_batch_wait_event(self, instance_id, instance_queue):
    """Resets batch_wait_event in case the current batch is filled up.

    This is a helper method that separates thread locked logic. Called from
    _wait_for_batch_completion_if_needed() when a given instance update completes.
    Resumes worker threads if all batch instances are updated.

    Arguments:
    instance_id -- Instance ID being processed.
    instance_queue -- Instance update work queue.
    """
    with self._thread_lock:
      log.debug("Instance ID %s: Completion queue size %s" %
                (instance_id, self._batch_completion_queue.qsize()))
      log.debug("Instance ID %s: Instance queue size %s" %
                (instance_id, instance_queue.qsize()))
      self._batch_completion_queue.put(instance_id)
      filled_up = self._batch_completion_queue.qsize() % self._update_config.batch_size == 0
      all_done = instance_queue.qsize() == 0
      if filled_up or all_done:
        # Required batch size of completed instances has filled up -> unlock waiting threads.
        log.debug('Instance %s completes the batch wait.' % instance_id)
        self._batch_wait_event.set()
        self._batch_wait_event.clear()
        return True

    return False

  def _wait_for_batch_completion_if_needed(self, instance_id, instance_queue):
    """Waits for batch completion if wait_for_batch_completion flag is set.

    Arguments:
    instance_id -- Instance ID.
    instance_queue -- Instance update work queue.
    """
    if not self._update_config.wait_for_batch_completion:
      return

    if not self._try_reset_batch_wait_event(instance_id, instance_queue):
      # The current batch has not filled up -> block the work thread.
      log.debug('Instance %s is done. Waiting for batch to complete.' % instance_id)
      self._batch_wait_event.wait()

  def _terminate(self):
    """Attempts to terminate all outstanding activities."""
    if not self._terminating:
      log.info('Cleaning up')
      self._terminating = True
      self._scheduler.terminate()
      self._job_monitor.terminate()
      self._scheduler_mux.terminate()
      self._watcher.terminate()
      self._batch_wait_event.set()

  def _update_instance(self, instance_queue):
    """Works through the instance_queue and performs instance updates (one at a time).

    Arguments:
    instance_queue -- Queue of InstanceData to update.
    """
    while not self._terminating and not self._is_failed_update():
      try:
        instance_data = instance_queue.get_nowait()
      except Empty:
        return

      update = True
      restart = False
      while update or restart and not self._terminating and not self._is_failed_update():
        instances_to_watch = []
        if update:
          instances_to_watch += self._kill_and_add_instance(instance_data)
          update = False
        else:
          instances_to_watch += self._request_restart_instance(instance_data)

        if instances_to_watch:
          failed_instances = self._watcher.watch(instances_to_watch)
          restart = self._is_restart_needed(failed_instances)

      self._wait_for_batch_completion_if_needed(instance_data.instance_id, instance_queue)

  def _revert_instance(self, instance_queue):
    """Works through the instance_queue and performs instance rollbacks (one at a time).

    Arguments:
    instance_queue -- Queue of InstanceData to revert.
    """
    while not self._terminating:
      try:
        instance_data = instance_queue.get_nowait()
      except Empty:
        return

      log.info('Reverting instance: %s' % instance_data.instance_id)
      instances_to_watch = self._kill_and_add_instance(instance_data)
      if instances_to_watch and self._watcher.watch(instances_to_watch):
        log.error('Rollback failed for instance: %s' % instance_data.instance_id)

  def _kill_and_add_instance(self, instance_data):
    """Acquires update instructions and performs required kill/add/kill+add sequence.

    Arguments:
    instance_data -- InstanceData to update.

    Returns added instance ID.
    """
    log.info('Examining instance: %s' % instance_data.instance_id)
    to_kill, to_add = self._create_kill_add_lists(
        [instance_data.instance_id],
        instance_data.operation_configs)
    if not to_kill and not to_add:
      log.info('Skipping unchanged instance: %s' % instance_data.instance_id)
      return to_add

    if to_kill:
      self._request_kill_instance(instance_data)
    if to_add:
      self._request_add_instance(instance_data)

    return to_add

  def _request_kill_instance(self, instance_data):
    """Instructs the scheduler to kill instance and waits for completion.

    Arguments:
    instance_data -- InstanceData to kill.
    """
    log.info('Killing instance: %s' % instance_data.instance_id)
    self._enqueue_and_wait(instance_data, self._kill_instances)
    result = self._job_monitor.wait_until(
        JobMonitor.terminal,
        [instance_data.instance_id],
        with_timeout=True)

    if not result:
      raise self.Error('Instance %s was not killed in time' % instance_data.instance_id)
    log.info('Killed: %s' % instance_data.instance_id)

  def _request_add_instance(self, instance_data):
    """Instructs the scheduler to add instance.

    Arguments:
    instance_data -- InstanceData to add.
    """
    log.info('Adding instance: %s' % instance_data.instance_id)
    self._enqueue_and_wait(instance_data, self._add_instances)
    log.info('Added: %s' % instance_data.instance_id)

  def _request_restart_instance(self, instance_data):
    """Instructs the scheduler to restart instance.

    Arguments:
    instance_data -- InstanceData to restart.

    Returns restarted instance ID.
    """
    log.info('Restarting instance: %s' % instance_data.instance_id)
    self._enqueue_and_wait(instance_data, self._restart_instances)
    log.info('Restarted: %s' % instance_data.instance_id)
    return [instance_data.instance_id]

  def _enqueue_and_wait(self, instance_data, command):
    """Queues up the scheduler call and waits for completion.

    Arguments:
    instance_data -- InstanceData to query scheduler for.
    command -- scheduler command to run.
    """
    try:
      self._scheduler_mux.enqueue_and_wait(
          command,
          instance_data,
          timeout=self._rpc_completion_timeout)
    except SchedulerMux.Error as e:
      raise self.Error('Failed to complete instance %s operation. Reason: %s'
          % (instance_data.instance_id, e))

  def _is_failed_update(self, quiet=True):
    """Verifies the update status in a thread-safe manner.

    Arguments:
    quiet -- Whether the logging should be suppressed in case of a failed update. Default True.

    Returns True if update failed, False otherwise.
    """
    with self._thread_lock:
      return self.failure_threshold.is_failed_update(log_errors=not quiet)

  def _is_restart_needed(self, failed_instances):
    """Checks if there are any failed instances recoverable via restart.

    Arguments:
    failed_instances -- Failed instance IDs.

    Returns True if restart is allowed, False otherwise (i.e. update failed).
    """
    if not failed_instances:
      return False

    log.info('Failed instances: %s' % failed_instances)

    with self._thread_lock:
      unretryable_instances = self.failure_threshold.update_failure_counts(failed_instances)
      if unretryable_instances:
        log.warn('Not restarting failed instances %s, which exceeded '
                 'maximum allowed instance failure limit of %s' %
                 (unretryable_instances, self._update_config.max_per_instance_failures))
      return False if unretryable_instances else True

  def _get_rollback_ids(self, update_list, update_queue):
    """Gets a list of instance ids to rollback.

    Arguments:
    update_list -- original list of instances intended for update.
    update_queue -- untouched instances not processed during update.

    Returns sorted list of instance IDs to rollback.
    """
    untouched_ids = []
    while not update_queue.empty():
      untouched_ids.append(update_queue.get_nowait().instance_id)

    return sorted(list(set(update_list) - set(untouched_ids)), reverse=True)

  def _hashable(self, element):
    if isinstance(element, (list, set)):
      return tuple(sorted(self._hashable(item) for item in element))
    elif isinstance(element, dict):
      return tuple(
          sorted((self._hashable(key), self._hashable(value)) for (key, value) in element.items())
      )
    return element

  def _thrift_to_json(self, config):
    return json.loads(
        serialize(config, protocol_factory=TJSONProtocol.TSimpleJSONProtocolFactory()))

  def _diff_configs(self, from_config, to_config):
    # Thrift objects do not correctly compare against each other due to the unhashable nature
    # of python sets. That results in occasional diff failures with the following symptoms:
    # - Sets are not equal even though their reprs are identical;
    # - Items are reordered within thrift structs;
    # - Items are reordered within sets;
    # To overcome all the above, thrift objects are converted into JSON dicts to flatten out
    # thrift type hierarchy. Next, JSONs are recursively converted into nested tuples to
    # ensure proper ordering on compare.
    return ''.join(unified_diff(repr(self._hashable(self._thrift_to_json(from_config))),
                                repr(self._hashable(self._thrift_to_json(to_config)))))

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
        diff_output = self._diff_configs(from_config, to_config)
        if diff_output:
          log.debug('Task configuration changed for instance [%s]:\n%s'
                    % (instance_id, diff_output))
          to_kill.append(instance_id)
          to_add.append(instance_id)
      elif from_config and not to_config:
        to_kill.append(instance_id)
      elif not from_config and to_config:
        to_add.append(instance_id)
      else:
        raise self.Error('Instance %s is outside of supported range' % instance_id)

    return to_kill, to_add

  def _kill_instances(self, instance_data):
    """Instructs the scheduler to batch-kill instances and waits for completion.

    Arguments:
    instance_data -- list of InstanceData to kill.
    """
    instance_ids = [data.instance_id for data in instance_data]
    log.debug('Batch killing instances: %s' % instance_ids)
    query = self._create_task_query(instanceIds=frozenset(int(s) for s in instance_ids))
    self._check_and_log_response(self._scheduler.killTasks(query, self._lock))
    log.debug('Done batch killing instances: %s' % instance_ids)

  def _add_instances(self, instance_data):
    """Instructs the scheduler to batch-add instances.

    Arguments:
    instance_data -- list of InstanceData to add.
    """
    instance_ids = [data.instance_id for data in instance_data]
    to_config = instance_data[0].operation_configs.to_config

    log.debug('Batch adding instances: %s' % instance_ids)
    add_config = AddInstancesConfig(
        key=self._job_key,
        taskConfig=to_config[instance_ids[0]],  # instance_ids will always have at least 1 item.
        instanceIds=frozenset(int(s) for s in instance_ids))
    self._check_and_log_response(self._scheduler.addInstances(add_config, self._lock))
    log.debug('Done batch adding instances: %s' % instance_ids)

  def _restart_instances(self, instance_data):
    """Instructs the scheduler to batch-restart instances.

    Arguments:
    instance_data -- list of InstanceData to restart.
    """
    instance_ids = [data.instance_id for data in instance_data]
    log.debug('Batch restarting instances: %s' % instance_ids)
    resp = self._scheduler.restartShards(self._job_key, instance_ids, self._lock)
    self._check_and_log_response(resp)
    log.debug('Done batch restarting instances: %s' % instance_ids)

  def _validate_quota(self, instance_configs):
    """Validates job update will not exceed quota for production tasks.
    Arguments:
    instance_configs -- InstanceConfig with update details.

    Returns Response.OK if quota check was successful.
    """
    instance_operation = self.OPERATION_CONFIGS(
      from_config=instance_configs.remote_config_map,
      to_config=instance_configs.local_config_map
    )

    def _aggregate_quota(ops_list, config_map):
      request = CapacityRequest()
      for instance in ops_list:
        task = config_map[instance]
        if task.production:
          request += CapacityRequest.from_task(task)

      return request

    to_kill, to_add = self._create_kill_add_lists(
        instance_configs.instances_to_process,
        instance_operation)

    return self._quota_check.validate_quota_from_requested(
        self._job_key,
        self._config.job().taskConfig.production,
        _aggregate_quota(to_kill, instance_operation.from_config),
        _aggregate_quota(to_add, instance_operation.to_config))

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
        raise self.Error('Instances %s are outside of supported range' % unrecognized)

    # Populate local config map
    local_config_map = dict.fromkeys(job_config_instances, local_task_config)

    return self.INSTANCE_CONFIGS(remote_config_map, local_config_map, instances_to_process)

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
    resp = self._scheduler.populateJobConfig(self._config.job())
    self._check_and_log_response(resp)

    # Safe to take the first element as Scheduler would throw in case zero instances provided.
    return list(resp.result.populateJobResult.populatedDEPRECATED)[0]

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
    return TaskQuery(jobKeys=[self._job_key], statuses=ACTIVE_STATES, instanceIds=instanceIds)

  def _failed_response(self, message):
    # TODO(wfarner): Avoid synthesizing scheduler responses, consider using an exception instead.
    return Response(responseCode=ResponseCode.ERROR, details=[ResponseDetail(message=message)])

  def update(self, instances=None):
    """Performs the job update, blocking until it completes.

    A rollback will be performed if the update was considered a failure based on the
    update configuration.

    Arguments:
    instances -- (optional) instances to update. If not specified, all instances will be updated.

    Returns a response object with update result status.
    """
    try:
      resp = self._start()
      if resp.responseCode != ResponseCode.OK:
        return resp

      try:
        # Handle cron jobs separately from other jobs.
        if self._replace_template_if_cron():
          log.info('Cron template updated, next run will reflect changes')
          return self._finish()
        else:
          try:
            instance_configs = self._get_update_instructions(instances)
            self._check_and_log_response(self._validate_quota(instance_configs))
          except self.Error as e:
            # Safe to release the lock acquired above as no job mutation has happened yet.
            self._finish()
            return self._failed_response('Unable to start job update: %s' % e)

          if not self._update(instance_configs):
            log.warn('Update failures threshold reached')
            self._finish()
            return self._failed_response('Update reverted')
          else:
            log.info('Update successful')
            return self._finish()
      except (self.Error, ExecutionError, Exception) as e:
        return self._failed_response('Aborting update without rollback! Fatal error: %s' % e)
    finally:
      self._scheduler_mux.terminate()

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

  def _check_and_log_response(self, resp):
    """Checks scheduler return status, raises Error in case of unexpected response.

    Arguments:
    resp -- scheduler response object.

    Raises Error in case of unexpected response status.
    """
    message = format_response(resp)
    if resp.responseCode == ResponseCode.OK:
      log.debug(message)
    else:
      raise self.Error(message)
