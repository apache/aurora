import collections
from math import ceil
from mesos_twitter.ttypes import *
import twitter.common.log
from twitter.common import options

options.add('--mesos_updater_status_check_inteval',
  dest='mesos_updater_status_check_interval',
  default=3,
  type='int',
  help='How often Mesos runs update loop...')
log = twitter.common.log.get()

class InvalidUpdaterConfigException(Exception): pass

class Updater(object):
  """Update the shards of a job in batches."""

  def __init__(self, role, job, scheduler, clock, update_token):
    self._role = role
    self._job = job
    self._scheduler = scheduler
    self._clock = clock
    self._update_token = update_token
    self._total_fail_count = 0
    self._max_total_failures = 0
    self._failures_by_shard = collections.defaultdict(int)

  @staticmethod
  def validate_config(update_config, shard_count):
    """Perform sanity test on update_config."""
    if update_config.batchSize < 1:
      raise InvalidUpdaterConfigException('Batch size should be greater than 0')
    if update_config.restartThreshold < 1:
      raise InvalidUpdaterConfigException('Restart Threshold should be greater than 0')
    if update_config.watchSecs < 1:
      raise InvalidUpdaterConfigException('Watch seconds should be greater than 0')
    if update_config.batchSize > shard_count:
      raise InvalidUpdaterConfigException('Batch size is greater than the total shards present')

  def exceeded_total_fail_count(self, failed_shards):
    """Checks if the total number of failures is greater than a threshold."""
    self._total_fail_count += len(failed_shards)
    if self._total_fail_count > self._max_total_failures:
      log.info('%s failures observed, maximum allowed is %s' % (self._total_fail_count,
          self._max_total_failures))
      return True
    return False

  def exceeded_shard_fail_count(self, failed_shards):
    """Checks if the per shard failure is greater than a threshold."""
    for shard in failed_shards:
      self._failures_by_shard[shard] += 1
      if self._failures_by_shard[shard] > self._max_shard_failures:
        log.info('%s shard failures for shard %s, maximum allowed is %s' %
            (self._failures_by_shard[shard], shard, self._max_shard_failures))
        return True
    return False

  def is_failed_update(self, failed_shards):
    return (self.exceeded_total_fail_count(failed_shards) or
        self.exceeded_shard_fail_count(failed_shards))

  def update(self, job_config):
    """Performs the job update, blocking until it completes.
    A rollback will be performed if the update was considered a failure based on the
    update configuration.

    Arguments:
    job_config -- job configuration object.

    Returns the set of shards that failed to update.
    """
    # TODO(Sathya): Include an option in update_config to choose either,
    #     a constant forward progress option or a retry batch untill success option.
    update_config = job_config.updateConfig
    failed_shards = []
    initial_shards = []
    self._max_total_failures = update_config.maxTotalFailures
    self._max_shard_failures = update_config.maxPerShardFailures
    for config in job_config.taskConfigs:
      initial_shards += [config.shardId]
    initial_shards.sort()
    Updater.validate_config(update_config, len(initial_shards))
    remaining_shards = initial_shards
    update_in_progress = True
    while update_in_progress:
      batch_shards = remaining_shards[0 : update_config.batchSize]
      remaining_shards = [shard for shard in set(remaining_shards).difference(batch_shards)]
      resp = self.restart_shards(batch_shards, update_config)
      log.info('Response from scheduler: %s (message: %s)'
          % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
      failed_shards = self.watch_tasks(batch_shards, update_config.restartThreshold,
          update_config.watchSecs)
      log.info('Failed_tasks : %s' % failed_shards)
      remaining_shards += failed_shards
      remaining_shards.sort()
      update_in_progress = not(self.is_failed_update(failed_shards) or remaining_shards == [])
    if failed_shards:
      shards_to_rollback = [shard for shard in set(initial_shards).difference(remaining_shards)] + failed_shards
      self.rollback(update_config, shards_to_rollback)
    return failed_shards

  def rollback(self, update_config, shards_to_rollback):
    """Performs the job rollback.

    Arguments:
    update_config -- update configuration object that describes how the rollback is performed.
    shards_to_rollback -- shard ids.
    """
    log.info('Reverting update for %s' % shards_to_rollback)
    shards_to_rollback.sort()
    rollback_in_progress = True
    while rollback_in_progress:
      batch_shards = shards_to_rollback[0 : update_config.batchSize]
      shards_to_rollback = [shard for shard in set(shards_to_rollback).difference(batch_shards)]
      resp = self._scheduler.rollbackShards(self._role, self._job['name'], batch_shards, self._update_token)
      log.info('Response from scheduler: %s (message: %s)'
          % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
      rollback_in_progress = not(shards_to_rollback == [])

  def restart_shards(self, shard_ids, update_config):
    """Performs a scheduler call for restart.

    Arguments:
    shard_ids -- set of shards to be restarted by the scheduler.
    update_config -- update configuration object that describes how the restart is performed.

    Returns a map of the current status of the restarted shards as returned by the scheduler.
    """
    log.info('Restarting shards')
    return self._scheduler.updateShards(self._role, self._job['name'], shard_ids, self._update_token)

  def watch_tasks(self, task_ids, restart_threshold, watch_secs):
    """Monitors the restarted shards.

    Arguments:
    task_ids -- set of shards to watch.
    restart_threshold -- Maximum number of seconds before which a task must move
                         to the RUNNING state.
    watch_secs -- Number of seconds to watch the task once it is RUNNING.

    Returns a set of tasks that failed to meet the following criteria,
    1. Failed to move to RUNNING state before restart_threshold from the time of restart.
    2. Failed to stay in the RUNNING state before watch_secs expire.
    """
    ACTIVE_STATES = set([ScheduleStatus.PENDING, ScheduleStatus.STARTING, ScheduleStatus.RUNNING])
    start_time = self._clock.time()
    expected_running_by = start_time + restart_threshold
    statuses = {}
    running_state_times = {}
    healthy_tasks = set()
    failed_shards = set()
    while True:
      log.info('Getting Status...')
      query = TaskQuery()
      query.owner = Identity(role = self._role)
      query.jobName = self._job['name']
      resp = self._scheduler.getTasksStatus(query)
      log.info('Response from scheduler: %s (message: %s)'
          % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
      if resp.tasks:
        for task in resp.tasks:
          statuses[task.scheduledTask.assignedTask.task.shardId] = task.scheduledTask.status
      else:
        log.info('No tasks found.')
      log.info('Got statuses: %s' % statuses)
      now = self._clock.time()
      for task_id, status in statuses.items():
        if status is ScheduleStatus.RUNNING and task_id not in (
          failed_shards.union(running_state_times.keys())):
          running_state_times[task_id] = now
          log.info('Adding %s to Running Tasks' % task_id)
      if now > expected_running_by:
        non_running_tasks = [id for id in task_ids if id not in running_state_times]
        log.info('Tasks failed to move into running: %s' % non_running_tasks)
        failed_shards.update(non_running_tasks)
      healthy_tasks.update(id for id in statuses
          if id in running_state_times and now > running_state_times[id] + watch_secs)
      failed_shards.update(id for id in statuses
          if id not in healthy_tasks and statuses[id] not in ACTIVE_STATES)
      if healthy_tasks.union(failed_shards) == set(task_ids):
        return [shard for shard in failed_shards]
      elif now > (start_time + restart_threshold + watch_secs):
        return [shard for shard in set(task_ids).difference(healthy_tasks)]
      self._clock.sleep(options.values().mesos_updater_status_check_interval)