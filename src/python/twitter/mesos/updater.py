import collections
from math import ceil
from gen.twitter.mesos.ttypes import *
from twitter.common import app, log

app.add_option('--mesos_updater_status_check_interval',
  dest='mesos_updater_status_check_interval',
  default=3,
  type='int',
  help='How often Mesos runs update loop...')

def debug_if(condition):
  return log.DEBUG if condition else log.ERROR

class Updater(object):
  """Update the shards of a job in batches."""

  class InvalidConfigError(Exception): pass

  def __init__(self, role, job_name, scheduler, clock, update_token, session):
    self._role = role
    self._job_name = job_name
    self._scheduler = scheduler
    self._clock = clock
    self._update_token = update_token
    self._session = session
    self._total_fail_count = 0
    self._max_total_failures = 0
    self._failures_by_shard = collections.defaultdict(int)

  @staticmethod
  def validate_config(update_config):
    """Perform sanity test on update_config."""
    if update_config.batchSize < 1:
      raise Updater.InvalidConfigError('Batch size should be greater than 0')
    if update_config.restartThreshold < 1:
      raise Updater.InvalidConfigError('Restart Threshold should be greater than 0')
    if update_config.watchSecs < 1:
      raise Updater.InvalidConfigError('Watch seconds should be greater than 0')

  def update_failure_counts(self, failed_shards):
    """Update the failure counts metrics based upon a batch of failed shards."""
    self._total_fail_count += len(failed_shards)
    for shard in failed_shards:
      self._failures_by_shard[shard] += 1

  def exceeded_total_fail_count(self):
    """Checks if the total number of failures is greater than a threshold."""
    return self._total_fail_count > self._max_total_failures

  def exceeded_shard_fail_count(self):
    """Checks if the per shard failure is greater than a threshold."""
    return any(map(lambda num_failures: num_failures > self._max_shard_failures,
      self._failures_by_shard.values()))

  def is_failed_update(self, failed_shards):
    is_failed = self.exceeded_total_fail_count() or self.exceeded_shard_fail_count()

    if is_failed:
      log.error('%s failures observed, maximum allowed is %s' % (self._total_fail_count,
          self._max_total_failures))
      for shard in self._failures_by_shard:
        if self._failures_by_shard[shard] > self._max_shard_failures:
          log.error('%s shard failures for shard %s, maximum allowed is %s' %
              (self._failures_by_shard[shard], shard, self._max_shard_failures))
    return is_failed

  def update(self, job_config):
    """Performs the job update, blocking until it completes.
    A rollback will be performed if the update was considered a failure based on the
    update configuration.

    Arguments:
    job_config -- job configuration object.

    Returns the set of shards that failed to update.
    """
    Updater.validate_config(job_config.updateConfig)

    update_config = job_config.updateConfig
    failed_shards = []
    self._max_total_failures = update_config.maxTotalFailures
    self._max_shard_failures = update_config.maxPerShardFailures
    initial_shards = sorted(map(lambda config: config.shardId, job_config.taskConfigs))
    remaining_shards = initial_shards[:]
    update_in_progress = True

    log.info('Starting job update...')
    while update_in_progress:
      batch_shards = remaining_shards[0 : update_config.batchSize]
      remaining_shards = list(set(remaining_shards) - set(batch_shards))
      resp = self.restart_shards(batch_shards, update_config)
      log.log(debug_if(resp.responseCode == UpdateResponseCode.OK),
        'Response from scheduler: %s (message: %s)' % (
          UpdateResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
      failed_shards = self.watch_tasks(batch_shards, update_config.restartThreshold,
          update_config.watchSecs)
      log.log(debug_if(failed_shards == []),
        'Failed_tasks : %s' % (failed_shards if failed_shards != [] else 'None'))
      remaining_shards += failed_shards
      remaining_shards.sort()
      self.update_failure_counts(failed_shards)
      update_in_progress = len(remaining_shards) == 0 and not self.is_failed_update()

    if failed_shards:
      shards_to_rollback = [shard for shard in set(initial_shards).difference(remaining_shards)] + (
          failed_shards)
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
    while len(shards_to_rollback) > 0:
      batch_shards = shards_to_rollback[0 : update_config.batchSize]
      shards_to_rollback = list(set(shards_to_rollback) - set(batch_shards))
      resp = self._scheduler.rollbackShards(self._role, self._job_name, batch_shards,
          self._update_token, self._session)
      self.watch_tasks(batch_shards, update_config.restartThreshold, update_config.watchSecs)
      log.log(debug_if(resp == UpdateResponseCode.OK),
        'Response from scheduler: %s (message: %s)'
          % (UpdateResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))

  def restart_shards(self, shard_ids, update_config):
    """Performs a scheduler call for restart.

    Arguments:
    shard_ids -- set of shards to be restarted by the scheduler.
    update_config -- update configuration object that describes how the restart is performed.

    Returns a map of the current status of the restarted shards as returned by the scheduler.
    """
    log.info('Restarting shards: %s' % shard_ids)
    return self._scheduler.updateShards(self._role, self._job_name, shard_ids,
        self._update_token, self._session)

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
    # TODO(Sathya): Consider adding PREEMPTING as an active state.
    ACTIVE_STATES = set([ScheduleStatus.PENDING, ScheduleStatus.STARTING, ScheduleStatus.RUNNING,
        ScheduleStatus.ASSIGNED, ScheduleStatus.UPDATING, ScheduleStatus.ROLLBACK])
    start_time = self._clock.time()
    expected_running_by = start_time + restart_threshold
    statuses = {}
    running_state_times = {}
    healthy_tasks = set()
    failed_shards = set()
    while True:
      log.debug('Getting status...')
      query = TaskQuery()
      query.owner = Identity(role = self._role)
      query.jobName = self._job_name
      resp = self._scheduler.getTasksStatus(query)
      log.debug('Response from scheduler: %s (message: %s)'
          % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
      if resp.tasks:
        for task in resp.tasks:
          statuses[task.scheduledTask.assignedTask.task.shardId] = task.scheduledTask.status
      else:
        log.debug('No tasks found.')
      log.debug('Got statuses: %s' %
          dict([(task,ScheduleStatus._VALUES_TO_NAMES[status])
          for task,status in statuses.iteritems()]))
      now = self._clock.time()
      for task_id, status in statuses.items():
        if status is ScheduleStatus.RUNNING and task_id not in (
          failed_shards.union(running_state_times.keys())):
          running_state_times[task_id] = now
          log.debug('Adding task-%s to running tasks' % task_id)
      if now > expected_running_by:
        non_running_tasks = [id for id in task_ids if id not in running_state_times]
        log.log(debug_if(non_running_tasks == []),
          'Tasks failed to move into running: %s' %
            (non_running_tasks if non_running_tasks != [] else 'None'))
        failed_shards.update(non_running_tasks)
      healthy_tasks.update(id for id in statuses
          if id in running_state_times and now > running_state_times[id] + watch_secs)
      failed_shards.update(id for id in statuses
          if id not in healthy_tasks and statuses[id] not in ACTIVE_STATES)
      if healthy_tasks.union(failed_shards) == set(task_ids):
        return [shard for shard in failed_shards]
      elif now > (start_time + restart_threshold + watch_secs):
        return [shard for shard in set(task_ids).difference(healthy_tasks)]
      self._clock.sleep(app.get_options().mesos_updater_status_check_interval)
