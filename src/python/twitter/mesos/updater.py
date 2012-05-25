import collections
from math import ceil
from gen.twitter.mesos.ttypes import *
from twitter.common import app, log

app.add_option('--mesos_updater_status_check_interval',
  dest='mesos_updater_status_check_interval',
  default=3,
  type='int',
  help='How often Mesos runs update loop.')

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
    if update_config['batchSize'] < 1:
      raise Updater.InvalidConfigError('Batch size should be greater than 0')
    if update_config['restartThreshold'] < 1:
      raise Updater.InvalidConfigError('Restart Threshold should be greater than 0')
    if update_config['watchSecs'] < 1:
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

  def update(self, update_config, initial_shards):
    """Performs the job update, blocking until it completes.
    A rollback will be performed if the update was considered a failure based on the
    update configuration.

    Arguments:
    update_config -- job update configuration object.
    initial_shards -- a list of shards to update.

    Returns the set of shards that failed to update.
    """
    Updater.validate_config(update_config)

    failed_shards = []
    self._max_total_failures = update_config['maxTotalFailures']
    self._max_shard_failures = update_config['maxPerShardFailures']
    remaining_shards = initial_shards[:]

    log.info('Starting job update.')
    while remaining_shards and not self.is_failed_update(failed_shards):
      batch_shards = remaining_shards[0 : update_config['batchSize']]
      remaining_shards = list(set(remaining_shards) - set(batch_shards))
      resp = self.restart_shards(batch_shards)
      log.log(debug_if(resp.responseCode == UpdateResponseCode.OK),
        'Response from scheduler: %s (message: %s)' % (
          UpdateResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
      failed_shards = self.watch_shards(batch_shards, update_config['restartThreshold'],
          update_config['watchSecs'])
      log.log(debug_if(not failed_shards), 'Failed shards: %s' % failed_shards)
      remaining_shards += failed_shards
      remaining_shards.sort()
      self.update_failure_counts(failed_shards)

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
    failed_shards = []
    while shards_to_rollback:
      batch_shards = shards_to_rollback[0 : update_config['batchSize']]
      shards_to_rollback = list(set(shards_to_rollback) - set(batch_shards))
      resp = self._scheduler.rollbackShards(self._role, self._job_name, batch_shards,
          self._update_token, self._session)
      log.log(debug_if(resp.responseCode == UpdateResponseCode.OK),
        'Response from scheduler: %s (message: %s)'
          % (UpdateResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
      failed_shards += self.watch_shards(batch_shards, update_config['restartThreshold'],
          update_config['watchSecs'])

    if failed_shards:
      log.error('Rollback failed for shards: %s' % failed_shards)

  def restart_shards(self, shard_ids):
    """Performs a scheduler call for restart.

    Arguments:
    shard_ids -- set of shards to be restarted by the scheduler.

    Returns a map of the current status of the restarted shards as returned by the scheduler.
    """
    log.info('Restarting shards: %s' % shard_ids)
    return self._scheduler.updateShards(self._role, self._job_name, shard_ids,
        self._update_token, self._session)

  def watch_shards(self, shard_ids, restart_threshold, watch_secs):
    """Monitors the restarted shards.

    Arguments:
    shard_ids -- set of shards to watch.
    restart_threshold -- Maximum number of seconds before which a task must move
                         to the RUNNING state.
    watch_secs -- Number of seconds to watch the task once it is RUNNING.

    Returns a set of tasks that failed to meet the following criteria,
    1. Failed to move to RUNNING state before restart_threshold from the time of restart.
    2. Failed to stay in the RUNNING state before watch_secs expire.
    """
    log.info('Watching shards for %s seconds: %s' % (watch_secs, shard_ids))
    shard_ids = set(shard_ids)
    start_time = self._clock.time()
    expected_running_by = start_time + restart_threshold

    class Shard:
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
        if now > (shard.birthday + watch_secs):
          shard.set_healthy(True)
          log.info('Shard %s is up and healthy' % shard_id)

      log.debug('Shards health: {%s}' % ['%s: %s' % val for val in shards_health.items()])

      # Return if all tasks are finished.
      if set(shards(finished=True)) == shard_ids:
        return [s_id for (s_id, s) in shards_health.items() if not s.healthy]
      # Return if time is up.
      if now > (start_time + restart_threshold + watch_secs):
        return [s_id for s_id in shard_ids if s_id not in shards_health
                                           or not shards_health[s_id].healthy]
      self._clock.sleep(app.get_options().mesos_updater_status_check_interval)
