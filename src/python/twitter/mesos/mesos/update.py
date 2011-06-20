from math import ceil
from twitter.common import options
import twitter.common.log
from mesos_twitter.ttypes import *

options.add('--mesos_updater_status_check_inteval',
  dest='mesos_updater_status_check_interval',
  default=3,
  type='int',
  help='How often Mesos runs update loop...')
log = twitter.common.log.get()

class InvalidUpdaterConfigException(Exception):
  pass

class Updater(object):
  """Update the shards of a job in batches."""

  def __init__(self, role, job, scheduler, clock):
    self._role = role
    self._job = job
    self._scheduler = scheduler
    self._clock = clock

  def raise_if_invalid_update_config(self, update_config, shard_count):
    if update_config.batchSize < 1:
      raise InvalidUpdaterConfigException('Batch size should be greater than 0')
    if update_config.restartThreshold < 1:
      raise InvalidUpdaterConfigException('Restart Threshold should be greater than 0')
    if update_config.watchSecs < 1:
      raise InvalidUpdaterConfigException('Watch seconds should be greater than 0')
    if update_config.batchSize > shard_count:
      raise InvalidUpdaterConfigException('Batch size is greater than the total shards present')

  def update(self, update_config):
    """Performs the job update, blocking until it completes.

    Arguments:
    update_config -- update configuration object that describes how the update is performed

    Returns the set of shards that failed to update.
    """
    failed_shards = set()
    job_shards = [shard for shard in self._scheduler.get_shards(self._role, self._job)]
    self.raise_if_invalid_update_config(update_config, len(job_shards))
    batches = int(ceil(len(job_shards) / float(update_config.batchSize)))
    for batch in range(batches):
      batch_start = batch * update_config.batchSize
      batch_end = min((batch + 1) * update_config.batchSize, len(job_shards))
      batch_shards = set(job_shards[batch_start : batch_end])
      task_ids = set(self.restart_tasks(batch_shards))
      failed_shards = failed_shards.union(self.watch_tasks(task_ids, update_config.restartThreshold,
        update_config.watchSecs))
    return failed_shards

  def rollback(self, update_config):
    # TODO(sathya): Implement
    pass

  def restart_tasks(self, shard_ids):
    """Performs a scheduler call for restart.

    Arguments:
    shard_ids -- set of shards to be restarted by the scheduler.

    Returns a map of the current status of the restarted shards as returned by the scheduler.
    """
    log.info('Restarting shards')
    return self._scheduler.restart_tasks(self._role, self._job, shard_ids)

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
    running_state_times = {}
    healthy_tasks = set()
    failed_shards = set()
    while True:
      log.info('Getting Status...')
      statuses = self._scheduler.get_statuses(task_ids)
      log.info('Got statuses: %s' % statuses)
      assert set(statuses.keys()) == task_ids, ('Scheduler status response did not match request:'
          ' asked for %s, got %s' % (task_ids, statuses))
      now = self._clock.time()
      for task_id, status in statuses.items():
        if status is ScheduleStatus.RUNNING and task_id not in (
          failed_shards.union(running_state_times.keys())):
          running_state_times[task_id] = now
          log.info('Adding %s to Running Tasks' % task_id)
      if now > expected_running_by:
        non_running_tasks = [id for id in task_ids if id not in running_state_times]
        log.error('Tasks failed to move into running: %s' % non_running_tasks)
        failed_shards.update(non_running_tasks)
      healthy_tasks.update(id for id in statuses
          if id in running_state_times and now > running_state_times[id] + watch_secs)
      failed_shards.update(id for id in statuses
          if id not in healthy_tasks and statuses[id] not in ACTIVE_STATES)
      if healthy_tasks.union(failed_shards) == set(task_ids):
        return failed_shards
      elif now > (start_time + restart_threshold + watch_secs):
        return task_ids.difference(healthy_tasks)
      self._clock.sleep(options.values().mesos_updater_status_check_interval)