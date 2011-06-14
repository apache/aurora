from math import ceil
from twitter.common import options
import twitter.common.log

options.add('--mesos_updater_status_check_inteval',
  dest='mesos_updater_status_check_interval',
  default=3,
  type='int',
  help='How often Mesos runs update loop...')
log = twitter.common.log.get()

PENDING = 0
ASSIGNED = 1
STARTING = 2
RUNNING = 3
KNOWN_STATES = [PENDING, ASSIGNED, STARTING, RUNNING]

class Updater(object):
  """Update the shards of a job in batches."""

  def __init__(self, role, job, scheduler, clock):
    self._role = role
    self._job = job
    self._scheduler = scheduler
    self._clock = clock

  def update(self, batch_size, restart_threshold, watch_secs):
    """Performs the job update, blocking until it completes.

    Arguments:
    batch_size -- Number of shards to update simultaneously. After a batch is deemed healthy,
                  the update will proceed to the next batch.
                  Must be an integer greater than 0.
    restart_threshold -- Maximum number of seconds before which a task must move to the RUNNING state.
                         Else be considered a failed update.
    watch_secs -- Number of seconds to watch the task once it is RUNNING.
                  And ensure it remains in the RUNNING state, else be considered a failed update.

    Returns the set of shards that failed to update.
    """
    failed_tasks = set()
    job_shards = [shard for shard in self._scheduler.get_shards(self._role, self._job)]
    if batch_size > job_shards:
      log.error('Batch size is greater than the total shards present')
      return
    if batch_size < 0:
      log.error('Batch size specified is less than zero')
      return
    # TODO(sathya): Threshold on batch size
    # Range for restart_threshold and watch_secs - Out of range error
    batches = int(ceil(len(job_shards) / float(batch_size)));
    for batch in range(batches):
      batch_start = batch * batch_size
      batch_end = min((batch + 1) * batch_size, len(job_shards))
      batch_shards = set(job_shards[batch_start : batch_end])
      task_ids = set(self.restart_tasks(batch_shards))
      failed_tasks = failed_tasks.union(self.watch_tasks(task_ids, restart_threshold, watch_secs))
    return failed_tasks

  def rollback(self, batch_size):
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
    restart_threshold -- Maximum number of seconds before which a task must move to the RUNNING state.
                         Else be considered a failed update.
    watch_secs -- Number of seconds to watch the task once it is RUNNING.
                  And ensure it remains in the RUNNING state, else be considered a failed update.

    Returns a set of tasks that failed to meet the following criteria,
    1. Failed to move to RUNNING state before restart_threshold from the time of restart.
    2. Failed to stay in the RUNNING state before watch_secs expire.
    """
    start_time = self._clock.time()
    expected_running_by = start_time + restart_threshold
    running_state_times = {}
    healthy_tasks = set()
    failed_tasks = set()
    while True:
      log.info('Getting Status...')
      statuses = self._scheduler.get_statuses(task_ids)
      log.info('Got statuses: %s' % statuses)
      assert set(statuses.keys()) == task_ids, ('Scheduler status response did not match request:'
          ' asked for %s, got %s' % (task_ids, statuses))
      now = self._clock.time()
      for task_id, status in statuses.items():
        if status is RUNNING and task_id not in failed_tasks.union(running_state_times.keys()):
          running_state_times[task_id] = now
          log.info('Adding %s to Running Tasks' % task_id)
      if now > expected_running_by:
        non_running_tasks = [id for id in task_ids if id not in running_state_times]
        log.error('Tasks failed to move into running: %s' % non_running_tasks)
        failed_tasks.update(non_running_tasks)
      healthy_tasks.update(id for id in statuses
          if id in running_state_times and now > running_state_times[id] + watch_secs)
      failed_tasks.update(id for id in statuses
          if id not in healthy_tasks and statuses[id] not in KNOWN_STATES)
      if healthy_tasks.union(failed_tasks) == set(task_ids):
        return failed_tasks
      elif now > (start_time + restart_threshold + watch_secs):
        return task_ids.difference(healthy_tasks)
      self._clock.sleep(options.values().mesos_updater_status_check_interval)