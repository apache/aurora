from twitter.common import log

from .shard_watcher import ShardWatcher
from .updater_util import FailureThreshold

from gen.twitter.mesos.constants import ACTIVE_STATES
from gen.twitter.mesos.ttypes import ScheduleStatus, ResponseCode


class Restarter(object):
  def __init__(self,
               job_key,
               update_config,
               health_check_interval_seconds,
               scheduler,
               shard_watcher=None):
    self._job_key = job_key
    self._update_config = update_config
    self.health_check_interval_seconds = health_check_interval_seconds
    self._scheduler = scheduler
    self._shard_watcher = shard_watcher or ShardWatcher(
        scheduler,
        job_key.to_thrift(),
        update_config.restart_threshold,
        update_config.watch_secs,
        health_check_interval_seconds)

  def restart(self, shards):
    failure_threshold = FailureThreshold(
        self._update_config.max_per_shard_failures,
        self._update_config.max_total_failures)

    if not shards:
      query = self._job_key.to_thrift_query()
      query.statuses = ACTIVE_STATES
      status = self._scheduler.getTasksStatus(query)

      if status.responseCode != ResponseCode.OK:
        return status

      shards = sorted(task.assignedTask.task.shardId for task in status.tasks)
      if not shards:
        log.info("No shards specified, and no active shards found in job %s. Nothing to do." % self._job_key)
        return status

    log.info("Performing rolling restart of job %s (shards: %s)" % (self._job_key, shards))

    while shards and not failure_threshold.is_failed_update():
      batch = shards[:self._update_config.batch_size]
      shards = shards[self._update_config.batch_size:]

      log.info("Restarting shards: %s", batch)

      # TODO(ksweeney): Remove Nones before resolving MESOS-2403.
      resp = self._scheduler.restartShards(None, None, self._job_key.to_thrift(), batch)
      if resp.responseCode != ResponseCode.OK:
        log.error('Error restarting shards: %s', resp.message)
        return resp

      failed_shards = self._shard_watcher.watch(batch)
      shards += failed_shards
      failure_threshold.update_failure_counts(failed_shards)

    if failure_threshold.is_failed_update():
      log.info("Restart failures threshold reached. Aborting")
    else:
      log.info("All shards were restarted successfully")

    return resp
