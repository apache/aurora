import collections

from twitter.common import log

class UpdaterConfig(object):
  """
  For updates involving a health check,

  UPDATE SHARD                         HEALTHY              REMAIN HEALTHY
  ----------------------------------------|-----------------------|
  \--------------------------------------/ \----------------------/
            restart_thresold                      watch_secs

  When an update is initiated, a shard is expected to be "healthy" before restart_threshold. A shard
  is also expected to remain healthy for at least watch_secs. If these conditions are not satisfied,
  the shard is deemed unhealthy.
  """
  def __init__(self,
               batch_size,
               restart_threshold,
               watch_secs,
               max_per_shard_failures,
               max_total_failures):

    if batch_size <= 0:
      raise ValueError('Batch size should be greater than 0')
    if restart_threshold <= 0:
      raise ValueError('Restart Threshold should be greater than 0')
    if watch_secs <= 0:
      raise ValueError('Watch seconds should be greater than 0')
    self.batch_size = batch_size
    self.restart_threshold = restart_threshold
    self.watch_secs = watch_secs
    self.max_total_failures = max_total_failures
    self.max_per_shard_failures = max_per_shard_failures


class FailureThreshold(object):
  def __init__(self, max_per_shard_failures, max_total_failures):
    self._max_per_shard_failures = max_per_shard_failures
    self._max_total_failures = max_total_failures
    self._failures_by_shard = collections.defaultdict(int)


  def update_failure_counts(self, failed_shards):
    """Update the failure counts metrics based upon a batch of failed shards."""
    for shard in failed_shards:
      self._failures_by_shard[shard] += 1

  def is_failed_update(self):
    total_failed_shards = self._exceeded_shard_fail_count()
    is_failed = total_failed_shards > self._max_total_failures

    if is_failed:
      log.error('%s failed shards observed, maximum allowed is %s' % (total_failed_shards,
          self._max_total_failures))
      for shard, failure_count in self._failures_by_shard.items():
        if failure_count > self._max_per_shard_failures:
          log.error('%s shard failures for shard %s, maximum allowed is %s' %
              (failure_count, shard, self._max_per_shard_failures))
    return is_failed

  def _exceeded_shard_fail_count(self):
    """Checks if the per shard failure is greater than a threshold."""
    return sum(count > self._max_per_shard_failures
               for count in self._failures_by_shard.values())
