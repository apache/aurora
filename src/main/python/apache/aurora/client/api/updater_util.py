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

import collections
from itertools import groupby
from operator import itemgetter

from twitter.common import log

from gen.apache.aurora.api.ttypes import JobUpdateSettings, Range


class UpdaterConfig(object):
  """
  For updates involving a health check,

  UPDATE INSTANCE                         HEALTHY              REMAIN HEALTHY
  ----------------------------------------|-----------------------|
  \--------------------------------------/ \----------------------/
            restart_thresold                      watch_secs

  When an update is initiated, an instance is expected to be "healthy" before restart_threshold.
  An instance is also expected to remain healthy for at least watch_secs. If these conditions are
  not satisfied, the instance is deemed unhealthy.
  """

  def __init__(self,
               batch_size,
               restart_threshold,
               watch_secs,
               max_per_shard_failures,
               max_total_failures,
               rollback_on_failure=True,
               wait_for_batch_completion=False):

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
    self.max_per_instance_failures = max_per_shard_failures
    self.rollback_on_failure = rollback_on_failure
    self.wait_for_batch_completion = wait_for_batch_completion

  @classmethod
  def instances_to_ranges(cls, instances):
    """Groups instance IDs into a set of contiguous integer ranges.

    Every Range(first, last) represents a closed span of instance IDs. For example,
    :param instances:
    instances=[0,1,2,5,8,9] would result in the following set of ranges:
    (Range(first=0, last=2], Range(first=5, last=5), Range(first=8, last=9))

    Algorithm: http://stackoverflow.com/questions/3149440

    Arguments:
    instances - sorted list of instance IDs.
    """
    if not instances:
      return None

    ranges = set()
    for _, group in groupby(enumerate(instances), lambda(element, position): element - position):
      range_seq = map(itemgetter(1), group)
      ranges.add(Range(first=range_seq[0], last=range_seq[-1]))
    return ranges

  def to_thrift_update_settings(self, instances=None):
    """Converts UpdateConfig into thrift JobUpdateSettings object.

    Arguments:
    instances - optional list of instances to update.
    """
    return JobUpdateSettings(
        updateGroupSize=self.batch_size,
        maxPerInstanceFailures=self.max_per_instance_failures,
        maxFailedInstances=self.max_total_failures,
        maxWaitToInstanceRunningMs=self.restart_threshold*1000,
        minWaitInInstanceRunningMs=self.watch_secs*1000,
        rollbackOnFailure=self.rollback_on_failure,
        updateOnlyTheseInstances=self.instances_to_ranges(instances) if instances else None)


class FailureThreshold(object):
  def __init__(self, max_per_instance_failures, max_total_failures):
    self._max_per_instance_failures = max_per_instance_failures
    self._max_total_failures = max_total_failures
    self._failures_by_instance = collections.defaultdict(int)

  def update_failure_counts(self, failed_instances):
    """Update the failure counts metrics based upon a batch of failed instances.

    Arguments:
    failed_instances - list of failed instances

    Returns a list of instances with failure counts exceeding _max_per_instance_failures.
    """
    exceeded_failure_count_instances = []
    for instance in failed_instances:
      self._failures_by_instance[instance] += 1
      if self._failures_by_instance[instance] > self._max_per_instance_failures:
        exceeded_failure_count_instances.append(instance)

    return exceeded_failure_count_instances

  def is_failed_update(self, log_errors=True):
    total_failed_instances = self._exceeded_instance_fail_count()
    is_failed = total_failed_instances > self._max_total_failures

    if is_failed and log_errors:
      log.error('%s failed instances observed, maximum allowed is %s' % (total_failed_instances,
          self._max_total_failures))
      for instance, failure_count in self._failures_by_instance.items():
        if failure_count > self._max_per_instance_failures:
          log.error('%s instance failures for instance %s, maximum allowed is %s' %
              (failure_count, instance, self._max_per_instance_failures))
    return is_failed

  def _exceeded_instance_fail_count(self):
    """Checks if the per instance failure is greater than a threshold."""
    return sum(count > self._max_per_instance_failures
               for count in self._failures_by_instance.values())
