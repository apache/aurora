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

from pystachio import Choice, Empty
from twitter.common import log

from apache.aurora.config.schema.base import (
    BatchUpdateStrategy as PystachioBatchUpdateStrategy,
    QueueUpdateStrategy as PystachioQueueUpdateStrategy,
    VariableBatchUpdateStrategy as PystachioVariableBatchUpdateStrategy
)
from apache.aurora.config.thrift import create_update_strategy_config

from gen.apache.aurora.api.ttypes import JobUpdateSettings, Range


class UpdaterConfig(object):
  MIN_PULSE_INTERVAL_SECONDS = 60

  def __init__(self, config):
    self.batch_size = config.batch_size().get()
    self.watch_secs = config.watch_secs().get()
    self.max_total_failures = config.max_total_failures().get()
    self.max_per_instance_failures = config.max_per_shard_failures().get()
    self.update_strategy = config.update_strategy()
    self.sla_aware = config.sla_aware().get()
    self.wait_for_batch_completion = config.wait_for_batch_completion().get()
    self.rollback_on_failure = config.rollback_on_failure().get()
    self.pulse_interval_secs = None

    # Override default values if they are provided.
    if config.pulse_interval_secs() is not Empty:
      self.pulse_interval_secs = config.pulse_interval_secs().get()

    if self.batch_size <= 0:
      raise ValueError('Batch size should be greater than 0')
    if self.watch_secs < 0:
      raise ValueError('Watch seconds should be greater than or equal to 0')
    if (self.pulse_interval_secs is not None and
            self.pulse_interval_secs < self.MIN_PULSE_INTERVAL_SECONDS):
      raise ValueError('Pulse interval seconds must be at least %s seconds.'
                      % self.MIN_PULSE_INTERVAL_SECONDS)
    if self.wait_for_batch_completion and self.update_strategy is not Empty:
      raise ValueError('Ambiguous update configuration. Cannot combine '
                       'wait_batch_completion with an '
                       'explicit update strategy.')
    if self.batch_size > 1 and self.update_strategy is not Empty:
      raise ValueError('Ambiguous update configuration. Cannot combine '
                       'update strategy with batch size. Please set batch'
                       'size inside of update strategy instead.')

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

    if self.update_strategy is Empty:
      update_strategy = Choice([PystachioQueueUpdateStrategy,
                                PystachioBatchUpdateStrategy,
                                PystachioVariableBatchUpdateStrategy])
      if self.wait_for_batch_completion:
        self.update_strategy = update_strategy(
          PystachioBatchUpdateStrategy(batch_size=self.batch_size))
      else:
        self.update_strategy = update_strategy(
          PystachioQueueUpdateStrategy(batch_size=self.batch_size))
    else:
      unwrapped = self.update_strategy.unwrap()
      if (isinstance(unwrapped, PystachioQueueUpdateStrategy) or
          isinstance(unwrapped, PystachioBatchUpdateStrategy)):
        self.batch_size = self.update_strategy.groupSize
      elif isinstance(unwrapped, PystachioBatchUpdateStrategy):
        self.batch_size = self.update_strategy.groupSizes[0]

    return JobUpdateSettings(
        updateGroupSize=self.batch_size,
        maxPerInstanceFailures=self.max_per_instance_failures,
        maxFailedInstances=self.max_total_failures,
        minWaitInInstanceRunningMs=self.watch_secs * 1000,
        rollbackOnFailure=self.rollback_on_failure,
        waitForBatchCompletion=self.wait_for_batch_completion,
        updateOnlyTheseInstances=self.instances_to_ranges(instances) if instances else None,
        updateStrategy=create_update_strategy_config(self.update_strategy),
        blockIfNoPulsesAfterMs=(self.pulse_interval_secs * 1000 if self.pulse_interval_secs
            else None),
        slaAware=self.sla_aware
    )

  def __eq__(self, other):
    return self.__dict__ == other.__dict__


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
