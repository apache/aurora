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
import copy
import unittest

from pystachio import Choice
from pytest import raises

from apache.aurora.client.api import UpdaterConfig
from apache.aurora.config.schema.base import (
    BatchUpdateStrategy as PystachioBatchUpdateStrategy,
    QueueUpdateStrategy as PystachioQueueUpdateStrategy,
    UpdateConfig,
    VariableBatchUpdateStrategy as PystachioVariableBatchUpdateStrategy
)

from gen.apache.aurora.api.ttypes import (
    BatchJobUpdateStrategy,
    JobUpdateSettings,
    JobUpdateStrategy,
    QueueJobUpdateStrategy,
    Range,
    VariableBatchJobUpdateStrategy
)


class TestUpdaterUtil(unittest.TestCase):

  EXPECTED_JOB_UPDATE_SETTINGS = JobUpdateSettings(
    blockIfNoPulsesAfterMs=None,
    updateOnlyTheseInstances=None,
    slaAware=False,
    maxPerInstanceFailures=0,
    waitForBatchCompletion=False,
    rollbackOnFailure=True,
    minWaitInInstanceRunningMs=45000,
    updateGroupSize=1,
    maxFailedInstances=0)

  UPDATE_STRATEGIES = Choice([PystachioQueueUpdateStrategy,
                            PystachioBatchUpdateStrategy,
                            PystachioVariableBatchUpdateStrategy])

  def test_multiple_ranges(self):
    """Test multiple ranges."""
    ranges = [repr(e) for e in UpdaterConfig.instances_to_ranges([1, 2, 3, 5, 7, 8])]
    assert 3 == len(ranges), "Wrong number of ranges:%s" % len(ranges)
    assert repr(Range(first=1, last=3)) in ranges, "Missing range [1,3]"
    assert repr(Range(first=5, last=5)) in ranges, "Missing range [5,5]"
    assert repr(Range(first=7, last=8)) in ranges, "Missing range [7,8]"

  def test_one_element(self):
    """Test one ID in the list."""
    ranges = [repr(e) for e in UpdaterConfig.instances_to_ranges([1])]
    assert 1 == len(ranges), "Wrong number of ranges:%s" % len(ranges)
    assert repr(Range(first=1, last=1)) in ranges, "Missing range [1,1]"

  def test_none_list(self):
    """Test None list produces None result."""
    assert UpdaterConfig.instances_to_ranges(None) is None, "Result must be None."

  def test_empty_list(self):
    """Test empty list produces None result."""
    assert UpdaterConfig.instances_to_ranges([]) is None, "Result must be None."

  def test_pulse_interval_secs(self):
    config = UpdaterConfig(
      UpdateConfig(batch_size=1,
                   watch_secs=1,
                   max_per_shard_failures=1,
                   max_total_failures=1,
                   pulse_interval_secs=60))
    assert 60000 == config.to_thrift_update_settings().blockIfNoPulsesAfterMs

  def test_pulse_interval_unset(self):
    config = UpdaterConfig(
      UpdateConfig(batch_size=1, watch_secs=1, max_per_shard_failures=1, max_total_failures=1))
    assert config.to_thrift_update_settings().blockIfNoPulsesAfterMs is None

  def test_pulse_interval_too_low(self):
    threshold = UpdaterConfig.MIN_PULSE_INTERVAL_SECONDS
    with raises(ValueError) as e:
      UpdaterConfig(UpdateConfig(batch_size=1,
                                 watch_secs=1,
                                 max_per_shard_failures=1,
                                 max_total_failures=1,
                                 pulse_interval_secs=threshold - 1))
    assert 'Pulse interval seconds must be at least %s seconds.' % threshold in e.value.message

  def test_to_thrift_update_settings_strategy(self):

    """Test to_thrift produces an expected thrift update settings configuration
       from a Pystachio update object.
    """

    config = UpdaterConfig(
      UpdateConfig(
        update_strategy=self.UPDATE_STRATEGIES(
          PystachioVariableBatchUpdateStrategy(batch_sizes=[1, 2, 3, 4]))))

    thrift_update_config = config.to_thrift_update_settings()

    update_settings = copy.deepcopy(self.EXPECTED_JOB_UPDATE_SETTINGS)

    update_settings.updateStrategy = JobUpdateStrategy(
      batchStrategy=None,
      queueStrategy=None,
      varBatchStrategy=VariableBatchJobUpdateStrategy(groupSizes=(1, 2, 3, 4)))

    assert thrift_update_config == update_settings

  def test_to_thrift_update_settings_no_strategy_queue(self):

    """Test to_thrift produces an expected thrift update settings configuration
       from a Pystachio update object that doesn't include an update strategy.

       The configuration in this test should be converted to a
       QueueJobUpdateStrategy.
    """

    config = UpdaterConfig(UpdateConfig())

    thrift_update_config = config.to_thrift_update_settings()

    update_settings = copy.deepcopy(self.EXPECTED_JOB_UPDATE_SETTINGS)
    update_settings.updateStrategy = JobUpdateStrategy(
        batchStrategy=None,
        queueStrategy=QueueJobUpdateStrategy(groupSize=1),
        varBatchStrategy=None)

    assert thrift_update_config == update_settings

  def test_to_thrift_update_settings_no_strategy_batch(self):

    """Test to_thrift produces an expected thrift update settings configuration
       from a Pystachio update object that doesn't include an update strategy.

       The configuration in this test should be converted to a
       BatchJobUpdateStrategy.
    """

    config = UpdaterConfig(UpdateConfig(wait_for_batch_completion=True))

    thrift_update_config = config.to_thrift_update_settings()

    update_settings = copy.deepcopy(self.EXPECTED_JOB_UPDATE_SETTINGS)
    update_settings.updateStrategy = JobUpdateStrategy(
        batchStrategy=BatchJobUpdateStrategy(groupSize=1),
        queueStrategy=None,
        varBatchStrategy=None)
    update_settings.waitForBatchCompletion = True

    assert thrift_update_config == update_settings

  def test_wait_for_batch_completion_and_update_strategy(self):

    """Test setting wait_for_batch_completion along with an update strategy.
       This combination should result in a fast fail.
    """

    with raises(ValueError) as e:
      UpdaterConfig(UpdateConfig(wait_for_batch_completion=True,
                                 update_strategy=self.UPDATE_STRATEGIES(
                                     PystachioBatchUpdateStrategy(
                                         batch_size=3))))

    assert ('Ambiguous update configuration. Cannot combine '
            'wait_batch_completion with an '
            'explicit update strategy.' in e.value.message)

  def test_batch_size_and_update_strategy(self):

    """Test setting a batch size along with an update strategy.
       This combination should result in a fast fail.
    """

    with raises(ValueError) as e:
      UpdaterConfig(UpdateConfig(batch_size=2,
                                 update_strategy=self.UPDATE_STRATEGIES(
                                     PystachioBatchUpdateStrategy(
                                         batch_size=3))))

    assert ('Ambiguous update configuration. Cannot combine '
            'update strategy with batch size. Please set batch'
            'size inside of update strategy instead.' in e.value.message)
