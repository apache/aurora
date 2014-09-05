/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.updater;

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;

import org.apache.aurora.scheduler.storage.entities.IInstanceTaskConfig;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSettings;
import org.apache.aurora.scheduler.storage.entities.IRange;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.updater.strategy.QueueStrategy;
import org.apache.aurora.scheduler.updater.strategy.UpdateStrategy;

import static java.util.Objects.requireNonNull;

/**
 * A factory that produces one-way job updaters based on a job update configuration.
 */
interface OneWayJobUpdaterFactory {

  /**
   * Creates a one-way job updater that will execute the job update configuration in the direction
   * specified by {@code rollingForward}.
   *
   * @param configuration Configuration to act on.
   * @param rollingForward {@code true} if this is a job update, {@code false} if it is a rollback.
   * @return A one-way updater that will execute the job update as specified in the
   *         {@code configuration}.
   * @throws UpdateConfigurationException If the provided configuration cannot be used.
   */
  OneWayJobUpdater<Integer, Optional<IScheduledTask>> newUpdate(
      IJobUpdateConfiguration configuration,
      boolean rollingForward) throws UpdateConfigurationException;

  /**
   * Thrown when an invalid job update configuration is encountered.
   */
  class UpdateConfigurationException extends Exception {
    UpdateConfigurationException(String msg) {
      super(msg);
    }
  }

  class OneWayJobUpdaterFactoryImpl implements OneWayJobUpdaterFactory {
    private final Clock clock;

    @Inject
    OneWayJobUpdaterFactoryImpl(Clock clock) {
      this.clock = requireNonNull(clock);
    }

    @Override
    public OneWayJobUpdater<Integer, Optional<IScheduledTask>> newUpdate(
        IJobUpdateConfiguration configuration,
        boolean rollingForward) throws UpdateConfigurationException {

      requireNonNull(configuration);

      Set<Integer> instances;
      IJobUpdateSettings settings = configuration.getSettings();
      Range<Integer> updateConfigurationInstances =
          Range.closedOpen(0, configuration.getInstanceCount());
      if (settings.getUpdateOnlyTheseInstances().isEmpty()) {
        Set<Integer> newInstanceIds =
            ImmutableRangeSet.of(updateConfigurationInstances).asSet(DiscreteDomain.integers());

        // In a full job update, the working set is the union of instance IDs before and after.
        instances =  ImmutableSet.copyOf(
            Sets.union(expandInstanceIds(configuration.getOldTaskConfigs()), newInstanceIds));
      } else {
        instances = rangesToInstanceIds(settings.getUpdateOnlyTheseInstances());

        if (!updateConfigurationInstances.containsAll(instances)) {
          throw new UpdateConfigurationException(
              "When updating specific instances, "
                  + "all specified instances must be in the update configuration.");
        }
      }

      ImmutableMap.Builder<Integer, StateEvaluator<Optional<IScheduledTask>>> evaluators =
          ImmutableMap.builder();
      for (int instanceId : instances) {
        Optional<ITaskConfig> desiredState;
        if (rollingForward) {
          desiredState = updateConfigurationInstances.contains(instanceId)
              ? Optional.of(configuration.getNewTaskConfig())
              : Optional.<ITaskConfig>absent();
        } else {
          desiredState = getConfig(instanceId, configuration.getOldTaskConfigs());
        }

        evaluators.put(
            instanceId,
            new InstanceUpdater(
                desiredState,
                settings.getMaxPerInstanceFailures(),
                Amount.of((long) settings.getMinWaitInInstanceRunningMs(), Time.MILLISECONDS),
                Amount.of((long) settings.getMaxWaitToInstanceRunningMs(), Time.MILLISECONDS),
                clock));
      }

      // TODO(wfarner): Add the batch_completion flag to JobUpdateSettings and pick correct
      // strategy.
      UpdateStrategy<Integer> strategy = new QueueStrategy<>(settings.getUpdateGroupSize());

      return new OneWayJobUpdater<>(
          strategy,
          settings.getMaxFailedInstances(),
          evaluators.build());
    }

    private static Range<Integer> toRange(IRange range) {
      return Range.closed(range.getFirst(), range.getLast());
    }

    private static Set<Integer> rangesToInstanceIds(Set<IRange> ranges) {
      ImmutableRangeSet.Builder<Integer> instanceIds = ImmutableRangeSet.builder();
      for (IRange range : ranges) {
        instanceIds.add(toRange(range));
      }

      return instanceIds.build().asSet(DiscreteDomain.integers());
    }

    private static Set<Integer> expandInstanceIds(Set<IInstanceTaskConfig> instanceGroups) {
      ImmutableRangeSet.Builder<Integer> instanceIds = ImmutableRangeSet.builder();
      for (IInstanceTaskConfig group : instanceGroups) {
        for (IRange range : group.getInstances()) {
          instanceIds.add(toRange(range));
        }
      }

      return instanceIds.build().asSet(DiscreteDomain.integers());
    }

    private static Optional<ITaskConfig> getConfig(
        int id,
        Set<IInstanceTaskConfig> instanceGroups) {

      for (IInstanceTaskConfig group : instanceGroups) {
        for (IRange range : group.getInstances()) {
          if (toRange(range).contains(id)) {
            return Optional.of(group.getTask());
          }
        }
      }

      return Optional.absent();
    }
  }
}
