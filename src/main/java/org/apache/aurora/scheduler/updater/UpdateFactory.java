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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.scheduler.storage.entities.IInstanceTaskConfig;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSettings;
import org.apache.aurora.scheduler.storage.entities.IRange;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.updater.strategy.BatchStrategy;
import org.apache.aurora.scheduler.updater.strategy.QueueStrategy;
import org.apache.aurora.scheduler.updater.strategy.UpdateStrategy;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.aurora.scheduler.base.Numbers.toRange;

/**
 * A factory that produces job updaters based on a job update configuration.
 * <p>
 * TODO(wfarner): Use AssistedInject to inject this (github.com/google/guice/wiki/AssistedInject).
 */
interface UpdateFactory {

  /**
   * Creates a one-way job updater that will execute the job update configuration in the direction
   * specified by {@code rollingForward}.
   *
   * @param configuration Configuration to act on.
   * @param rollingForward {@code true} if this is a job update, {@code false} if it is a rollback.
   * @return An updater that will execute the job update as specified in the
   *         {@code configuration}.
   */
  Update newUpdate(
      IJobUpdateInstructions configuration,
      boolean rollingForward);

  class UpdateFactoryImpl implements UpdateFactory {
    private final Clock clock;

    @Inject
    UpdateFactoryImpl(Clock clock) {
      this.clock = requireNonNull(clock);
    }

    @Override
    public Update newUpdate(IJobUpdateInstructions instructions, boolean rollingForward) {
      requireNonNull(instructions);
      IJobUpdateSettings settings = instructions.getSettings();
      checkArgument(
          settings.getMinWaitInInstanceRunningMs() > 0,
          "Min wait in running must be positive.");
      checkArgument(
          settings.getUpdateGroupSize() > 0,
          "Update group size must be positive.");

      Set<Integer> desiredInstances = instructions.isSetDesiredState()
          ? expandInstanceIds(ImmutableSet.of(instructions.getDesiredState()))
          : ImmutableSet.of();

      Set<Integer> instances = ImmutableSet.copyOf(
          Sets.union(expandInstanceIds(instructions.getInitialState()), desiredInstances));

      ImmutableMap.Builder<Integer, StateEvaluator<Optional<IScheduledTask>>> evaluators =
          ImmutableMap.builder();
      for (int instanceId : instances) {
        Optional<ITaskConfig> desiredStateConfig;
        if (rollingForward) {
          desiredStateConfig = desiredInstances.contains(instanceId)
              ? Optional.of(instructions.getDesiredState().getTask())
              : Optional.absent();
        } else {
          desiredStateConfig = getConfig(instanceId, instructions.getInitialState());
        }

        evaluators.put(
            instanceId,
            new InstanceUpdater(
                desiredStateConfig,
                settings.getMaxPerInstanceFailures(),
                Amount.of((long) settings.getMinWaitInInstanceRunningMs(), Time.MILLISECONDS),
                clock));
      }

      Ordering<Integer> updateOrder = rollingForward
          ? Ordering.natural()
          : Ordering.natural().reverse();

      UpdateStrategy<Integer> strategy = settings.isWaitForBatchCompletion()
          ? new BatchStrategy<>(updateOrder, settings.getUpdateGroupSize())
          : new QueueStrategy<>(updateOrder, settings.getUpdateGroupSize());
      JobUpdateStatus successStatus =
          rollingForward ? JobUpdateStatus.ROLLED_FORWARD : JobUpdateStatus.ROLLED_BACK;
      JobUpdateStatus failureStatus = rollingForward && settings.isRollbackOnFailure()
          ? JobUpdateStatus.ROLLING_BACK
          : JobUpdateStatus.FAILED;

      return new Update(
          new OneWayJobUpdater<>(
              strategy,
              settings.getMaxFailedInstances(),
              evaluators.build()),
          successStatus,
          failureStatus);
    }

    @VisibleForTesting
    static Set<Integer> expandInstanceIds(Set<IInstanceTaskConfig> instanceGroups) {
      return Updates.getInstanceIds(instanceGroups).asSet(DiscreteDomain.integers());
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

  class Update {
    private final OneWayJobUpdater<Integer, Optional<IScheduledTask>> updater;
    private final JobUpdateStatus successStatus;
    private final JobUpdateStatus failureStatus;

    Update(
        OneWayJobUpdater<Integer, Optional<IScheduledTask>> updater,
        JobUpdateStatus successStatus,
        JobUpdateStatus failureStatus) {

      this.updater = requireNonNull(updater);
      this.successStatus = requireNonNull(successStatus);
      this.failureStatus = requireNonNull(failureStatus);
    }

    OneWayJobUpdater<Integer, Optional<IScheduledTask>> getUpdater() {
      return updater;
    }

    JobUpdateStatus getSuccessStatus() {
      return successStatus;
    }

    JobUpdateStatus getFailureStatus() {
      return failureStatus;
    }
  }
}
