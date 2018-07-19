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

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.updater.strategy.BatchStrategy;
import org.apache.aurora.scheduler.updater.strategy.QueueStrategy;
import org.apache.aurora.scheduler.updater.strategy.UpdateStrategy;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.aurora.scheduler.updater.Updates.getConfig;

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
          settings.getMinWaitInInstanceRunningMs() >= 0,
          "Min wait in running must be non-negative.");
      checkArgument(
          settings.getUpdateGroupSize() > 0,
          "Update group size must be positive.");

      Set<Integer> currentInstances = expandInstanceIds(instructions.getInitialState());
      Set<Integer> desiredInstances = instructions.isSetDesiredState()
          ? expandInstanceIds(ImmutableSet.of(instructions.getDesiredState()))
          : ImmutableSet.of();

      Set<Integer> instances = ImmutableSet.copyOf(Sets.union(currentInstances, desiredInstances));

      ImmutableMap.Builder<Integer, StateEvaluator<Optional<IScheduledTask>>> evaluators =
          ImmutableMap.builder();
      for (int instanceId : instances) {
        Optional<ITaskConfig> desiredStateConfig;
        if (rollingForward) {
          desiredStateConfig = desiredInstances.contains(instanceId)
              ? Optional.of(instructions.getDesiredState().getTask())
              : Optional.empty();
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

      Ordering<Integer> updateOrdering = new UpdateOrdering(currentInstances, desiredInstances);
      Ordering<Integer> updateOrder = rollingForward
          ? updateOrdering
          : updateOrdering.reverse();

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
  }

  /**
   * An instance ID ordering that prefers to create new instances first, then update existing
   * instances, and finally kill instances.
   */
  @VisibleForTesting
  class UpdateOrdering extends Ordering<Integer> implements Serializable {
    /**
     * Associates an instance ID to an action (create, update, or kill) priority.
     */
    private final ImmutableMap<Integer, Integer> instanceToActionPriority;

    /**
     * Creates an {@link UpdateOrdering}. Determines the action of the instance (create, update, or
     * kill) by comparing the current instance IDs against the desired instance IDs after the
     * update.
     *
     * @param currentInstances The current instance IDs.
     * @param desiredInstances The desired instance IDs after the update.
     */
    UpdateOrdering(Set<Integer> currentInstances, Set<Integer> desiredInstances) {
      requireNonNull(desiredInstances);
      requireNonNull(currentInstances);

      Set<Integer> toCreate = Sets.difference(desiredInstances, currentInstances);
      Set<Integer> toUpdate = Sets.intersection(desiredInstances, currentInstances);
      Set<Integer> toKill = Sets.difference(currentInstances, desiredInstances);

      // Build a mapping of ordering priority (lower is more important) to the instance action
      // group. Then, we invert it for easy lookup of instance ID to priority.
      ImmutableMap.Builder<Integer, Integer> builder = new ImmutableMap.Builder<>();
      ImmutableMap.of(
          1, toCreate,
          2, toUpdate,
          3, toKill
      ).forEach((priority, instances) -> instances.forEach(id -> builder.put(id, priority)));
      this.instanceToActionPriority = builder.build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compare(Integer a, Integer b) {
      Integer aActionPriority = instanceToActionPriority.get(a);
      Integer bActionPriority = instanceToActionPriority.get(b);

      // Try to order by the instance's action.
      if (!aActionPriority.equals(bActionPriority)) {
        return Integer.compare(aActionPriority, bActionPriority);
      }

      // If it is the same action, order the IDs numerically.
      return Integer.compare(a, b);
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
