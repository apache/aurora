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
package org.apache.aurora.scheduler.async.preemptor;

import javax.inject.Inject;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.async.preemptor.PreemptionSlotFinder.PreemptionSlot;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ScheduleStatus.PREEMPTING;

/**
 * Attempts to preempt active tasks in favor of the provided PENDING task in case a preemption
 * slot has been previously found.
 */
public interface Preemptor {
  /**
   * Preempts victim tasks in case a valid preemption slot exists.
   *
   * @param task Preempting task.
   * @param jobState Current job state aggregate.
   * @param storeProvider Store provider to use for task preemption.
   * @return ID of the slave where preemption occurred.
   */
  Optional<String> attemptPreemptionFor(
      IAssignedTask task,
      AttributeAggregate jobState,
      MutableStoreProvider storeProvider);

  class PreemptorImpl implements Preemptor {
    private final StateManager stateManager;
    private final PreemptionSlotFinder preemptionSlotFinder;
    private final PreemptorMetrics metrics;
    private final PreemptionSlotCache slotCache;

    @Inject
    PreemptorImpl(
        StateManager stateManager,
        PreemptionSlotFinder preemptionSlotFinder,
        PreemptorMetrics metrics,
        PreemptionSlotCache slotCache) {

      this.stateManager = requireNonNull(stateManager);
      this.preemptionSlotFinder = requireNonNull(preemptionSlotFinder);
      this.metrics = requireNonNull(metrics);
      this.slotCache = requireNonNull(slotCache);
    }

    @Override
    public Optional<String> attemptPreemptionFor(
        IAssignedTask pendingTask,
        AttributeAggregate jobState,
        MutableStoreProvider storeProvider) {

      final Optional<PreemptionSlot> preemptionSlot = slotCache.get(pendingTask.getTaskId());

      // A preemption slot is available -> attempt to preempt tasks.
      if (preemptionSlot.isPresent()) {
        slotCache.remove(pendingTask.getTaskId());

        // Validate a PreemptionSlot is still valid for the given task.
        Optional<ImmutableSet<PreemptionVictim>> validatedVictims =
            preemptionSlotFinder.validatePreemptionSlotFor(
                pendingTask,
                jobState,
                preemptionSlot.get(),
                storeProvider);

        metrics.recordSlotValidationResult(validatedVictims);
        if (!validatedVictims.isPresent()) {
          // Previously found victims are no longer valid -> let the next run find a new slot.
          return Optional.absent();
        }

        for (PreemptionVictim toPreempt : validatedVictims.get()) {
          metrics.recordTaskPreemption(toPreempt);
          stateManager.changeState(
              storeProvider,
              toPreempt.getTaskId(),
              Optional.<ScheduleStatus>absent(),
              PREEMPTING,
              Optional.of("Preempting in favor of " + pendingTask.getTaskId()));
        }
        return Optional.of(preemptionSlot.get().getSlaveId());
      }

      return Optional.absent();
    }
  }
}
