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
package org.apache.aurora.scheduler.preemptor;

import java.util.Iterator;

import javax.inject.Inject;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.mesos.v1.Protos.AgentID;

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
    private final OfferManager offerManager;
    private final PreemptionVictimFilter preemptionVictimFilter;
    private final PreemptorMetrics metrics;
    private final BiCache<PreemptionProposal, TaskGroupKey> slotCache;

    @Inject
    PreemptorImpl(
        StateManager stateManager,
        OfferManager offerManager,
        PreemptionVictimFilter preemptionVictimFilter,
        PreemptorMetrics metrics,
        BiCache<PreemptionProposal, TaskGroupKey> slotCache) {

      this.stateManager = requireNonNull(stateManager);
      this.offerManager = requireNonNull(offerManager);
      this.preemptionVictimFilter = requireNonNull(preemptionVictimFilter);
      this.metrics = requireNonNull(metrics);
      this.slotCache = requireNonNull(slotCache);
    }

    @Override
    public Optional<String> attemptPreemptionFor(
        IAssignedTask pendingTask,
        AttributeAggregate jobState,
        MutableStoreProvider store) {

      TaskGroupKey groupKey = TaskGroupKey.from(pendingTask.getTask());
      Iterator<PreemptionProposal> proposalIterator = slotCache.getByValue(groupKey).iterator();

      // A preemption slot is available -> attempt to preempt tasks.
      while (proposalIterator.hasNext()) {
        // Get the next available preemption slot.
        PreemptionProposal slot = proposalIterator.next();
        slotCache.remove(slot, groupKey);

        // Validate PreemptionProposal is still valid for the given task.
        AgentID slaveId = AgentID.newBuilder().setValue(slot.getSlaveId()).build();
        Optional<ImmutableSet<PreemptionVictim>> validatedVictims =
            preemptionVictimFilter.filterPreemptionVictims(
                pendingTask.getTask(),
                slot.getVictims(),
                jobState,
                offerManager.get(slaveId),
                store);

        metrics.recordSlotValidationResult(validatedVictims);
        if (validatedVictims.isPresent()) {
          for (PreemptionVictim toPreempt : validatedVictims.get()) {
            metrics.recordTaskPreemption(toPreempt);
            stateManager.changeState(
                store,
                toPreempt.getTaskId(),
                Optional.absent(),
                PREEMPTING,
                Optional.of("Preempting in favor of " + pendingTask.getTaskId()));
          }
          return Optional.of(slot.getSlaveId());
        }
      }

      return Optional.absent();
    }
  }
}
