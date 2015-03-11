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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.async.preemptor.PreemptionSlotFinder.PreemptionSlot;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ScheduleStatus.PREEMPTING;

/**
 * Coordinates preemption slot search for a PENDING tasks and triggers preemption if such
 * slot is found.
 */
@VisibleForTesting
public class PreemptorImpl implements Preemptor {

  private final Storage storage;
  private final StateManager stateManager;
  private final PreemptionSlotFinder preemptionSlotFinder;
  private final PreemptorMetrics metrics;

  @Inject
  PreemptorImpl(
      Storage storage,
      StateManager stateManager,
      PreemptionSlotFinder preemptionSlotFinder,
      PreemptorMetrics metrics) {

    this.storage = requireNonNull(storage);
    this.stateManager = requireNonNull(stateManager);
    this.preemptionSlotFinder = requireNonNull(preemptionSlotFinder);
    this.metrics = requireNonNull(metrics);
  }

  @Override
  public synchronized Optional<String> attemptPreemptionFor(
      final String taskId,
      final AttributeAggregate attributeAggregate) {

    return storage.write(new Storage.MutateWork.Quiet<Optional<String>>() {
      @Override
      public Optional<String> apply(Storage.MutableStoreProvider storeProvider) {
        // TODO(maxim): Move preemption slot search into a read-only transaction and validate
        //              weakly-consistent slot data before making a preemption.
        Optional<PreemptionSlot> preemptionSlot =
            preemptionSlotFinder.findPreemptionSlotFor(taskId, attributeAggregate, storeProvider);

        if (preemptionSlot.isPresent()) {
          for (PreemptionVictim toPreempt : preemptionSlot.get().getVictims()) {
            metrics.recordTaskPreemption(toPreempt);
            stateManager.changeState(
                storeProvider,
                toPreempt.getTaskId(),
                Optional.<ScheduleStatus>absent(),
                PREEMPTING,
                Optional.of("Preempting in favor of " + taskId));
          }
          return Optional.of(preemptionSlot.get().getSlaveId());
        }
        return Optional.absent();
      }
    });
  }
}
