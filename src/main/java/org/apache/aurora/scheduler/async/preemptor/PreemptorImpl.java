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

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.async.preemptor.PreemptionSlotFinder.PreemptionSlot;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.PREEMPTING;
import static org.apache.aurora.scheduler.base.Tasks.SCHEDULED_TO_ASSIGNED;

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
  private final Amount<Long, Time> preemptionCandidacyDelay;
  private final Clock clock;

  /**
   * Binding annotation for the time interval after which a pending task becomes eligible to
   * preempt other tasks. To avoid excessive churn, the preemptor requires that a task is PENDING
   * for a duration (dictated by {@link #preemptionCandidacyDelay}) before it becomes eligible
   * to preempt other tasks.
   */
  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface PreemptionDelay { }

  @Inject
  PreemptorImpl(
      Storage storage,
      StateManager stateManager,
      PreemptionSlotFinder preemptionSlotFinder,
      PreemptorMetrics metrics,
      @PreemptionDelay Amount<Long, Time> preemptionCandidacyDelay,
      Clock clock) {

    this.storage = requireNonNull(storage);
    this.stateManager = requireNonNull(stateManager);
    this.preemptionSlotFinder = requireNonNull(preemptionSlotFinder);
    this.metrics = requireNonNull(metrics);
    this.preemptionCandidacyDelay = requireNonNull(preemptionCandidacyDelay);
    this.clock = requireNonNull(clock);
  }

  @Override
  public synchronized Optional<String> attemptPreemptionFor(
      final String taskId,
      final AttributeAggregate attributeAggregate) {

    return storage.write(new Storage.MutateWork.Quiet<Optional<String>>() {
      @Override
      public Optional<String> apply(Storage.MutableStoreProvider storeProvider) {
        final Optional<IAssignedTask> pendingTask = fetchIdlePendingTask(taskId, storeProvider);

        // Task is no longer PENDING no need to preempt.
        if (!pendingTask.isPresent()) {
          return Optional.absent();
        }

        // TODO(maxim): Move preemption slot search into a read-only transaction and validate
        //              weakly-consistent slot data before making a preemption.
        Optional<PreemptionSlot> preemptionSlot = preemptionSlotFinder.findPreemptionSlotFor(
            pendingTask.get(),
            attributeAggregate,
            storeProvider);

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

  private Optional<IAssignedTask> fetchIdlePendingTask(String taskId, Storage.StoreProvider store) {
    Query.Builder query = Query.taskScoped(taskId).byStatus(PENDING);
    Iterable<IAssignedTask> result = FluentIterable
        .from(store.getTaskStore().fetchTasks(query))
        .filter(isIdleTask)
        .transform(SCHEDULED_TO_ASSIGNED);
    return Optional.fromNullable(Iterables.getOnlyElement(result, null));
  }

  private final Predicate<IScheduledTask> isIdleTask = new Predicate<IScheduledTask>() {
    @Override
    public boolean apply(IScheduledTask task) {
      return (clock.nowMillis() - Tasks.getLatestEvent(task).getTimestamp())
          >= preemptionCandidacyDelay.as(Time.MILLISECONDS);
    }
  };
}
