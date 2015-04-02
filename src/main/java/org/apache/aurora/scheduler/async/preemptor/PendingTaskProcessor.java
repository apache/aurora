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
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;

import org.apache.aurora.scheduler.async.preemptor.PreemptionSlotFinder.PreemptionSlot;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.base.Tasks.SCHEDULED_TO_ASSIGNED;

/**
 * Attempts to find preemption slots for all PENDING tasks eligible for preemption.
 */
class PendingTaskProcessor implements Runnable {
  private final Storage storage;
  private final PreemptionSlotFinder preemptionSlotFinder;
  private final PreemptorMetrics metrics;
  private final Amount<Long, Time> preemptionCandidacyDelay;
  private final PreemptionSlotCache slotCache;
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
  PendingTaskProcessor(
      Storage storage,
      PreemptionSlotFinder preemptionSlotFinder,
      PreemptorMetrics metrics,
      @PreemptionDelay Amount<Long, Time> preemptionCandidacyDelay,
      PreemptionSlotCache slotCache,
      Clock clock) {

    this.storage = requireNonNull(storage);
    this.preemptionSlotFinder = requireNonNull(preemptionSlotFinder);
    this.metrics = requireNonNull(metrics);
    this.preemptionCandidacyDelay = requireNonNull(preemptionCandidacyDelay);
    this.slotCache = requireNonNull(slotCache);
    this.clock = requireNonNull(clock);
  }

  @Override
  public void run() {
    metrics.recordTaskProcessorRun();
    storage.read(new Storage.Work.Quiet<Void>() {
      @Override
      public Void apply(StoreProvider storeProvider) {
        Multimap<IJobKey, IAssignedTask> pendingTasks = fetchIdlePendingTasks(storeProvider);

        for (IJobKey job : pendingTasks.keySet()) {
          AttributeAggregate jobState = AttributeAggregate.getJobActiveState(storeProvider, job);

          for (IAssignedTask pendingTask : pendingTasks.get(job)) {
            ITaskConfig task = pendingTask.getTask();
            metrics.recordPreemptionAttemptFor(task);

            Optional<PreemptionSlot> slot = preemptionSlotFinder.findPreemptionSlotFor(
                pendingTask,
                jobState,
                storeProvider);

            metrics.recordSlotSearchResult(slot, task);

            if (slot.isPresent()) {
              slotCache.add(pendingTask.getTaskId(), slot.get());
            }
          }
        }
        return null;
      }
    });
  }

  private Multimap<IJobKey, IAssignedTask> fetchIdlePendingTasks(StoreProvider store) {
    return Multimaps.index(
        FluentIterable
            .from(store.getTaskStore().fetchTasks(Query.statusScoped(PENDING)))
            .filter(Predicates.and(isIdleTask, Predicates.not(hasCachedSlot)))
            .transform(SCHEDULED_TO_ASSIGNED),
        Tasks.ASSIGNED_TO_JOB_KEY);
  }

  private final Predicate<IScheduledTask> hasCachedSlot = new Predicate<IScheduledTask>() {
    @Override
    public boolean apply(IScheduledTask input) {
      return slotCache.get(input.getAssignedTask().getTaskId()).isPresent();
    }
  };

  private final Predicate<IScheduledTask> isIdleTask = new Predicate<IScheduledTask>() {
    @Override
    public boolean apply(IScheduledTask task) {
      return (clock.nowMillis() - Tasks.getLatestEvent(task).getTimestamp())
          >= preemptionCandidacyDelay.as(Time.MILLISECONDS);
    }
  };
}
