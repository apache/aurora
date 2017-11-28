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

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.offers.OfferManager;
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

/**
 * Attempts to find preemption slots for all PENDING tasks eligible for preemption.
 */
@VisibleForTesting
public class PendingTaskProcessor implements Runnable {
  private final Storage storage;
  private final OfferManager offerManager;
  private final PreemptionVictimFilter preemptionVictimFilter;
  private final PreemptorMetrics metrics;
  private final Amount<Long, Time> preemptionCandidacyDelay;
  private final BiCache<PreemptionProposal, TaskGroupKey> slotCache;
  private final ClusterState clusterState;
  private final Clock clock;
  private final Integer reservationBatchSize;

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

  /**
   * Binding annotation for the maximum number of reservations for a task group to be processed in
   * a batch. Performing more reservations per task group improves preemption performance at the
   * cost of reduced preemption fairness.
   */
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface ReservationBatchSize { }

  @Inject
  PendingTaskProcessor(
      Storage storage,
      OfferManager offerManager,
      PreemptionVictimFilter preemptionVictimFilter,
      PreemptorMetrics metrics,
      @PreemptionDelay Amount<Long, Time> preemptionCandidacyDelay,
      BiCache<PreemptionProposal, TaskGroupKey> slotCache,
      ClusterState clusterState,
      Clock clock,
      @ReservationBatchSize Integer reservationBatchSize) {

    this.storage = requireNonNull(storage);
    this.offerManager = requireNonNull(offerManager);
    this.preemptionVictimFilter = requireNonNull(preemptionVictimFilter);
    this.metrics = requireNonNull(metrics);
    this.preemptionCandidacyDelay = requireNonNull(preemptionCandidacyDelay);
    this.slotCache = requireNonNull(slotCache);
    this.clusterState = requireNonNull(clusterState);
    this.clock = requireNonNull(clock);
    this.reservationBatchSize = requireNonNull(reservationBatchSize);
  }

  @Timed("pending_task_processor_run")
  @Override
  public void run() {
    metrics.recordTaskProcessorRun();
    storage.read(store -> {
      Multimap<String, PreemptionVictim> slavesToActiveTasks =
          clusterState.getSlavesToActiveTasks();

      if (slavesToActiveTasks.isEmpty()) {
        // No preemption victims to consider.
        return null;
      }

      // Group the offers by slave id so they can be paired with active tasks from the same slave.
      Map<String, HostOffer> slavesToOffers =
          Maps.uniqueIndex(offerManager.getAll(), OFFER_TO_SLAVE_ID);

      Set<String> allSlaves = Sets.newHashSet(Iterables.concat(
          slavesToOffers.keySet(),
          slavesToActiveTasks.keySet()));

      // The algorithm below attempts to find a reservation for every task group by matching
      // it against all available slaves until a preemption slot is found. Groups are evaluated
      // in a round-robin fashion to ensure fairness (e.g.: G1, G2, G3, G1, G2).
      // A slave is removed from further matching once a reservation is made. Similarly, all
      // identical task group instances are removed from further iteration if none of the
      // available slaves could yield a preemption proposal. A consuming iterator is used for
      // task groups to ensure iteration order is preserved after a task group is removed.
      LoadingCache<IJobKey, AttributeAggregate> jobStates = attributeCache(store);
      List<TaskGroupKey> pendingGroups = fetchIdlePendingGroups(store);
      Iterator<TaskGroupKey> groups = Iterators.consumingIterator(pendingGroups.iterator());
      TaskGroupKey lastGroup = null;
      Iterator<String> slaveIterator = allSlaves.iterator();

      while (!pendingGroups.isEmpty()) {
        boolean matched = false;
        TaskGroupKey group = groups.next();
        ITaskConfig task = group.getTask();

        metrics.recordPreemptionAttemptFor(task);
        // start over only if a different task group is being processed
        if (!group.equals(lastGroup)) {
          slaveIterator = allSlaves.iterator();
        }
        while (slaveIterator.hasNext()) {
          String slaveId = slaveIterator.next();
          Optional<ImmutableSet<PreemptionVictim>> candidates =
              preemptionVictimFilter.filterPreemptionVictims(
                  task,
                  slavesToActiveTasks.get(slaveId),
                  jobStates.getUnchecked(task.getJob()),
                  Optional.fromNullable(slavesToOffers.get(slaveId)),
                  store);

          metrics.recordSlotSearchResult(candidates, task);
          if (candidates.isPresent()) {
            // Slot found -> remove slave to avoid multiple task reservations.
            slaveIterator.remove();
            slotCache.put(new PreemptionProposal(candidates.get(), slaveId), group);
            matched = true;
            break;
          }
        }
        if (!matched) {
          // No slot found for the group -> remove group and reset group iterator.
          pendingGroups.removeAll(ImmutableSet.of(group));
          groups = Iterators.consumingIterator(pendingGroups.iterator());
          metrics.recordUnmatchedTask();
        }
        lastGroup = group;
      }
      return null;
    });
  }

  private List<TaskGroupKey> fetchIdlePendingGroups(StoreProvider store) {
    Multiset<TaskGroupKey> taskGroupCounts = HashMultiset.create(
        FluentIterable.from(store.getTaskStore().fetchTasks(Query.statusScoped(PENDING)))
            .filter(Predicates.and(isIdleTask, Predicates.not(hasCachedSlot)))
            .transform(Functions.compose(ASSIGNED_TO_GROUP_KEY, IScheduledTask::getAssignedTask)));

    return getPreemptionSequence(taskGroupCounts, reservationBatchSize);
  }

  /**
   * Creates execution sequence for pending task groups by interleaving batches of requested size of
   * their occurrences. For example: {G1, G1, G1, G2, G2} with batch size of 2 task per group will
   * be converted into {G1, G1, G2, G2, G1}.
   *
   * @param groups Multiset of task groups.
   * @param batchSize The batch size of tasks from each group to sequence together.
   * @return A task group execution sequence.
   */
  @VisibleForTesting
  static List<TaskGroupKey> getPreemptionSequence(
      Multiset<TaskGroupKey> groups,
      int batchSize) {

    Preconditions.checkArgument(batchSize > 0, "batchSize should be positive.");

    Multiset<TaskGroupKey> mutableGroups = HashMultiset.create(groups);
    List<TaskGroupKey> instructions = Lists.newLinkedList();
    Set<TaskGroupKey> keys = ImmutableSet.copyOf(groups.elementSet());
    while (!mutableGroups.isEmpty()) {
      for (TaskGroupKey key : keys) {
        if (mutableGroups.contains(key)) {
          int elementCount = mutableGroups.remove(key, batchSize);
          int removedCount = Math.min(elementCount, batchSize);
          instructions.addAll(Collections.nCopies(removedCount, key));
        }
      }
    }

    return instructions;
  }

  private LoadingCache<IJobKey, AttributeAggregate> attributeCache(final StoreProvider store) {
    return CacheBuilder.newBuilder().build(CacheLoader.from(
        new Function<IJobKey, AttributeAggregate>() {
          @Override
          public AttributeAggregate apply(IJobKey job) {
            return AttributeAggregate.getJobActiveState(store, job);
          }
        }));
  }

  private static final Function<IAssignedTask, TaskGroupKey> ASSIGNED_TO_GROUP_KEY =
      task -> TaskGroupKey.from(task.getTask());

  private final Predicate<IScheduledTask> hasCachedSlot = new Predicate<IScheduledTask>() {
    @Override
    public boolean apply(IScheduledTask task) {
      return !slotCache.getByValue(TaskGroupKey.from(task.getAssignedTask().getTask())).isEmpty();
    }
  };

  private final Predicate<IScheduledTask> isIdleTask = new Predicate<IScheduledTask>() {
    @Override
    public boolean apply(IScheduledTask task) {
      return (clock.nowMillis() - Tasks.getLatestEvent(task).getTimestamp())
          >= preemptionCandidacyDelay.as(Time.MILLISECONDS);
    }
  };

  private static final Function<HostOffer, String> OFFER_TO_SLAVE_ID =
      offer -> offer.getOffer().getAgentId().getValue();
}
