/*
 * Copyright 2013 Twitter, Inc.
 *
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
package com.twitter.aurora.scheduler.async;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.BindingAnnotation;

import com.twitter.aurora.scheduler.ResourceSlot;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.ScheduleException;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.filter.SchedulingFilter;
import com.twitter.aurora.scheduler.state.SchedulerCore;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.entities.IAssignedTask;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import org.apache.mesos.Protos.Offer;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.aurora.gen.ScheduleStatus.PENDING;
import static com.twitter.aurora.gen.ScheduleStatus.PREEMPTING;
import static com.twitter.aurora.scheduler.base.Tasks.SCHEDULED_TO_ASSIGNED;

/**
 * Preempts active tasks in favor of higher priority tasks.
 */
public interface Preemptor {

  /**
   * Preempts active tasks in favor of the input task.
   *
   * @param taskId ID of the preempting task.
   * @return ID of the slave where preemption occured.
   */
  Optional<String> findPreemptionSlotFor(String taskId);

  /**
   * A task preemptor that tries to find tasks that are waiting to be scheduled, which are of higher
   * priority than tasks that are currently running.
   *
   * To avoid excessive churn, the preemptor requires that a task is PENDING for a duration
   * (dictated by {@link #preemptionCandidacyDelay}) before it becomes eligible to preempt other
   * tasks.
   */
  class PreemptorImpl implements Preemptor {

    /**
     * Binding annotation for the time interval after which a pending task becomes eligible to
     * preempt other tasks.
     */
    @BindingAnnotation
    @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
    @interface PreemptionDelay { }

    @VisibleForTesting
    static final Query.Builder CANDIDATE_QUERY = Query.statusScoped(
        EnumSet.copyOf(Sets.difference(Tasks.ACTIVE_STATES, EnumSet.of(PENDING, PREEMPTING))));

    private static final Logger LOG = Logger.getLogger(PreemptorImpl.class.getName());

    private static final Function<IAssignedTask, Integer> GET_PRIORITY =
        new Function<IAssignedTask, Integer>() {
          @Override public Integer apply(IAssignedTask task) {
            return task.getTask().getPriority();
          }
        };

    private final AtomicLong tasksPreempted = Stats.exportLong("preemptor_tasks_preempted");
    private final AtomicLong failedPreemptions = Stats.exportLong("preemptor_failed_preemptions");
    // Incremented every time the preemptor is invoked and finds tasks pending and preemptable tasks
    private final AtomicLong attemptedPreemptions = Stats.exportLong("preemptor_attempts");
    // Incremented every time we fail to find tasks to preempt for a pending task.
    private final AtomicLong noSlotsFound = Stats.exportLong("preemptor_no_slots_found");

    private final Predicate<IScheduledTask> isIdleTask = new Predicate<IScheduledTask>() {
      @Override public boolean apply(IScheduledTask task) {
        return (clock.nowMillis() - Iterables.getLast(task.getTaskEvents()).getTimestamp())
            >= preemptionCandidacyDelay.as(Time.MILLISECONDS);
      }
    };

    private final Storage storage;
    private final SchedulerCore scheduler;
    private final OfferQueue offerQueue;
    private final SchedulingFilter schedulingFilter;
    private final Amount<Long, Time> preemptionCandidacyDelay;
    private final Clock clock;

    /**
     * Creates a new preemptor.
     *
     * @param storage Backing store for tasks.
     * @param scheduler Scheduler to fetch task information from, and instruct when preempting
     *                  tasks.
     * @param offerQueue Queue that contains available Mesos resource offers.
     * @param schedulingFilter Filter to identify whether tasks may reside on given slaves.
     * @param preemptionCandidacyDelay Time a task must be PENDING before it may preempt other
     *                                 tasks.
     * @param clock Clock to check current time.
     */
    @Inject
    PreemptorImpl(
        Storage storage,
        SchedulerCore scheduler,
        OfferQueue offerQueue,
        SchedulingFilter schedulingFilter,
        @PreemptionDelay Amount<Long, Time> preemptionCandidacyDelay,
        Clock clock) {

      this.storage = checkNotNull(storage);
      this.scheduler = checkNotNull(scheduler);
      this.offerQueue = checkNotNull(offerQueue);
      this.schedulingFilter = checkNotNull(schedulingFilter);
      this.preemptionCandidacyDelay = checkNotNull(preemptionCandidacyDelay);
      this.clock = checkNotNull(clock);
    }

    private List<IAssignedTask> fetch(Query.Builder query, Predicate<IScheduledTask> filter) {
      return Lists.newArrayList(Iterables.transform(Iterables.filter(
          Storage.Util.consistentFetchTasks(storage, query), filter),
          SCHEDULED_TO_ASSIGNED));
    }

    private List<IAssignedTask> fetch(Query.Builder query) {
      return fetch(query, Predicates.<IScheduledTask>alwaysTrue());
    }

    private static final Function<IAssignedTask, String> TASK_TO_SLAVE_ID =
        new Function<IAssignedTask, String>() {
          @Override public String apply(IAssignedTask input) {
            return input.getSlaveId();
          }
        };

    private static final Function<IAssignedTask, String> TASK_TO_HOST =
        new Function<IAssignedTask, String>() {
          @Override public String apply(IAssignedTask input) {
            return input.getSlaveHost();
          }
        };

    private static Predicate<IAssignedTask> canPreempt(final IAssignedTask pending) {
      return new Predicate<IAssignedTask>() {
        @Override public boolean apply(IAssignedTask possibleVictim) {
          return preemptionFilter(possibleVictim).apply(pending);
        }
      };
    }

    private static final Function<IAssignedTask, ResourceSlot> TASK_TO_RESOURCES =
        new Function<IAssignedTask, ResourceSlot>() {
          @Override public ResourceSlot apply(IAssignedTask input) {
            return ResourceSlot.from(input.getTask());
          }
        };

    private static final Function<Offer, ResourceSlot> OFFER_TO_RESOURCE_SLOT =
        new Function<Offer, ResourceSlot>() {
          @Override public ResourceSlot apply(Offer offer) {
            return ResourceSlot.from(offer);
          }
        };

    private static final Function<Offer, String> OFFER_TO_HOST =
        new Function<Offer, String>() {
          @Override public String apply(Offer offer) {
            return offer.getHostname();
          }
        };

    // TODO(zmanji) Consider using Dominant Resource Fairness for ordering instead of the vector
    // ordering
    private static final Ordering<IAssignedTask> RESOURCE_ORDER =
        ResourceSlot.ORDER.onResultOf(TASK_TO_RESOURCES).reverse();

    /**
     * Optional.absent indicates that this slave does not have enough resources to satisfy the task.
     * The empty set indicates the offers (slack) are enough.
     * A set with elements indicates those tasks and the offers are enough.
     */
    private Optional<Set<IAssignedTask>> getTasksToPreempt(
        Iterable<IAssignedTask> possibleVictims,
        Iterable<Offer> offers,
        IAssignedTask pendingTask) {

      // This enforces the precondition that all of the resources are from the same host. We need to
      // get the host for the schedulingFilter.
      Set<String> hosts = ImmutableSet.<String>builder()
          .addAll(Iterables.transform(possibleVictims, TASK_TO_HOST))
          .addAll(Iterables.transform(offers, OFFER_TO_HOST)).build();

      String host = Iterables.getOnlyElement(hosts);

      ResourceSlot slackResources =
          ResourceSlot.sum(Iterables.transform(offers, OFFER_TO_RESOURCE_SLOT));

      if (!Iterables.isEmpty(offers)) {
        Set<SchedulingFilter.Veto> vetos = schedulingFilter.filter(
            slackResources,
            host,
            pendingTask.getTask(),
            pendingTask.getTaskId());

        if (vetos.isEmpty()) {
          return Optional.<Set<IAssignedTask>>of(ImmutableSet.<IAssignedTask>of());
        }
      }

      FluentIterable<IAssignedTask> preemptableTasks =
          FluentIterable.from(possibleVictims).filter(canPreempt(pendingTask));

      if (preemptableTasks.isEmpty()) {
        return Optional.absent();
      }

      List<IAssignedTask> toPreemptTasks = Lists.newArrayList();

      Iterable<IAssignedTask> sortedVictims = RESOURCE_ORDER.immutableSortedCopy(preemptableTasks);

      for (IAssignedTask victim : sortedVictims) {
        toPreemptTasks.add(victim);

        ResourceSlot totalResource = ResourceSlot.sum(
            ResourceSlot.sum(Iterables.transform(toPreemptTasks, TASK_TO_RESOURCES)),
            slackResources);

        Set<SchedulingFilter.Veto> vetos = schedulingFilter.filter(
            totalResource,
            host,
            pendingTask.getTask(),
            pendingTask.getTaskId());

        if (vetos.isEmpty()) {
          return Optional.<Set<IAssignedTask>>of(ImmutableSet.copyOf(toPreemptTasks));
        }
      }
      return Optional.absent();
    }

    private static final Function<Offer, String> OFFER_TO_SLAVE_ID =
        new Function<Offer, String>() {
          @Override public String apply(Offer offer) {
            return offer.getSlaveId().getValue();
          }
        };

    private Multimap<String, IAssignedTask> getSlavesToActiveTasks() {
      // Only non-pending active tasks may be preempted.
      List<IAssignedTask> activeTasks = fetch(CANDIDATE_QUERY);

      // Walk through the preemption candidates in reverse scheduling order.
      Collections.sort(activeTasks, Tasks.SCHEDULING_ORDER.reverse());

      // Group the tasks by slave id so they can be paired with offers from the same slave.
      return Multimaps.index(activeTasks, TASK_TO_SLAVE_ID);
    }

    // TODO(zmanji): Add throttling to prevent how much preemption a single task can cause over
    // time.
    // TODO(zmanji): Get the offer queue to associate a slave with a pending task.
    @Override
    public synchronized Optional<String> findPreemptionSlotFor(String taskId) {
      List<IAssignedTask> pendingTasks =
          fetch(Query.statusScoped(PENDING).byId(taskId), isIdleTask);

      // Task is no longer PENDING no need to preempt
      if (pendingTasks.isEmpty()) {
        return Optional.absent();
      }

      IAssignedTask pendingTask = Iterables.getOnlyElement(pendingTasks);

      Multimap<String, IAssignedTask> slavesToActiveTasks = getSlavesToActiveTasks();

      if (slavesToActiveTasks.isEmpty()) {
        return Optional.absent();
      }

      attemptedPreemptions.incrementAndGet();

      // Group the offers by slave id so they can be paired with active tasks from the same slave.
      Multimap<String, Offer> slavesToOffers =
          Multimaps.index(offerQueue.getOffers(), OFFER_TO_SLAVE_ID);

      Set<String> allSlaves = ImmutableSet.<String>builder()
          .addAll(slavesToOffers.keySet())
          .addAll(slavesToActiveTasks.keySet())
          .build();

      for (String slaveID : allSlaves) {
        Optional<Set<IAssignedTask>> toPreemptTasks = getTasksToPreempt(
            slavesToActiveTasks.get(slaveID),
            slavesToOffers.get(slaveID),
            pendingTask);

        if (toPreemptTasks.isPresent()) {
          try {
            for (IAssignedTask toPreempt : toPreemptTasks.get()) {
              scheduler.preemptTask(toPreempt, pendingTask);
              tasksPreempted.incrementAndGet();
            }
            return Optional.of(slaveID);
          } catch (ScheduleException e) {
            LOG.log(Level.SEVERE, "Preemption failed", e);
            failedPreemptions.incrementAndGet();
          }
        } else {
          noSlotsFound.incrementAndGet();
        }
      }

      return Optional.absent();
    }

    private static final Predicate<IAssignedTask> IS_PRODUCTION =
        Predicates.compose(Tasks.IS_PRODUCTION, Tasks.ASSIGNED_TO_INFO);

    /**
     * Creates a static filter that will identify tasks that may preempt the provided task.
     * A task may preempt another task if the following conditions hold true:
     * - The resources reserved for {@code preemptableTask} are sufficient to satisfy the task.
     * - The tasks are owned by the same user and the priority of {@code preemptableTask} is lower
     *     OR {@code preemptableTask} is non-production and the compared task is production.
     *
     * @param preemptableTask Task to possibly preempt.
     * @return A filter that will compare the priorities and resources required by other tasks
     *     with {@code preemptableTask}.
     */
    private static Predicate<IAssignedTask> preemptionFilter(IAssignedTask preemptableTask) {
      Predicate<IAssignedTask> preemptableIsProduction = preemptableTask.getTask().isProduction()
          ? Predicates.<IAssignedTask>alwaysTrue()
          : Predicates.<IAssignedTask>alwaysFalse();

      Predicate<IAssignedTask> priorityFilter =
          greaterPriorityFilter(GET_PRIORITY.apply(preemptableTask));
      return Predicates.or(
          Predicates.and(Predicates.not(preemptableIsProduction), IS_PRODUCTION),
          Predicates.and(isOwnedBy(getRole(preemptableTask)), priorityFilter)
      );
    }

    private static Predicate<IAssignedTask> isOwnedBy(final String role) {
      return new Predicate<IAssignedTask>() {
        @Override public boolean apply(IAssignedTask task) {
          return getRole(task).equals(role);
        }
      };
    }

    private static String getRole(IAssignedTask task) {
      return task.getTask().getOwner().getRole();
    }

    private static Predicate<Integer> greaterThan(final int value) {
      return new Predicate<Integer>() {
        @Override public boolean apply(Integer input) {
          return input > value;
        }
      };
    }

    private static Predicate<IAssignedTask> greaterPriorityFilter(int priority) {
      return Predicates.compose(greaterThan(priority), GET_PRIORITY);
    }
  }
}

