package com.twitter.mesos.scheduler.periodic;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.Resources;
import com.twitter.mesos.scheduler.ScheduleException;
import com.twitter.mesos.scheduler.SchedulerCore;
import com.twitter.mesos.scheduler.SchedulingFilter;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.mesos.Tasks.SCHEDULED_TO_ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;

/**
 * A task preempter that tries to find tasks that are waiting to be scheduled, which are of higher
 * priority than tasks that are currently running.
 *
 * To avoid excessive churn, the preempter requires that a task is PENDING for a duration (dictated
 * by {@link #preemptionCandidacyDelay}) before it becomes eligible to preempt other tasks.
 *
 * @author William Farner
 */
class Preempter implements Runnable {

  @VisibleForTesting
  static final TaskQuery PENDING_QUERY = Query.byStatus(PENDING);

  @VisibleForTesting
  static final TaskQuery ACTIVE_NOT_PENDING_QUERY = new TaskQuery()
      .setStatuses(ImmutableSet.copyOf(Sets.difference(Tasks.ACTIVE_STATES, EnumSet.of(PENDING))));

  private static final Logger LOG = Logger.getLogger(Preempter.class.getName());

  private static final Predicate<AssignedTask> IS_PRODUCTION = new Predicate<AssignedTask>() {
    @Override public boolean apply(AssignedTask task) {
      return task.getTask().isProduction();
    }
  };

  private static final Function<AssignedTask, Integer> GET_PRIORITY =
      new Function<AssignedTask, Integer>() {
        @Override public Integer apply(AssignedTask task) {
          return task.getTask().getPriority();
        }
      };

  private final Predicate<ScheduledTask> isIdleTask = new Predicate<ScheduledTask>() {
    @Override public boolean apply(ScheduledTask task) {
      return (clock.nowMillis() - Iterables.getLast(task.getTaskEvents()).getTimestamp())
          >= preemptionCandidacyDelay.as(Time.MILLISECONDS);
    }
  };

  private final SchedulerCore scheduler;
  private final SchedulingFilter schedulingFilter;
  private final Amount<Long, Time> preemptionCandidacyDelay;
  private final Clock clock;

  /**
   * Creates a new preempter.
   *
   * @param scheduler Scheduler to fetch task information from, and instruct when preempting tasks.
   * @param schedulingFilter Filter to identify whether tasks may reside on given host machines.
   * @param preemptionCandidacyDelay Time a task must be PENDING before it may preempt other tasks.
   * @param clock Clock to check current time.
   */
  @Inject
  Preempter(SchedulerCore scheduler, SchedulingFilter schedulingFilter,
      Amount<Long, Time> preemptionCandidacyDelay, Clock clock) {
    this.scheduler = checkNotNull(scheduler);
    this.schedulingFilter = checkNotNull(schedulingFilter);
    this.preemptionCandidacyDelay = checkNotNull(preemptionCandidacyDelay);
    this.clock = checkNotNull(clock);
  }

  private List<AssignedTask> fetch(TaskQuery query, Predicate<ScheduledTask> filter) {
    return Lists.newArrayList(Iterables.transform(
        Iterables.filter(scheduler.getTasks(query), filter), SCHEDULED_TO_ASSIGNED));
  }

  private List<AssignedTask> fetch(TaskQuery query) {
    return fetch(query, Predicates.<ScheduledTask>alwaysTrue());
  }

  @Override
  public void run() {
    // We are only interested in preempting in favor of pending tasks.
    List<AssignedTask> pendingTasks = fetch(PENDING_QUERY, isIdleTask);
    if (pendingTasks.isEmpty()) {
      return;
    }

    // Only non-pending active tasks may be preempted.
    List<AssignedTask> activeTasks = fetch(ACTIVE_NOT_PENDING_QUERY);
    if (activeTasks.isEmpty()) {
      return;
    }

    // Arrange the pending tasks in scheduling order.
    Collections.sort(pendingTasks, Tasks.SCHEDULING_ORDER);

    // Walk through the preemption candidates in reverse scheduling order.
    Collections.sort(activeTasks, Tasks.SCHEDULING_ORDER.reverse());

    for (AssignedTask preemptableTask : activeTasks) {
      // TODO(William Farner): This doesn't fully work, since the preemption is based solely on
      // the resources reserved for the task running, and does not account for slack resource on
      // the machine.  For example, a machine has 1 CPU available, and is running a low priority
      // task reserving 1 CPU.  If we have a pending high priority task requiring 2 CPUs, it will
      // still not be scheduled.  This implies that a preempter would need to be in the resource
      // offer flow, or that we should make accepting of resource offers asynchronous, so that we
      // operate scheduling and preemption in an independent loop.
      Predicate<AssignedTask> preemptionFilter = preemptionFilter(preemptableTask);

      Resources slot = Resources.from(preemptableTask.getTask());
      String host = preemptableTask.getSlaveHost();

      AssignedTask preempting = null;
      for (AssignedTask task : Iterables.filter(pendingTasks, preemptionFilter)) {
        if (schedulingFilter.filter(slot, Optional.of(host), task.getTask()).isEmpty()) {
          preempting = task;
          break;
        }
      }

      if (preempting != null) {
        pendingTasks.remove(preempting);
        try {
          scheduler.preemptTask(preemptableTask, preempting);
        } catch (ScheduleException e) {
          LOG.log(Level.SEVERE, "Preemption failed", e);
        }
      }
    }
  }

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
  private Predicate<AssignedTask> preemptionFilter(AssignedTask preemptableTask) {
    Predicate<AssignedTask> preemptableIsProduction = preemptableTask.getTask().isProduction()
        ? Predicates.<AssignedTask>alwaysTrue()
        : Predicates.<AssignedTask>alwaysFalse();

    Predicate<AssignedTask> priorityFilter =
        greaterPriorityFilter(GET_PRIORITY.apply(preemptableTask));
    return Predicates.or(
        Predicates.and(Predicates.not(preemptableIsProduction), IS_PRODUCTION),
        Predicates.and(isOwnedBy(getRole(preemptableTask)), priorityFilter)
    );
  }

  private Predicate<AssignedTask> isOwnedBy(final String role) {
    return new Predicate<AssignedTask>() {
      @Override public boolean apply(AssignedTask task) {
        return getRole(task).equals(role);
      }
    };
  }

  private static String getRole(AssignedTask task) {
    return task.getTask().getOwner().getRole();
  }

  private static Predicate<Integer> greaterThan(final int value) {
    return new Predicate<Integer>() {
      @Override public boolean apply(Integer input) {
        return input > value;
      }
    };
  }

  private static Predicate<AssignedTask> greaterPriorityFilter(int priority) {
    return Predicates.compose(greaterThan(priority), GET_PRIORITY);
  }
}
