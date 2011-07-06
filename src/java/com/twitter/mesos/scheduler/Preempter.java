package com.twitter.mesos.scheduler;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;

import static com.google.common.base.Preconditions.checkNotNull;

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

  private static final Logger LOG = Logger.getLogger(Preempter.class.getName());

  private static final Query PENDING_QUERY = Query.byStatus(ScheduleStatus.PENDING);
  private static final Query ACTIVE_NOT_PENDING_QUERY = new Query(
      new TaskQuery().setStatuses(Tasks.ACTIVE_STATES),
      Predicates.not(Tasks.hasStatus(ScheduleStatus.PENDING)));

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

  @Override
  public void run() {
    // We are only interested in preempting in favor of pending tasks.
    Iterable<AssignedTask> pendingTasks = Iterables.transform(
        scheduler.getTasks(Query.and(PENDING_QUERY, isIdleTask)), STATE_TO_ASSIGNED);
    if (Iterables.isEmpty(pendingTasks)) {
      return;
    }

    // Only non-pending active tasks may be preempted.
    Iterable<AssignedTask> activeTasks =
        Iterables.transform(scheduler.getTasks(ACTIVE_NOT_PENDING_QUERY), STATE_TO_ASSIGNED);
    if (Iterables.isEmpty(activeTasks)) {
      return;
    }

    // We only preempt tasks that are of lower priority than the max priority of pending tasks.
    // Sort the preemption candidates in the reverse scheduling order.  This ensures we try to evict
    // the lowest priority tasks first.
    int maxPendingPriority =
        Ordering.natural().max(Iterables.transform(pendingTasks, GET_PRIORITY));
    List<AssignedTask> preemptableTasks = Tasks.SCHEDULING_ORDER.reverse().sortedCopy(
        Iterables.filter(activeTasks, lowerPriorityFilter(maxPendingPriority)));
    if (preemptableTasks.isEmpty()) {
      return;
    }

    // The tasks that we preempt in favor of must be of higher priority than the lowest priority
    // preemption candidate.
    int minPreemptablePriority =
        Ordering.natural().min(Iterables.transform(preemptableTasks, GET_PRIORITY));
    List<AssignedTask> preemptingCandidates = Tasks.SCHEDULING_ORDER.sortedCopy(
        Iterables.filter(pendingTasks, greaterPriorityFilter(minPreemptablePriority)));

    // Memoize filters.
    Map<String, Predicate<AssignedTask>> filtersByHost = new MapMaker().makeComputingMap(
        new Function<String, Predicate<AssignedTask>>() {
          @Override public Predicate<AssignedTask> apply(String host) {
            return Predicates.compose(
                schedulingFilter.dynamicHostFilter(scheduler, host), Tasks.ASSIGNED_TO_INFO);
          }
        }
    );

    for (AssignedTask preemptableTask : preemptableTasks) {
      // TODO(William Farner): This doesn't fully work, since the preemption is based solely on
      // the resources reserved for the task running, and does not account for slack resource on
      // the machine.  For example, a machine has 1 CPU available, and is running a low priority
      // task reserving 1 CPU.  If we have a pending high priority task requiring 2 CPUs, it will
      // still not be scheduled.  This implies that a preempter would need to be in the resource
      // offer flow, or that we should make accepting of resource offers asynchronous, so that we
      // operate scheduling and preemption in an independent loop.
      Predicate<AssignedTask> preemptionFilter = Predicates.and(
          preemptionFilter(preemptableTask),
          filtersByHost.get(preemptableTask.getSlaveHost()));
      AssignedTask preempting =
          Iterables.get(Iterables.filter(preemptingCandidates, preemptionFilter), 0, null);
      if (preempting != null) {
        preemptingCandidates.remove(preempting);
        try {
          scheduler.preemptTask(preemptableTask, preempting);
        } catch (ScheduleException e) {
          LOG.log(Level.SEVERE, "Preemption failed", e);
        }
      }
    }
  }

  /**
   * Creates a static filter that will identify tasks that may preempt the provided task
   *
   * @param preemptableTask Task to possibly preempt.
   * @return A filter that will compare the priorities and resources required by other tasks
   *     with {@code preemptableTask}.
   */
  private Predicate<AssignedTask> preemptionFilter(AssignedTask preemptableTask) {
    Predicate<AssignedTask> staticResourceFilter = Predicates.compose(
        schedulingFilter.staticFilter(preemptableTask.getTask(),
            preemptableTask.getSlaveHost()), Tasks.ASSIGNED_TO_INFO);
    Predicate<AssignedTask> priorityFilter =
        greaterPriorityFilter(GET_PRIORITY.apply(preemptableTask));
    return Predicates.and(staticResourceFilter, priorityFilter);
  }

  private final Predicate<ScheduledTask> isIdleTask = new Predicate<ScheduledTask>() {
    @Override public boolean apply(ScheduledTask task) {
      return (clock.nowMillis() - Iterables.getLast(task.getTaskEvents()).getTimestamp()) >=
          preemptionCandidacyDelay.as(Time.MILLISECONDS);
    }
  };

  private static final Function<TaskState, AssignedTask> STATE_TO_ASSIGNED =
      new Function<TaskState, AssignedTask>() {
        @Override public AssignedTask apply(TaskState state) {
          return Tasks.SCHEDULED_TO_ASSIGNED.apply(state.task);
        }
      };

  private static final Function<AssignedTask, Integer> GET_PRIORITY =
      new Function<AssignedTask, Integer>() {
        @Override public Integer apply(AssignedTask task) {
          return task.getTask().getPriority();
        }
      };

  private static Predicate<Integer> lessThan(final int value) {
    return new Predicate<Integer>() {
      @Override public boolean apply(Integer input) {
        return input < value;
      }
    };
  }

  private static Predicate<Integer> greaterThan(final int value) {
    return new Predicate<Integer>() {
      @Override public boolean apply(Integer input) {
        return input > value;
      }
    };
  }

  private static Predicate<AssignedTask> lowerPriorityFilter(int priority) {
    return Predicates.compose(lessThan(priority), GET_PRIORITY);
  }

  private static Predicate<AssignedTask> greaterPriorityFilter(int priority) {
    return Predicates.compose(greaterThan(priority), GET_PRIORITY);
  }
}
