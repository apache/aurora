package com.twitter.mesos.scheduler;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduledTask;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * A task history pruner that maintains a per-job history goal, while also pruning tasks that
 * have been inactive for a configurable period of time.
 *
 * @author William Farner
 */
interface HistoryPruner extends Function<Set<ScheduledTask>, Set<ScheduledTask>> {

  static class HistoryPrunerImpl implements HistoryPruner {
    private final int perJobHistoryGoal;

    /**
     * Binding annotation for a prune threshold.
     *
     * @author William Farner
     */
    @BindingAnnotation
    @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME)
    public @interface PruneThreshold {}

    private final Predicate<ScheduledTask> inactivePeriodFilter;

    @Inject
    HistoryPrunerImpl(
        final Clock clock,
        @PruneThreshold Amount<Long, Time> inactivePruneThreshold,
        @PruneThreshold int perJobHistoryGoal) {

      this.perJobHistoryGoal = perJobHistoryGoal;

      final long pruneThresholdMillis = inactivePruneThreshold.as(Time.MILLISECONDS);

      inactivePeriodFilter = new Predicate<ScheduledTask>() {
        @Override public boolean apply(ScheduledTask task) {
          // Sanity check to ensure this is not an active task.
          Preconditions.checkArgument(!Tasks.ACTIVE_STATES.contains(task.getStatus()),
              "Pruning filter should not be applied to active tasks.");

          return (clock.nowMillis() - lastEventTimestamp(task)) > pruneThresholdMillis;
        }
      };
    }

    private static long lastEventTimestamp(ScheduledTask task) {
      return Iterables.getLast(task.getTaskEvents()).getTimestamp();
    }

    private static final Ordering<ScheduledTask> LATEST_ACTIVITY_ORDER = Ordering.natural()
        .onResultOf(new Function<ScheduledTask, Long>() {
          @Override public Long apply(ScheduledTask task) {
            return lastEventTimestamp(task);
          }
        });

    @Timed("history_prune")
    @Override
    public Set<ScheduledTask> apply(Set<ScheduledTask> inactiveTasks) {
      ImmutableSet.Builder<ScheduledTask> toPrune = ImmutableSet.builder();

      // Prune tasks from jobs exceeding per-job history goal.
      Multimap<String, ScheduledTask> byJob =
          Multimaps.index(inactiveTasks, Tasks.SCHEDULED_TO_JOB_KEY);
      for (Collection<ScheduledTask> jobTasks : byJob.asMap().values()) {
        if (jobTasks.size() > perJobHistoryGoal) {
          toPrune.addAll(
              LATEST_ACTIVITY_ORDER.leastOf(jobTasks, jobTasks.size() - perJobHistoryGoal));
        }
      }

      return toPrune
          .addAll(ImmutableList.copyOf(Iterables.filter(inactiveTasks, inactivePeriodFilter)))
          .build();
    }
  }
}
