package com.twitter.mesos.scheduler.async;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.eventbus.Subscribe;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.StateManager;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.mesos.scheduler.events.PubsubEvent.EventSubscriber;
import static com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;
import static com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import static com.twitter.mesos.scheduler.events.PubsubEvent.TasksDeleted;

/**
 * Prunes tasks in a job based on per-job history and an inactive time threshold by observing tasks
 * transitioning into one of the inactive states.
 */
public class HistoryPruner implements EventSubscriber {
  private static final Logger LOG = Logger.getLogger(HistoryPruner.class.getName());

  private static final Ordering<ScheduledTask> LATEST_ACTIVITY = Ordering.natural()
      .onResultOf(new Function<ScheduledTask, Long>() {
        @Override public Long apply(ScheduledTask task) {
          return Iterables.getLast(task.getTaskEvents()).getTimestamp();
        }
      });

  @VisibleForTesting
  static final TaskQuery INACTIVE_QUERY = new TaskQuery().setStatuses(Tasks.TERMINAL_STATES);

  @VisibleForTesting
  final Multimap<String, String> tasksByJob = LinkedHashMultimap.create();

  private final ScheduledExecutorService executor;
  private final StateManager stateManager;
  private final Clock clock;
  private final long pruneThresholdMillis;
  private final int perJobHistoryGoal;
  private final Map<String, Future<?>> futures = Maps.newHashMap();

  @BindingAnnotation
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface PruneThreshold { }

  @Inject
  HistoryPruner(
      ScheduledExecutorService executor,
      final StateManager stateManager,
      final Clock clock,
      @PruneThreshold Amount<Long, Time> inactivePruneThreshold,
      @PruneThreshold int perJobHistoryGoal) {

    this.executor = checkNotNull(executor);
    this.stateManager = checkNotNull(stateManager);
    this.clock = checkNotNull(clock);
    this.pruneThresholdMillis = inactivePruneThreshold.as(Time.MILLISECONDS);
    this.perJobHistoryGoal = checkNotNull(perJobHistoryGoal);
  }

  /**
   * When triggered, records an inactive task state change.
   *
   * @param change Event when a task changes state.
   */
  @Subscribe
  public synchronized void recordStateChange(TaskStateChange change) {
    if (Tasks.isTerminated(change.getNewState())) {
      registerInactiveTask(
          Tasks.jobKey(change.getTask()), change.getTaskId(), calculateTimeout(clock.nowMillis()));
    }
  }

  /**
   * When triggered, iterates through inactive tasks in the system and prunes tasks that
   * exceed the history goal for a job or are beyond the time threshold.
   *
   * @param event A new StorageStarted event.
   */
  @Subscribe
  public synchronized void storageStarted(StorageStarted event) {
    for (ScheduledTask task : LATEST_ACTIVITY.sortedCopy(stateManager.fetchTasks(INACTIVE_QUERY))) {
      registerInactiveTask(
          Tasks.jobKey(task),
          Tasks.id(task),
          calculateTimeout(Iterables.getLast(task.getTaskEvents()).getTimestamp()));
    }
  }

  private synchronized void delete(Set<ScheduledTask> tasks) {
    for (ScheduledTask task : tasks) {
      String id = Tasks.id(task);
      tasksByJob.remove(Tasks.jobKey(task), id);
      Future<?> future = futures.remove(id);
      if (future != null) {
        future.cancel(false);
      }
    }
  }

  /**
   * When triggered, removes the tasks scheduled for pruning and cancels any existing future.
   *
   * @param event A new TasksDeleted event.
   */
  @Subscribe
  public void tasksDeleted(final TasksDeleted event) {
    // This is 'eventually consistent', which is deemed to be okay in this case.
    executor.submit(new Runnable() {
      @Override public void run() {
        delete(event.getTasks());
      }
    });
  }

  @VisibleForTesting
  long calculateTimeout(long taskEventTimestampMillis) {
    return pruneThresholdMillis - Math.max(0, clock.nowMillis() - taskEventTimestampMillis);
  }

  private void registerInactiveTask(
      final String jobKey,
      final String taskId,
      long timeoutRemaining) {

    LOG.fine("Prune task " + taskId + " in " + timeoutRemaining + " ms.");
    // Insert the latest inactive task at the tail.
    tasksByJob.put(jobKey, taskId);
    Runnable runnable = new Runnable() {
      @Override public void run() {
        pruneTask(jobKey, taskId);
      }
    };
    futures.put(taskId, executor.schedule(runnable, timeoutRemaining, TimeUnit.MILLISECONDS));

    Collection<String> tasks = tasksByJob.get(jobKey);
    while (tasks.size() > perJobHistoryGoal) {
      // Pick oldest task from the head. Guaranteed by LinkedHashMultimap based on insertion order.
      String pruneTaskId = tasks.iterator().next();
      tasks.remove(pruneTaskId);
      try {
        LOG.fine("Pruning inactive task " + pruneTaskId);
        stateManager.deleteTasks(ImmutableSet.of(pruneTaskId));
      } finally {
        Future<?> future = futures.remove(pruneTaskId);
        if (future != null) {
          future.cancel(false);
        }
      }
    }
  }

  private synchronized void pruneTask(String jobKey, String taskId) {
    try {
      LOG.info("Pruning expired inactive task " + taskId);
      stateManager.deleteTasks(ImmutableSet.of(taskId));
    } finally {
      tasksByJob.remove(jobKey, taskId);
      futures.remove(taskId);
    }
  }
}
