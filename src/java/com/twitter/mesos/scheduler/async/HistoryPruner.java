package com.twitter.mesos.scheduler.async;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.eventbus.Subscribe;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;

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
  private final Clock clock;
  private final long pruneThresholdMillis;
  private final int perJobHistoryGoal;
  private final Map<String, Future<?>> taskIdToFuture = Maps.newHashMap();
  private final Closure<Set<String>> taskDeleter;
  private final Supplier<Set<ScheduledTask>> inactiveTasksSupplier;

  @BindingAnnotation
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface PruneThreshold { }

  @Inject
  HistoryPruner(
      final ScheduledExecutorService executor,
      final Storage storage,
      final Clock clock,
      @PruneThreshold Amount<Long, Time> inactivePruneThreshold,
      @PruneThreshold int perJobHistoryGoal) {

    this.executor = checkNotNull(executor);
    this.clock = checkNotNull(clock);
    this.pruneThresholdMillis = inactivePruneThreshold.as(Time.MILLISECONDS);
    this.perJobHistoryGoal = checkNotNull(perJobHistoryGoal);
    this.taskDeleter = new Closure<Set<String>>() {
      @Override public void execute(final Set<String> taskIds) {
        executor.submit(new Runnable() {
          @Override public void run() {
            LOG.info("Pruning inactive tasks " + taskIds);
            storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
              @Override protected void execute(MutableStoreProvider storeProvider) {
                storeProvider.getTaskStore().deleteTasks(taskIds);
              }
            });
          }
        });
      }
    };

    this.inactiveTasksSupplier = new Supplier<Set<ScheduledTask>>() {
      @Override public Set<ScheduledTask> get() {
        return Storage.Util.fetchTasks(storage, INACTIVE_QUERY);
      }
    };
  }

  @VisibleForTesting
  long calculateTimeout(long taskEventTimestampMillis) {
    return pruneThresholdMillis - Math.max(0, clock.nowMillis() - taskEventTimestampMillis);
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
    for (ScheduledTask task : LATEST_ACTIVITY.sortedCopy(inactiveTasksSupplier.get())) {
      registerInactiveTask(
          Tasks.jobKey(task),
          Tasks.id(task),
          calculateTimeout(Iterables.getLast(task.getTaskEvents()).getTimestamp()));
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
        deleteInternal(event.getTasks());
      }
    });
  }

  private synchronized void deleteInternal(Set<ScheduledTask> tasks) {
    for (ScheduledTask task : tasks) {
      String id = Tasks.id(task);
      tasksByJob.remove(Tasks.jobKey(task), id);
      Future<?> future = taskIdToFuture.remove(id);
      if (future != null) {
        future.cancel(false);
      }
    }
  }

  private void registerInactiveTask(
      final String jobKey,
      final String taskId,
      long timeRemaining) {

    LOG.fine("Prune task " + taskId + " in " + timeRemaining + " ms.");
    // Insert the latest inactive task at the tail.
    tasksByJob.put(jobKey, taskId);
    Runnable runnable = new Runnable() {
      @Override public void run() {
        pruneTask(jobKey, taskId);
      }
    };
    taskIdToFuture.put(taskId, executor.schedule(runnable, timeRemaining, TimeUnit.MILLISECONDS));

    ImmutableSet.Builder<String> pruneTaskIds = ImmutableSet.builder();
    Collection<String> tasks = tasksByJob.get(jobKey);
    Iterator<String> iterator = tasks.iterator();
    while (tasks.size() > perJobHistoryGoal) {
      // Pick oldest task from the head. Guaranteed by LinkedHashMultimap based on insertion order.
      String id = iterator.next();
      iterator.remove();
      pruneTaskIds.add(id);
      Future<?> future = taskIdToFuture.remove(id);
      if (future != null) {
        future.cancel(false);
      }
    }

    // Deferred to prevent triggering an event within an event handler.
    final Set<String> ids = pruneTaskIds.build();
    if (!ids.isEmpty()) {
      taskDeleter.execute(ids);
    }
  }

  private void pruneTask(String jobKey, String taskId) {
    try {
      LOG.info("Pruning expired inactive task " + taskId);
      taskDeleter.execute(ImmutableSet.of(taskId));
    } finally {
      // Synchronize only on internal structures to guarantee that no external locks are
      // obtained while holding the intrinsic lock.
      synchronized (this) {
        tasksByJob.remove(jobKey, taskId);
        taskIdToFuture.remove(taskId);
      }
    }
  }
}
