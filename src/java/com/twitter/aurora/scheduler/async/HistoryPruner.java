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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.eventbus.Subscribe;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.state.StateManager;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.entities.IJobKey;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import static com.twitter.aurora.scheduler.events.PubsubEvent.StorageStarted;
import static com.twitter.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import static com.twitter.aurora.scheduler.events.PubsubEvent.TasksDeleted;

/**
 * Prunes tasks in a job based on per-job history and an inactive time threshold by observing tasks
 * transitioning into one of the inactive states.
 */
public class HistoryPruner implements EventSubscriber {
  private static final Logger LOG = Logger.getLogger(HistoryPruner.class.getName());

  private static final Ordering<IScheduledTask> LATEST_ACTIVITY = Ordering.natural()
      .onResultOf(new Function<IScheduledTask, Long>() {
        @Override public Long apply(IScheduledTask task) {
          return Iterables.getLast(task.getTaskEvents()).getTimestamp();
        }
      });

  @VisibleForTesting
  static final Query.Builder INACTIVE_QUERY = Query.unscoped().terminal();

  private final Multimap<IJobKey, String> tasksByJob =
      Multimaps.synchronizedSetMultimap(LinkedHashMultimap.<IJobKey, String>create());
  @VisibleForTesting
  Multimap<IJobKey, String> getTasksByJob() {
    return tasksByJob;
  }

  private final ScheduledExecutorService executor;
  private final Storage storage;
  private final StateManager stateManager;
  private final Clock clock;
  private final long pruneThresholdMillis;
  private final int perJobHistoryGoal;
  private final Map<String, Future<?>> taskIdToFuture = Maps.newConcurrentMap();

  @BindingAnnotation
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface PruneThreshold { }

  @Inject
  HistoryPruner(
      final ScheduledExecutorService executor,
      final Storage storage,
      final StateManager stateManager,
      final Clock clock,
      @PruneThreshold Amount<Long, Time> inactivePruneThreshold,
      @PruneThreshold int perJobHistoryGoal) {

    this.executor = checkNotNull(executor);
    this.storage = checkNotNull(storage);
    this.stateManager = checkNotNull(stateManager);
    this.clock = checkNotNull(clock);
    this.pruneThresholdMillis = inactivePruneThreshold.as(Time.MILLISECONDS);
    this.perJobHistoryGoal = perJobHistoryGoal;
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
  public void recordStateChange(TaskStateChange change) {
    if (Tasks.isTerminated(change.getNewState())) {
      registerInactiveTask(
          Tasks.SCHEDULED_TO_JOB_KEY.apply(change.getTask()),
          change.getTaskId(),
          calculateTimeout(clock.nowMillis()));
    }
  }

  /**
   * When triggered, iterates through inactive tasks in the system and prunes tasks that
   * exceed the history goal for a job or are beyond the time threshold.
   *
   * @param event A new StorageStarted event.
   */
  @Subscribe
  public void storageStarted(StorageStarted event) {
    for (IScheduledTask task
        : LATEST_ACTIVITY.sortedCopy(Storage.Util.consistentFetchTasks(storage, INACTIVE_QUERY))) {

      registerInactiveTask(
          Tasks.SCHEDULED_TO_JOB_KEY.apply(task),
          Tasks.id(task),
          calculateTimeout(Iterables.getLast(task.getTaskEvents()).getTimestamp()));
    }
  }

  private void deleteTasks(Set<String> taskIds) {
    LOG.info("Pruning inactive tasks " + taskIds);
    stateManager.deleteTasks(taskIds);
  }

  /**
   * When triggered, removes the tasks scheduled for pruning and cancels any existing future.
   *
   * @param event A new TasksDeleted event.
   */
  @Subscribe
  public void tasksDeleted(final TasksDeleted event) {
    for (IScheduledTask task : event.getTasks()) {
      String id = Tasks.id(task);
      tasksByJob.remove(Tasks.SCHEDULED_TO_JOB_KEY.apply(task), id);
      Future<?> future = taskIdToFuture.remove(id);
      if (future != null) {
        future.cancel(false);
      }
    }
  }

  private void registerInactiveTask(
      final IJobKey jobKey,
      final String taskId,
      long timeRemaining) {

    LOG.fine("Prune task " + taskId + " in " + timeRemaining + " ms.");
    // Insert the latest inactive task at the tail.
    tasksByJob.put(jobKey, taskId);
    Runnable runnable = new Runnable() {
      @Override public void run() {
        LOG.info("Pruning expired inactive task " + taskId);
        tasksByJob.remove(jobKey, taskId);
        taskIdToFuture.remove(taskId);
        deleteTasks(ImmutableSet.of(taskId));
      }
    };
    taskIdToFuture.put(taskId, executor.schedule(runnable, timeRemaining, TimeUnit.MILLISECONDS));

    ImmutableSet.Builder<String> pruneTaskIds = ImmutableSet.builder();
    Collection<String> tasks = tasksByJob.get(jobKey);
    // From Multimaps javadoc: "It is imperative that the user manually synchronize on the returned
    // multimap when accessing any of its collection views".
    synchronized (tasksByJob) {
      Iterator<String> iterator = tasks.iterator();
      while (tasks.size() > perJobHistoryGoal) {
        // Pick oldest task from the head. Guaranteed by LinkedHashMultimap based on insertion
        // order.
        String id = iterator.next();
        iterator.remove();
        pruneTaskIds.add(id);
        Future<?> future = taskIdToFuture.remove(id);
        if (future != null) {
          future.cancel(false);
        }
      }
    }

    Set<String> ids = pruneTaskIds.build();
    if (!ids.isEmpty()) {
      deleteTasks(ids);
    }
  }
}
