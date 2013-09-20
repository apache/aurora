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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.configuration.ConfigurationManager;
import com.twitter.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.aurora.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.aurora.gen.ScheduleStatus.PENDING;

/**
 * A collection of task groups, where a task group is a collection of tasks that are known to be
 * equal in the way they schedule. This is expected to be tasks associated with the same job key,
 * who also have {@code equal()} {@link TaskConfig} values.
 * <p>
 * This is used to prevent redundant work in trying to schedule tasks as well as to provide
 * nearly-equal responsiveness when scheduling across jobs.  In other words, a 1000 shard job cannot
 * starve a 1 shard job.
 */
public class TaskGroups implements EventSubscriber {

  private static final Logger LOG = Logger.getLogger(TaskGroups.class.getName());

  private final Storage storage;
  private final LoadingCache<GroupKey, TaskGroup> groups;

  @Inject
  TaskGroups(
      ShutdownRegistry shutdownRegistry,
      Storage storage,
      BackoffStrategy backoffStrategy,
      RateLimiter rateLimiter,
      SchedulingAction schedulingAction) {

    this(
        createThreadPool(shutdownRegistry),
        storage,
        backoffStrategy,
        rateLimiter,
        schedulingAction);
  }

  TaskGroups(
      final ScheduledExecutorService executor,
      final Storage storage,
      final BackoffStrategy backoffStrategy,
      final RateLimiter rateLimiter,
      final SchedulingAction schedulingAction) {

    this.storage = checkNotNull(storage);
    checkNotNull(executor);
    checkNotNull(backoffStrategy);
    checkNotNull(schedulingAction);

    final SchedulingAction rateLimitedAction = new SchedulingAction() {
      @Override public boolean schedule(String taskId) {
        rateLimiter.acquire();
        return schedulingAction.schedule(taskId);
      }
    };

    groups = CacheBuilder.newBuilder().build(new CacheLoader<GroupKey, TaskGroup>() {
      @Override public TaskGroup load(GroupKey key) {
        TaskGroup group = new TaskGroup(key, backoffStrategy);
        LOG.info("Evaluating group " + key + " in " + group.getPenaltyMs() + " ms");
        startGroup(group, executor, rateLimitedAction);
        return group;
      }
    });
  }

  private synchronized boolean maybeInvalidate(TaskGroup group) {
    if (group.getTaskIds().isEmpty()) {
      groups.invalidate(group.getKey());
      return true;
    }
    return false;
  }

  private void startGroup(
      final TaskGroup group,
      final ScheduledExecutorService executor,
      final SchedulingAction action) {

    Runnable monitor = new Runnable() {
      @Override public void run() {
        @Nullable String taskId = group.pop();
        if (taskId != null) {
          if (action.schedule(taskId)) {
            if (!maybeInvalidate(group)) {
              executor.schedule(this, group.resetPenaltyAndGet(), TimeUnit.MILLISECONDS);
            }
          } else {
            group.push(taskId);
            executor.schedule(this, group.penalizeAndGet(), TimeUnit.MILLISECONDS);
          }
        } else {
          maybeInvalidate(group);
        }
      }
    };
    executor.schedule(monitor, group.getPenaltyMs(), TimeUnit.MILLISECONDS);
  }

  private static ScheduledExecutorService createThreadPool(ShutdownRegistry shutdownRegistry) {
    // TODO(William Farner): Leverage ExceptionHandlingScheduledExecutorService:
    // com.twitter.common.util.concurrent.ExceptionHandlingScheduledExecutorService
    final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
        1,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("TaskScheduler-%d").build());
    Stats.exportSize("schedule_queue_size", executor.getQueue());
    shutdownRegistry.addAction(new Command() {
      @Override public void execute() {
        new ExecutorServiceShutdown(executor, Amount.of(1L, Time.SECONDS)).execute();
      }
    });
    return executor;
  }

  private synchronized void add(AssignedTask task) {
    groups.getUnchecked(new GroupKey(task.getTask())).push(task.getTaskId());
  }

  /**
   * Informs the task groups of a task state change.
   * <p>
   * This is used to observe {@link com.twitter.aurora.gen.ScheduleStatus#PENDING} tasks and begin
   * attempting to schedule them.
   *
   * @param stateChange State change notification.
   */
  @Subscribe
  public synchronized void taskChangedState(TaskStateChange stateChange) {
    // TODO(William Farner): Use the ancestry of a task to check if the task is flapping, use
    // this to implement scheduling backoff (inducing a longer delay).
    if (stateChange.getNewState() == PENDING) {
      add(stateChange.getTask().getAssignedTask());
    }
  }

  /**
   * Signals that storage has started and is consistent.
   * <p>
   * Upon this signal, all {@link com.twitter.aurora.gen.ScheduleStatus#PENDING} tasks in the stoage
   * will become eligible for scheduling.
   *
   * @param event Storage started notification.
   */
  @Subscribe
  public void storageStarted(StorageStarted event) {
    for (ScheduledTask task
        : Storage.Util.consistentFetchTasks(storage, Query.unscoped().byStatus(PENDING))) {

      add(task.getAssignedTask());
    }
  }

  /**
   * Signals the scheduler that tasks have been deleted.
   *
   * @param deleted Tasks deleted event.
   */
  @Subscribe
  public synchronized void tasksDeleted(TasksDeleted deleted) {
    for (AssignedTask task : Iterables.transform(deleted.getTasks(), Tasks.SCHEDULED_TO_ASSIGNED)) {
      TaskGroup group = groups.getIfPresent(new GroupKey(task.getTask()));
      if (group != null) {
        group.remove(task.getTaskId());
      }
    }
  }

  public Iterable<TaskGroup> getGroups() {
    return ImmutableSet.copyOf(groups.asMap().values());
  }

  static class GroupKey {
    private final TaskConfig scrubbedCanonicalTask;

    GroupKey(TaskConfig task) {
      this.scrubbedCanonicalTask = ConfigurationManager.scrubNonUniqueTaskFields(task);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(scrubbedCanonicalTask);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof GroupKey)) {
        return false;
      }
      GroupKey other = (GroupKey) o;
      return Objects.equal(scrubbedCanonicalTask, other.scrubbedCanonicalTask);
    }

    @Override
    public String toString() {
      return JobKeys.toPath(Tasks.INFO_TO_JOB_KEY.apply(scrubbedCanonicalTask));
    }
  }

  interface SchedulingAction {
    /**
     * Attempts to schedule a task, possibly performing irreversible actions.
     *
     * @param taskId The task to attempt to schedule.
     * @return {@code true} if the task was scheduled, {@code false} otherwise.
     */
    boolean schedule(String taskId);
  }
}
