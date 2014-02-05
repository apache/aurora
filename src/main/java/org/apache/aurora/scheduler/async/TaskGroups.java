/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.async;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.common.util.Clock;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;

import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.async.TaskGroup.GroupState;

/**
 * A collection of task groups, where a task group is a collection of tasks that are known to be
 * equal in the way they schedule. This is expected to be tasks associated with the same job key,
 * who also have {@code equal()} {@link ITaskConfig} values.
 * <p>
 * This is used to prevent redundant work in trying to schedule tasks as well as to provide
 * nearly-equal responsiveness when scheduling across jobs.  In other words, a 1000 instance job
 * cannot starve a 1 instance job.
 */
public class TaskGroups implements EventSubscriber {

  private static final Logger LOG = Logger.getLogger(TaskGroups.class.getName());

  private final LoadingCache<GroupKey, TaskGroup> groups;
  private final Clock clock;
  private final RescheduleCalculator rescheduleCalculator;

  static class TaskGroupsSettings {
    private final BackoffStrategy taskGroupBackoff;
    private final RateLimiter rateLimiter;

    TaskGroupsSettings(BackoffStrategy taskGroupBackoff, RateLimiter rateLimiter) {
      this.taskGroupBackoff = checkNotNull(taskGroupBackoff);
      this.rateLimiter = checkNotNull(rateLimiter);
    }
  }

  @Inject
  TaskGroups(
      ShutdownRegistry shutdownRegistry,
      TaskGroupsSettings settings,
      TaskScheduler taskScheduler,
      Clock clock,
      RescheduleCalculator rescheduleCalculator) {

    this(
        createThreadPool(shutdownRegistry),
        settings.taskGroupBackoff,
        settings.rateLimiter,
        taskScheduler,
        clock,
        rescheduleCalculator);
  }

  @VisibleForTesting
  TaskGroups(
      final ScheduledExecutorService executor,
      final BackoffStrategy taskGroupBackoffStrategy,
      final RateLimiter rateLimiter,
      final TaskScheduler taskScheduler,
      final Clock clock,
      final RescheduleCalculator rescheduleCalculator) {

    checkNotNull(executor);
    checkNotNull(taskGroupBackoffStrategy);
    checkNotNull(rateLimiter);
    checkNotNull(taskScheduler);
    this.clock = checkNotNull(clock);
    this.rescheduleCalculator = checkNotNull(rescheduleCalculator);

    final TaskScheduler ratelLimitedScheduler = new TaskScheduler() {
      @Override
      public TaskSchedulerResult schedule(String taskId) {
        rateLimiter.acquire();
        return taskScheduler.schedule(taskId);
      }
    };

    groups = CacheBuilder.newBuilder().build(new CacheLoader<GroupKey, TaskGroup>() {
      @Override
      public TaskGroup load(GroupKey key) {
        TaskGroup group = new TaskGroup(key, taskGroupBackoffStrategy);
        LOG.info("Evaluating group " + key + " in " + group.getPenaltyMs() + " ms");
        startGroup(group, executor, ratelLimitedScheduler);
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
      final TaskScheduler taskScheduler) {

    Runnable monitor = new Runnable() {
      @Override
      public void run() {
        GroupState state = group.isReady(clock.nowMillis());

        switch (state) {
          case EMPTY:
            maybeInvalidate(group);
            break;

          case READY:
            String id = group.pop();
            TaskScheduler.TaskSchedulerResult result = taskScheduler.schedule(id);
            switch (result) {
              case SUCCESS:
                if (!maybeInvalidate(group)) {
                  executor.schedule(this, group.resetPenaltyAndGet(), TimeUnit.MILLISECONDS);
                }
                break;

              case TRY_AGAIN:
                group.push(id, clock.nowMillis());
                executor.schedule(this, group.penalizeAndGet(), TimeUnit.MILLISECONDS);
                break;

              default:
                throw new IllegalStateException("Unknown TaskSchedulerResult " + result);
            }
            break;

          case NOT_READY:
            executor.schedule(this, group.getPenaltyMs(), TimeUnit.MILLISECONDS);
            break;

          default:
            throw new IllegalStateException("Unknown GroupState " + state);
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
      @Override
      public void execute() {
        new ExecutorServiceShutdown(executor, Amount.of(1L, Time.SECONDS)).execute();
      }
    });
    return executor;
  }

  private synchronized void add(IAssignedTask task, long scheduleDelayMs) {
    groups.getUnchecked(new GroupKey(task.getTask()))
        .push(task.getTaskId(), clock.nowMillis() + scheduleDelayMs);
  }

  /**
   * Informs the task groups of a task state change.
   * <p>
   * This is used to observe {@link org.apache.aurora.gen.ScheduleStatus#PENDING} tasks and begin
   * attempting to schedule them.
   *
   * @param stateChange State change notification.
   */
  @Subscribe
  public synchronized void taskChangedState(TaskStateChange stateChange) {
    if (stateChange.getNewState() == PENDING) {
      IScheduledTask task = stateChange.getTask();
      long readyAtMs = stateChange.isTransition()
          ? rescheduleCalculator.getFlappingPenaltyMs(task)
          : rescheduleCalculator.getStartupScheduleDelayMs(task);
      add(task.getAssignedTask(), readyAtMs);
    }
  }

  /**
   * Signals the scheduler that tasks have been deleted.
   *
   * @param deleted Tasks deleted event.
   */
  @Subscribe
  public synchronized void tasksDeleted(TasksDeleted deleted) {
    for (IAssignedTask task
        : Iterables.transform(deleted.getTasks(), Tasks.SCHEDULED_TO_ASSIGNED)) {
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
    private final ITaskConfig canonicalTask;

    GroupKey(ITaskConfig task) {
      this.canonicalTask = task;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(canonicalTask);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof GroupKey)) {
        return false;
      }
      GroupKey other = (GroupKey) o;
      return Objects.equal(canonicalTask, other.canonicalTask);
    }

    @Override
    public String toString() {
      return JobKeys.toPath(Tasks.INFO_TO_JOB_KEY.apply(canonicalTask));
    }
  }
}
