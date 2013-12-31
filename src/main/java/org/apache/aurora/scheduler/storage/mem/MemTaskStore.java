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
package com.twitter.aurora.scheduler.storage.mem;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import org.apache.commons.lang.StringUtils;

import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskQuery;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.storage.TaskStore;
import com.twitter.aurora.scheduler.storage.entities.IJobKey;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An in-memory task store.
 */
class MemTaskStore implements TaskStore.Mutable {

  private static final Logger LOG = Logger.getLogger(MemTaskStore.class.getName());

  @CmdLine(name = "slow_query_log_threshold",
      help = "Log all queries that take at least this long to execute.")
  private static final Arg<Amount<Long, Time>> SLOW_QUERY_LOG_THRESHOLD =
      Arg.create(Amount.of(25L, Time.MILLISECONDS));

  private final long slowQueryThresholdNanos = SLOW_QUERY_LOG_THRESHOLD.get().as(Time.NANOSECONDS);

  private final Map<String, Task> tasks = Maps.newConcurrentMap();
  private final Multimap<IJobKey, String> tasksByJobKey =
      Multimaps.synchronizedSetMultimap(HashMultimap.<IJobKey, String>create());

  // An interner is used here to collapse equivalent TaskConfig instances into canonical instances.
  // Ideally this would fall out of the object hierarchy (TaskConfig being associated with the job
  // rather than the task), but we intuit this detail here for performance reasons.
  private final Interner<TaskConfig, String> configInterner = new Interner<TaskConfig, String>();

  private final AtomicLong taskQueriesById = Stats.exportLong("task_queries_by_id");
  private final AtomicLong taskQueriesByJob = Stats.exportLong("task_queries_by_job");
  private final AtomicLong taskQueriesAll = Stats.exportLong("task_queries_all");

  @Timed("mem_storage_fetch_tasks")
  @Override
  public ImmutableSet<IScheduledTask> fetchTasks(Query.Builder query) {
    checkNotNull(query);

    long start = System.nanoTime();
    ImmutableSet<IScheduledTask> result = matches(query.get()).toSet();
    long durationNanos = System.nanoTime() - start;
    Level level = (durationNanos >= slowQueryThresholdNanos) ? Level.INFO : Level.FINE;
    if (LOG.isLoggable(level)) {
      Long time = Amount.of(durationNanos, Time.NANOSECONDS).as(Time.MILLISECONDS);
      LOG.log(level, "Query took " + time + " ms: " + query.get());
    }

    return result;
  }

  private final Function<IScheduledTask, Task> toTask =
      new Function<IScheduledTask, Task>() {
        @Override public Task apply(IScheduledTask task) {
          return new Task(task, configInterner);
        }
      };

  @Timed("mem_storage_save_tasks")
  @Override
  public void saveTasks(Set<IScheduledTask> newTasks) {
    checkNotNull(newTasks);
    Preconditions.checkState(Tasks.ids(newTasks).size() == newTasks.size(),
        "Proposed new tasks would create task ID collision.");

    Iterable<Task> canonicalized = Iterables.transform(newTasks, toTask);
    tasks.putAll(Maps.uniqueIndex(canonicalized, TO_ID));
    tasksByJobKey.putAll(taskIdsByJobKey(canonicalized));
  }

  private Multimap<IJobKey, String> taskIdsByJobKey(Iterable<Task> toIndex) {
    return Multimaps.transformValues(
        Multimaps.index(toIndex, Functions.compose(Tasks.SCHEDULED_TO_JOB_KEY, TO_SCHEDULED)),
        TO_ID);
  }

  @Timed("mem_storage_delete_all_tasks")
  @Override
  public void deleteAllTasks() {
    tasks.clear();
    tasksByJobKey.clear();
    configInterner.clear();
  }

  @Timed("mem_storage_delete_tasks")
  @Override
  public void deleteTasks(Set<String> taskIds) {
    checkNotNull(taskIds);

    for (String id : taskIds) {
      Task removed = tasks.remove(id);
      if (removed != null) {
        tasksByJobKey.remove(Tasks.SCHEDULED_TO_JOB_KEY.apply(removed.task), id);
        configInterner.removeAssociation(removed.task.getAssignedTask().getTask().newBuilder(), id);
      }
    }
  }

  @Timed("mem_storage_mutate_tasks")
  @Override
  public ImmutableSet<IScheduledTask> mutateTasks(
      Query.Builder query,
      Function<IScheduledTask, IScheduledTask> mutator) {

    checkNotNull(query);
    checkNotNull(mutator);

    ImmutableSet.Builder<IScheduledTask> mutated = ImmutableSet.builder();
    for (IScheduledTask original : matches(query.get())) {
      IScheduledTask maybeMutated = mutator.apply(original);
      if (!original.equals(maybeMutated)) {
        Preconditions.checkState(
            Tasks.id(original).equals(Tasks.id(maybeMutated)),
            "A task's ID may not be mutated.");
        tasks.put(Tasks.id(maybeMutated), toTask.apply(maybeMutated));
        mutated.add(maybeMutated);
      }
    }

    return mutated.build();
  }

  @Timed("mem_storage_unsafe_modify_in_place")
  @Override
  public boolean unsafeModifyInPlace(String taskId, ITaskConfig taskConfiguration) {
    MorePreconditions.checkNotBlank(taskId);
    checkNotNull(taskConfiguration);

    Task stored = tasks.get(taskId);
    if (stored == null) {
      return false;
    } else {
      ScheduledTask updated = stored.task.newBuilder();
      updated.getAssignedTask().setTask(taskConfiguration.newBuilder());
      tasks.put(taskId, toTask.apply(IScheduledTask.build(updated)));
      return true;
    }
  }

  private static Predicate<IScheduledTask> queryFilter(final TaskQuery query) {
    return new Predicate<IScheduledTask>() {
      @Override public boolean apply(IScheduledTask task) {
        ITaskConfig config = task.getAssignedTask().getTask();
        if (query.getOwner() != null) {
          if (!StringUtils.isBlank(query.getOwner().getRole())) {
            if (!query.getOwner().getRole().equals(config.getOwner().getRole())) {
              return false;
            }
          }
          if (!StringUtils.isBlank(query.getOwner().getUser())) {
            if (!query.getOwner().getUser().equals(config.getOwner().getUser())) {
              return false;
            }
          }
        }
        if (query.getEnvironment() != null) {
          if (!query.getEnvironment().equals(config.getEnvironment())) {
            return false;
          }
        }
        if (query.getJobName() != null) {
          if (!query.getJobName().equals(config.getJobName())) {
            return false;
          }
        }

        if (query.getTaskIds() != null) {
          if (!query.getTaskIds().contains(Tasks.id(task))) {
            return false;
          }
        }

        if (query.getStatusesSize() > 0) {
          if (!query.getStatuses().contains(task.getStatus())) {
            return false;
          }
        }
        if (!StringUtils.isEmpty(query.getSlaveHost())) {
          if (!query.getSlaveHost().equals(task.getAssignedTask().getSlaveHost())) {
            return false;
          }
        }
        if (query.getInstanceIdsSize() > 0) {
          if (!query.getInstanceIds().contains(task.getAssignedTask().getInstanceId())) {
            return false;
          }
        }

        return true;
      }
    };
  }

  private Iterable<Task> fromIdIndex(Iterable<String> taskIds) {
    ImmutableList.Builder<Task> matches = ImmutableList.builder();
    for (String id : taskIds) {
      Task match = tasks.get(id);
      if (match != null) {
        matches.add(match);
      }
    }
    return matches.build();
  }

  private FluentIterable<IScheduledTask> matches(TaskQuery query) {
    // Apply the query against the working set.
    Iterable<Task> from;
    Optional<IJobKey> jobKey = JobKeys.from(Query.arbitrary(query));
    if (query.isSetTaskIds()) {
      taskQueriesById.incrementAndGet();
      from = fromIdIndex(query.getTaskIds());
    } else if (jobKey.isPresent()) {
      taskQueriesByJob.incrementAndGet();
      Collection<String> taskIds = tasksByJobKey.get(jobKey.get());
      if (taskIds == null) {
        from = ImmutableList.of();
      } else {
        from = fromIdIndex(taskIds);
      }
    } else {
      taskQueriesAll.incrementAndGet();
      from = tasks.values();
    }

    return FluentIterable.from(from).transform(TO_SCHEDULED).filter(queryFilter(query));
  }

  private static final Function<Task, IScheduledTask> TO_SCHEDULED =
      new Function<Task, IScheduledTask>() {
        @Override public IScheduledTask apply(Task task) {
          return task.task;
        }
      };

  private static final Function<Task, String> TO_ID =
      Functions.compose(Tasks.SCHEDULED_TO_ID, TO_SCHEDULED);

  private static class Task {
    private final IScheduledTask task;

    Task(IScheduledTask task, Interner<TaskConfig, String> interner) {
      interner.removeAssociation(task.getAssignedTask().getTask().newBuilder(), Tasks.id(task));
      TaskConfig canonical = interner.addAssociation(
          task.getAssignedTask().getTask().newBuilder(),
          Tasks.id(task));
      ScheduledTask builder = task.newBuilder();
      builder.getAssignedTask().setTask(canonical);
      this.task = IScheduledTask.build(builder);
    }
  }
}
