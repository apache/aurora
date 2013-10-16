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
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import org.apache.commons.lang.StringUtils;

import com.twitter.aurora.gen.ScheduledTask;
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

  private final Map<String, IScheduledTask> tasks = Maps.newConcurrentMap();
  private final Multimap<IJobKey, String> tasksByJobKey =
      Multimaps.synchronizedSetMultimap(HashMultimap.<IJobKey, String>create());

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

  @Timed("mem_storage_save_tasks")
  @Override
  public void saveTasks(Set<IScheduledTask> newTasks) {
    checkNotNull(newTasks);
    Preconditions.checkState(Tasks.ids(newTasks).size() == newTasks.size(),
        "Proposed new tasks would create task ID collision.");

    tasks.putAll(Maps.uniqueIndex(newTasks, Tasks.SCHEDULED_TO_ID));
    tasksByJobKey.putAll(taskIdsByJobKey(newTasks));
  }

  private Multimap<IJobKey, String> taskIdsByJobKey(Iterable<IScheduledTask> toIndex) {
    return Multimaps.transformValues(
        Multimaps.index(toIndex, Tasks.SCHEDULED_TO_JOB_KEY),
        Tasks.SCHEDULED_TO_ID);
  }

  @Timed("mem_storage_delete_all_tasks")
  @Override
  public void deleteAllTasks() {
    tasks.clear();
    tasksByJobKey.clear();
  }

  @Timed("mem_storage_delete_tasks")
  @Override
  public void deleteTasks(Set<String> taskIds) {
    checkNotNull(taskIds);

    for (String id : taskIds) {
      IScheduledTask removed = tasks.remove(id);
      if (removed != null) {
        tasksByJobKey.remove(Tasks.SCHEDULED_TO_JOB_KEY.apply(removed), Tasks.id(removed));
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
        tasks.put(Tasks.id(maybeMutated), maybeMutated);
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

    IScheduledTask stored = tasks.get(taskId);
    if (stored == null) {
      return false;
    } else {
      ScheduledTask updated = stored.newBuilder();
      updated.getAssignedTask().setTask(taskConfiguration.newBuilder());
      tasks.put(taskId, IScheduledTask.build(updated));
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
          if (!query.getInstanceIds().contains(config.getInstanceIdDEPRECATED())) {
            return false;
          }
        }

        return true;
      }
    };
  }

  private Iterable<IScheduledTask> fromIdIndex(Iterable<String> taskIds) {
    ImmutableList.Builder<IScheduledTask> matches = ImmutableList.builder();
    for (String id : taskIds) {
      IScheduledTask match = tasks.get(id);
      if (match != null) {
        matches.add(match);
      }
    }
    return matches.build();
  }

  private FluentIterable<IScheduledTask> matches(TaskQuery query) {
    // Apply the query against the working set.
    Iterable<IScheduledTask> from;
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

    return FluentIterable.from(from).filter(queryFilter(query));
  }
}
