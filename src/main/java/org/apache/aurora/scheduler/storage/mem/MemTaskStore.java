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
package org.apache.aurora.scheduler.storage.mem;

import java.util.List;
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
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;

import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.commons.lang.StringUtils;

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

  private static final Function<Query.Builder, Optional<Set<IJobKey>>> QUERY_TO_JOB_KEY =
      new Function<Query.Builder, Optional<Set<IJobKey>>>() {
        @Override
        public Optional<Set<IJobKey>> apply(Query.Builder query) {
            return JobKeys.from(query);
        }
      };
  private static final Function<Query.Builder, Optional<Set<String>>> QUERY_TO_SLAVE_HOST =
      new Function<Query.Builder, Optional<Set<String>>>() {
        @Override
        public Optional<Set<String>> apply(Query.Builder query) {
          return Optional.fromNullable(query.get().getSlaveHosts());
        }
      };

  // Since this class operates under the API and umbrella of {@link Storage}, it is expected to be
  // thread-safe but not necessarily strongly-consistent unless the externally-controlled storage
  // lock is secured.  To adhere to that, these data structures are individually thread-safe, but
  // we don't lock across them because of the relaxed consistency guarantees.
  // Note that this behavior makes it possible to receive query results that are not sane,
  // specifically when a secondary key value is changed.  In other words, we currently don't always
  // support the invariant that a query by slave host yields a result with all tasks matching that
  // slave host.  This is deemed acceptable due to the fact that secondary key values are rarely
  // mutated in practice, and mutated in ways that are not impacted by this behavior.
  private final Map<String, Task> tasks = Maps.newConcurrentMap();
  private final List<SecondaryIndex<?>> secondaryIndices = ImmutableList.of(
      new SecondaryIndex<>(
          Tasks.SCHEDULED_TO_JOB_KEY,
          QUERY_TO_JOB_KEY,
          Stats.exportLong("task_queries_by_job")),
      new SecondaryIndex<>(
          Tasks.SCHEDULED_TO_SLAVE_HOST,
          QUERY_TO_SLAVE_HOST,
          Stats.exportLong("task_queries_by_host")));

  // An interner is used here to collapse equivalent TaskConfig instances into canonical instances.
  // Ideally this would fall out of the object hierarchy (TaskConfig being associated with the job
  // rather than the task), but we intuit this detail here for performance reasons.
  private final Interner<TaskConfig, String> configInterner = new Interner<>();

  private final AtomicLong taskQueriesById = Stats.exportLong("task_queries_by_id");
  private final AtomicLong taskQueriesAll = Stats.exportLong("task_queries_all");

  @Timed("mem_storage_fetch_tasks")
  @Override
  public ImmutableSet<IScheduledTask> fetchTasks(Query.Builder query) {
    checkNotNull(query);

    long start = System.nanoTime();
    ImmutableSet<IScheduledTask> result = matches(query).transform(TO_SCHEDULED).toSet();
    long durationNanos = System.nanoTime() - start;
    Level level = durationNanos >= slowQueryThresholdNanos ? Level.INFO : Level.FINE;
    if (LOG.isLoggable(level)) {
      Long time = Amount.of(durationNanos, Time.NANOSECONDS).as(Time.MILLISECONDS);
      LOG.log(level, "Query took " + time + " ms: " + query.get());
    }

    return result;
  }

  private final Function<IScheduledTask, Task> toTask =
      new Function<IScheduledTask, Task>() {
        @Override
        public Task apply(IScheduledTask task) {
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
    for (SecondaryIndex<?> index : secondaryIndices) {
      index.insert(Iterables.transform(canonicalized, TO_SCHEDULED));
    }
  }

  @Timed("mem_storage_delete_all_tasks")
  @Override
  public void deleteAllTasks() {
    tasks.clear();
    for (SecondaryIndex<?> index : secondaryIndices) {
      index.clear();
    }
    configInterner.clear();
  }

  @Timed("mem_storage_delete_tasks")
  @Override
  public void deleteTasks(Set<String> taskIds) {
    checkNotNull(taskIds);

    for (String id : taskIds) {
      Task removed = tasks.remove(id);
      if (removed != null) {
        for (SecondaryIndex<?> index : secondaryIndices) {
          index.remove(removed.task);
        }
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
    for (Task original : matches(query).toList()) {
      IScheduledTask maybeMutated = mutator.apply(original.task);
      if (!original.task.equals(maybeMutated)) {
        Preconditions.checkState(
            Tasks.id(original.task).equals(Tasks.id(maybeMutated)),
            "A task's ID may not be mutated.");
        tasks.put(Tasks.id(maybeMutated), toTask.apply(maybeMutated));
        for (SecondaryIndex<?> index : secondaryIndices) {
          index.replace(original.task, maybeMutated);
        }

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

  private static Predicate<Task> queryFilter(final TaskQuery query) {
    return new Predicate<Task>() {
      @Override
      public boolean apply(Task canonicalTask) {
        IScheduledTask task = canonicalTask.task;
        ITaskConfig config = task.getAssignedTask().getTask();
        if (query.getOwner() != null) {
          if (!StringUtils.isBlank(query.getOwner().getRole())
              && !query.getOwner().getRole().equals(config.getOwner().getRole())) {
            return false;
          }
          if (!StringUtils.isBlank(query.getOwner().getUser())
              && !query.getOwner().getUser().equals(config.getOwner().getUser())) {
            return false;
          }
        }
        if (query.getEnvironment() != null
            && !query.getEnvironment().equals(config.getEnvironment())) {
          return false;
        }
        if (query.getJobName() != null && !query.getJobName().equals(config.getJobName())) {
          return false;
        }

        if (query.getJobKeysSize() > 0
            && !query.getJobKeys().contains(JobKeys.from(config).newBuilder())) {
          return false;
        }
        if (query.getTaskIds() != null && !query.getTaskIds().contains(Tasks.id(task))) {
            return false;
        }

        if (query.getStatusesSize() > 0 && !query.getStatuses().contains(task.getStatus())) {
          return false;
        }
        if (query.getSlaveHostsSize() > 0
            && !query.getSlaveHosts().contains(task.getAssignedTask().getSlaveHost())) {
          return false;
        }
        if (query.getInstanceIdsSize() > 0
            && !query.getInstanceIds().contains(task.getAssignedTask().getInstanceId())) {
          return false;
        }

        return true;
      }
    };
  }

  private Iterable<Task> fromIdIndex(Iterable<String> taskIds) {
    return FluentIterable.from(taskIds)
        .transform(Functions.forMap(tasks, null))
        .filter(Predicates.notNull())
        .toList();
  }

  private FluentIterable<Task> matches(Query.Builder query) {
    // Apply the query against the working set.
    Optional<? extends Iterable<Task>> from = Optional.absent();
    if (query.get().isSetTaskIds()) {
      taskQueriesById.incrementAndGet();
      from = Optional.of(fromIdIndex(query.get().getTaskIds()));
    } else {
      for (SecondaryIndex<?> index : secondaryIndices) {
        Optional<Iterable<String>> indexMatch = index.getMatches(query);
        if (indexMatch.isPresent()) {
          // Note: we could leverage multiple indexes here if the query applies to them, by
          // choosing to intersect the results.  Given current indexes and query profile, this is
          // unlikely to offer much improvement, though.
          from = Optional.of(fromIdIndex(indexMatch.get()));
          break;
        }
      }

      // No indices match, fall back to a full scan.
      if (!from.isPresent()) {
        taskQueriesAll.incrementAndGet();
        from = Optional.of(tasks.values());
      }
    }

    return FluentIterable.from(from.get()).filter(queryFilter(query.get()));
  }

  private static final Function<Task, IScheduledTask> TO_SCHEDULED =
      new Function<Task, IScheduledTask>() {
        @Override
        public IScheduledTask apply(Task task) {
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

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Task)) {
        return false;
      }

      Task other = (Task) o;
      return task.equals(other.task);
    }

    @Override
    public int hashCode() {
      return task.hashCode();
    }
  }

  /**
   * A non-unique secondary index on the task store.  Maps a custom key type to a set of task IDs.
   *
   * @param <K> Key type.
   */
  private static class SecondaryIndex<K> {
    private final Multimap<K, String> index =
        Multimaps.synchronizedSetMultimap(HashMultimap.<K, String>create());
    private final Function<IScheduledTask, K> indexer;
    private final Function<Query.Builder, Optional<Set<K>>> queryExtractor;
    private final AtomicLong hitCount;

    /**
     * Creates a secondary index that will extract keys from tasks using the provided indexer.
     *
     * @param indexer Indexing function.
     * @param queryExtractor Function to extract the keys relevant to a query.
     * @param hitCount Counter for number of times the secondary index applies to a query.
     */
    SecondaryIndex(
        Function<IScheduledTask, K> indexer,
        Function<Query.Builder, Optional<Set<K>>> queryExtractor,
        AtomicLong hitCount) {

      this.indexer = indexer;
      this.queryExtractor = queryExtractor;
      this.hitCount = hitCount;
    }

    void insert(Iterable<IScheduledTask> tasks) {
      for (IScheduledTask task : tasks) {
        insert(task);
      }
    }

    void insert(IScheduledTask task) {
      K key = indexer.apply(task);
      if (key != null) {
        index.put(key, Tasks.id(task));
      }
    }

    void clear() {
      index.clear();
    }

    void remove(IScheduledTask task) {
      K key = indexer.apply(task);
      if (key != null) {
        index.remove(key, task);
      }
    }

    void replace(IScheduledTask old, IScheduledTask replacement) {
      synchronized (index) {
        remove(old);
        insert(replacement);
      }
    }

    private final Function<Set<K>, Iterable<String>> lookup =
        new Function<Set<K>, Iterable<String>>() {
          @Override
          public Iterable<String> apply(Set<K> keys) {
            hitCount.incrementAndGet();
            ImmutableSet.Builder<String> builder = ImmutableSet.builder();
            for (K key : keys) {
              builder.addAll(index.get(key));
            }
            return builder.build();
          }
    };

    Optional<Iterable<String>> getMatches(Query.Builder query) {
      return queryExtractor.apply(query).transform(lookup);
    }
  }
}
