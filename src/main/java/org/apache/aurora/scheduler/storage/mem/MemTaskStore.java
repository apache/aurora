/**
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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import org.apache.aurora.common.base.MorePreconditions;
import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.util.Objects.requireNonNull;

/**
 * An in-memory task store.
 */
class MemTaskStore implements TaskStore.Mutable {

  private static final Logger LOG = Logger.getLogger(MemTaskStore.class.getName());

  /**
   * When true, enable snapshot deflation.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.PARAMETER})
  @Qualifier
  public @interface SlowQueryThreshold { }

  private final long slowQueryThresholdNanos;

  private static final Function<Query.Builder, Optional<Set<IJobKey>>> QUERY_TO_JOB_KEY =
      JobKeys::from;
  private static final Function<Query.Builder, Optional<Set<String>>> QUERY_TO_SLAVE_HOST =
      query -> Optional.fromNullable(query.get().getSlaveHosts());

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
  private final List<SecondaryIndex<?>> secondaryIndices;

  // An interner is used here to collapse equivalent TaskConfig instances into canonical instances.
  // Ideally this would fall out of the object hierarchy (TaskConfig being associated with the job
  // rather than the task), but we intuit this detail here for performance reasons.
  private final Interner<TaskConfig, String> configInterner = new Interner<>();

  private final AtomicLong taskQueriesById;
  private final AtomicLong taskQueriesAll;

  @Inject
  MemTaskStore(
      StatsProvider statsProvider,
      @SlowQueryThreshold Amount<Long, Time> slowQueryThreshold) {

    secondaryIndices = ImmutableList.of(
        new SecondaryIndex<>(
            Tasks::getJob,
            QUERY_TO_JOB_KEY,
            statsProvider,
            "job"),
        new SecondaryIndex<>(
            Tasks::scheduledToSlaveHost,
            QUERY_TO_SLAVE_HOST,
            statsProvider,
            "host"));
    slowQueryThresholdNanos = slowQueryThreshold.as(Time.NANOSECONDS);
    taskQueriesById = statsProvider.makeCounter("task_queries_by_id");
    taskQueriesAll = statsProvider.makeCounter("task_queries_all");
  }

  @Timed("mem_storage_fetch_tasks")
  @Override
  public ImmutableSet<IScheduledTask> fetchTasks(Query.Builder query) {
    requireNonNull(query);

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

  @Timed("mem_storage_get_job_keys")
  @Override
  public Set<IJobKey> getJobKeys() {
    return FluentIterable.from(fetchTasks(Query.unscoped()))
        .transform(Tasks::getJob)
        .toSet();
  }

  private final Function<IScheduledTask, Task> toTask = task -> new Task(task, configInterner);

  @Timed("mem_storage_save_tasks")
  @Override
  public void saveTasks(Set<IScheduledTask> newTasks) {
    requireNonNull(newTasks);
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
    requireNonNull(taskIds);

    for (String id : taskIds) {
      Task removed = tasks.remove(id);
      if (removed != null) {
        for (SecondaryIndex<?> index : secondaryIndices) {
          index.remove(removed.storedTask);
        }
        configInterner.removeAssociation(
            removed.storedTask.getAssignedTask().getTask().newBuilder(),
            id);
      }
    }
  }

  @Timed("mem_storage_mutate_tasks")
  @Override
  public ImmutableSet<IScheduledTask> mutateTasks(
      Query.Builder query,
      Function<IScheduledTask, IScheduledTask> mutator) {

    requireNonNull(query);
    requireNonNull(mutator);

    ImmutableSet.Builder<IScheduledTask> mutated = ImmutableSet.builder();
    for (Task original : matches(query).toList()) {
      IScheduledTask maybeMutated = mutator.apply(original.storedTask);
      if (!original.storedTask.equals(maybeMutated)) {
        Preconditions.checkState(
            Tasks.id(original.storedTask).equals(Tasks.id(maybeMutated)),
            "A task's ID may not be mutated.");
        tasks.put(Tasks.id(maybeMutated), toTask.apply(maybeMutated));
        for (SecondaryIndex<?> index : secondaryIndices) {
          index.replace(original.storedTask, maybeMutated);
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
    requireNonNull(taskConfiguration);

    Task stored = tasks.get(taskId);
    if (stored == null) {
      return false;
    } else {
      ScheduledTask updated = stored.storedTask.newBuilder();
      updated.getAssignedTask().setTask(taskConfiguration.newBuilder());
      tasks.put(taskId, toTask.apply(IScheduledTask.build(updated)));
      return true;
    }
  }

  private static Predicate<Task> queryFilter(Query.Builder query) {
    return Predicates.compose(
        Util.queryFilter(query),
        new Function<Task, IScheduledTask>() {
          @Override
          public IScheduledTask apply(Task canonicalTask) {
            return canonicalTask.storedTask;
          }
        });
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

    return FluentIterable.from(from.get()).filter(queryFilter(query));
  }

  private static final Function<Task, IScheduledTask> TO_SCHEDULED = task -> task.storedTask;

  private static final Function<Task, String> TO_ID =
      Functions.compose(Tasks::id, TO_SCHEDULED);

  private static class Task {
    private final IScheduledTask storedTask;

    Task(IScheduledTask storedTask, Interner<TaskConfig, String> interner) {
      interner.removeAssociation(
          storedTask.getAssignedTask().getTask().newBuilder(),
          Tasks.id(storedTask));
      TaskConfig canonical = interner.addAssociation(
          storedTask.getAssignedTask().getTask().newBuilder(),
          Tasks.id(storedTask));
      ScheduledTask builder = storedTask.newBuilder();
      builder.getAssignedTask().setTask(canonical);
      this.storedTask = IScheduledTask.build(builder);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Task)) {
        return false;
      }

      Task other = (Task) o;
      return storedTask.equals(other.storedTask);
    }

    @Override
    public int hashCode() {
      return storedTask.hashCode();
    }
  }

  @VisibleForTesting
  static String getIndexSizeStatName(String name) {
    return "task_store_index_" + name + "_items";
  }

  /**
   * A non-unique secondary index on the task store.  Maps a custom key type to a set of task IDs.
   *
   * @param <K> Key type.
   */
  private static class SecondaryIndex<K> {
    private final Multimap<K, String> index =
        Multimaps.synchronizedSetMultimap(HashMultimap.create());
    private final Function<IScheduledTask, K> indexer;
    private final Function<Query.Builder, Optional<Set<K>>> queryExtractor;
    private final AtomicLong hitCount;

    /**
     * Creates a secondary index that will extract keys from tasks using the provided indexer.
     *
     * @param indexer Indexing function.
     * @param queryExtractor Function to extract the keys relevant to a query.
     * @param statsProvider Stats system to export metrics to.
     * @param name Name to use in stats keys.
     */
    SecondaryIndex(
        Function<IScheduledTask, K> indexer,
        Function<Query.Builder, Optional<Set<K>>> queryExtractor,
        StatsProvider statsProvider,
        String name) {

      this.indexer = indexer;
      this.queryExtractor = queryExtractor;
      this.hitCount = statsProvider.makeCounter("task_queries_by_" + name);
      statsProvider.makeGauge(
          getIndexSizeStatName(name),
          new Supplier<Number>() {
            @Override
            public Number get() {
              return index.size();
            }
          });
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
        index.remove(key, Tasks.id(task));
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
            synchronized (index) {
              for (K key : keys) {
                builder.addAll(index.get(key));
              }
            }
            return builder.build();
          }
    };

    Optional<Iterable<String>> getMatches(Query.Builder query) {
      return queryExtractor.apply(query).transform(lookup);
    }
  }
}
