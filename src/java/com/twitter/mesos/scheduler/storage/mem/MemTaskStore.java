package com.twitter.mesos.scheduler.storage.mem;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import org.apache.commons.lang.StringUtils;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Closure;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.storage.TaskStore;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An in-memory task store.
 */
public class MemTaskStore implements TaskStore.Mutable.Transactioned {

  private static final Logger LOG = Logger.getLogger(MemTaskStore.class.getName());

  @CmdLine(name = "slow_query_log_threshold",
      help = "Log all queries that take at least this long to execute.")
  private static final Arg<Amount<Long, Time>> SLOW_QUERY_LOG_THRESHOLD =
      Arg.create(Amount.of(25L, Time.MILLISECONDS));

  /**
   * COPIER is separate from deepCopy to capture timing information.  Only the instrumented
   * {@link #deepCopy(ScheduledTask)} should interact directly with {@code COPIER}.
   */
  private static final Function<ScheduledTask, ScheduledTask> COPIER =
      Util.deepCopier();
  private final Function<ScheduledTask, ScheduledTask> deepCopy =
      new Function<ScheduledTask, ScheduledTask>() {
        @Override public ScheduledTask apply(ScheduledTask input) {
          return deepCopy(input);
        }
      };

  private final long slowQueryThresholdNanos = SLOW_QUERY_LOG_THRESHOLD.get().as(Time.NANOSECONDS);

  private final TransactionalMap<String, ScheduledTask> tasks =
      TransactionalMap.wrap(Maps.<String, ScheduledTask>newHashMap());
  private final TransactionalMap<String, Set<String>> tasksByJobKey =
      TransactionalMap.wrap(Maps.<String, Set<String>>newHashMap());

  @Override
  public void commit() {
    tasks.commit();
    tasksByJobKey.commit();
  }

  @Override
  public void rollback() {
    tasks.rollback();
    tasksByJobKey.rollback();
  }

  @Timed("mem_storage_deep_copies")
  protected ScheduledTask deepCopy(ScheduledTask input) {
    return COPIER.apply(input);
  }

  @Timed("mem_storage_fetch_tasks")
  @Override
  public ImmutableSet<ScheduledTask> fetchTasks(TaskQuery query) {
    checkNotNull(query);

    long start = System.nanoTime();
    ImmutableSet<ScheduledTask> result = immutableMatches(query).toImmutableSet();
    long durationNanos = System.nanoTime() - start;
    Level level = (durationNanos >= slowQueryThresholdNanos) ? Level.INFO : Level.FINE;
    if (LOG.isLoggable(level)) {
      LOG.log(level, "Query took " + durationNanos + " ms: " + query);
    }

    return result;
  }

  @Timed("mem_storage_fetch_task_ids")
  @Override
  public Set<String> fetchTaskIds(TaskQuery query) {
    checkNotNull(query);

    return mutableMatches(query).transform(Tasks.SCHEDULED_TO_ID).toImmutableSet();
  }

  private Map<String, Collection<String>> taskIdsByJobKey(Iterable<ScheduledTask> toIndex) {
    return Multimaps.transformValues(
        Multimaps.index(toIndex, Tasks.SCHEDULED_TO_JOB_KEY),
        Tasks.SCHEDULED_TO_ID)
        .asMap();
  }

  @Timed("mem_storage_save_tasks")
  @Override
  public void saveTasks(Set<ScheduledTask> newTasks) {
    checkNotNull(newTasks);
    Preconditions.checkState(Tasks.ids(newTasks).size() == newTasks.size(),
        "Proposed new tasks would create task ID collision.");

    Set<ScheduledTask> immutable =
        FluentIterable.from(newTasks).transform(deepCopy).toImmutableSet();
    tasks.putAll(Maps.uniqueIndex(immutable, Tasks.SCHEDULED_TO_ID));

    // Update job key index.
    for (Map.Entry<String, Collection<String>> entry : taskIdsByJobKey(immutable).entrySet()) {
      ImmutableSet.Builder<String> newIds = ImmutableSet.builder();
      newIds.addAll(entry.getValue());
      Set<String> existingIds = tasksByJobKey.get(entry.getKey());
      if (existingIds != null) {
        newIds.addAll(existingIds);
      }

      // This is subtle but important: when modifying the items associated with a key, we must
      // re-insert rather than updating the contents of the set.  The transactional map has no
      // awareness of the contents of a value, so modifying the value set would break the
      // transactional guarantee.
      tasksByJobKey.put(entry.getKey(), newIds.build());
    }
  }

  @Timed("mem_storage_delete_all_tasks")
  @Override
  public void deleteTasks() {
    tasks.clear();
    tasksByJobKey.clear();
  }

  @Timed("mem_storage_delete_tasks")
  @Override
  public void deleteTasks(Set<String> taskIds) {
    checkNotNull(taskIds);

    // TransactionalMap does not presently support keySet modification, requiring iteration here.
    ImmutableList.Builder<ScheduledTask> gone = ImmutableList.builder();
    for (String id : taskIds) {
      ScheduledTask task = tasks.remove(id);
      if (task != null) {
        gone.add(task);
      }
    }
    for (Map.Entry<String, Collection<String>> entry : taskIdsByJobKey(gone.build()).entrySet()) {
      Set<String> existingIds = tasksByJobKey.get(entry.getKey());
      if (existingIds == null) {
        throw new IllegalStateException(
            "Index inconsistency - job for tasks not present in index: " + entry.getValue());
      }

      Set<String> newIds = Sets.difference(existingIds, ImmutableSet.copyOf(entry.getValue()));
      if (newIds.isEmpty()) {
        tasksByJobKey.remove(entry.getKey());
      } else {
        tasksByJobKey.put(entry.getKey(), newIds);
      }
    }
  }

  @Timed("mem_storage_mutate_tasks")
  @Override
  public ImmutableSet<ScheduledTask> mutateTasks(TaskQuery query, Closure<ScheduledTask> mutator) {
    checkNotNull(query);
    checkNotNull(mutator);

    ImmutableMap.Builder<String, ScheduledTask> mutated = ImmutableMap.builder();
    for (ScheduledTask original : immutableMatches(query)) {
      // Copy the object before invoking user code.  This is to support diff checking.
      ScheduledTask mutable = deepCopy.apply(original);
      mutator.execute(mutable);
      if (!original.equals(mutable)) {
        Preconditions.checkState(Tasks.id(original).equals(Tasks.id(mutable)),
            "A tasks ID may not be mutated.");

        // A diff is present - detach the mutable object from the closure's code to render
        // further mutation impossible.
        ScheduledTask updated = deepCopy.apply(mutable);
        mutated.put(Tasks.id(mutable), updated);
      }
    }

    Map<String, ScheduledTask> toUpdate = mutated.build();
    tasks.putAll(toUpdate);

    return ImmutableSet.copyOf(toUpdate.values());
  }

  private static Predicate<ScheduledTask> queryFilter(final TaskQuery query) {
    return new Predicate<ScheduledTask>() {
      @Override public boolean apply(ScheduledTask task) {
        TwitterTaskInfo config = task.getAssignedTask().getTask();
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
        if (query.getShardIdsSize() > 0) {
          if (!query.getShardIds().contains(config.getShardId())) {
            return false;
          }
        }

        return true;
      }
    };
  }

  private FluentIterable<ScheduledTask> immutableMatches(TaskQuery query) {
    return mutableMatches(query).transform(deepCopy);
  }

  private Iterable<ScheduledTask> fromIdIndex(Iterable<String> taskIds) {
    ImmutableList.Builder<ScheduledTask> matches = ImmutableList.builder();
    for (String id : taskIds) {
      ScheduledTask match = tasks.get(id);
      if (match != null) {
        matches.add(match);
      }
    }
    return matches.build();
  }

  private FluentIterable<ScheduledTask> mutableMatches(TaskQuery query) {
    // Apply the query against the working set.
    Iterable<ScheduledTask> from;
    if (query.isSetTaskIds()) {
      from = fromIdIndex(query.getTaskIds());
    } else if (Query.isJobScoped(query)) {
      Collection<String> taskIds =
          tasksByJobKey.get(Tasks.jobKey(query.getOwner().getRole(), query.getJobName()));
      if (taskIds == null) {
        from = ImmutableList.of();
      } else {
        from = fromIdIndex(taskIds);
      }
    } else {
      from = tasks.values();
    }

    return FluentIterable.from(from).filter(queryFilter(query));
  }
}
