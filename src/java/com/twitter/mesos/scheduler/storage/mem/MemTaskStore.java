package com.twitter.mesos.scheduler.storage.mem;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import org.apache.commons.lang.StringUtils;

import com.twitter.common.base.Closure;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.storage.TaskStore;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An in-memory task store.
 *
 * TODO(William Farner): Once deployed, study performance to determine if DbStorage's
 * IdComparedScheduledTask should be adopted here as well.
 *
 * TODO(William Farner): Eliminate indexes, use transactional maps.
 */
public class MemTaskStore implements TaskStore.Mutable {

  /**
   * COPIER is separate from deepCopy to capture timing information.  Only the instrumented
   * {@link #deepCopy(ScheduledTask)} should interact directly with {@code COPIER}.
   */
  private static final Function<ScheduledTask, ScheduledTask> COPIER =
      Util.deepCopier(ScheduledTask.class);
  private final Function<ScheduledTask, ScheduledTask> deepCopy =
      new Function<ScheduledTask, ScheduledTask>() {
        @Override public ScheduledTask apply(ScheduledTask input) {
          return deepCopy(input);
        }
      };

  private final Map<String, ScheduledTask> tasks = Maps.newHashMap();
  private final Set<Index> indices = ImmutableSet.of(new StatusIndex(), new IdIndex());

  @Timed("mem_storage_deep_copies")
  protected ScheduledTask deepCopy(ScheduledTask input) {
    return COPIER.apply(input);
  }

  @Timed("mem_storage_fetch_tasks")
  @Override
  public ImmutableSet<ScheduledTask> fetchTasks(TaskQuery query) {
    checkNotNull(query);

    return immutableMatches(query).toImmutableSet();
  }

  @Timed("mem_storage_fetch_task_ids")
  @Override
  public Set<String> fetchTaskIds(TaskQuery query) {
    checkNotNull(query);

    return mutableMatches(query).transform(Tasks.SCHEDULED_TO_ID).toImmutableSet();
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
    for (Index index : indices) {
      index.insert(immutable);
    }
  }

  @Timed("mem_storage_delete_all_tasks")
  @Override
  public void deleteTasks() {
    tasks.clear();
    for (Index index : indices) {
      index.invalidateAll();
    }
  }

  @Timed("mem_storage_delete_tasks")
  @Override
  public void deleteTasks(Set<String> taskIds) {
    checkNotNull(taskIds);

    Map<String, ScheduledTask> deleted = Maps.filterKeys(tasks, Predicates.in(taskIds));
    Set<ScheduledTask> deletedTasks = ImmutableSet.copyOf(deleted.values());
    tasks.keySet().removeAll(ImmutableSet.copyOf(deleted.keySet()));
    for (Index index : indices) {
      index.invalidate(deletedTasks);
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
        for (Index index : indices) {
          index.update(original, updated);
        }
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
        if (!StringUtils.isEmpty(query.getJobName())) {
          if (!query.getJobName().equals(config.getJobName())) {
            return false;
          }
        }
        if (!StringUtils.isEmpty(query.getJobKey())) {
          if (!query.getJobKey().equals(Tasks.jobKey(config))) {
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

  private FluentIterable<ScheduledTask> mutableMatches(TaskQuery query) {
    // Check for index matches.
    ImmutableList.Builder<Set<String>> indexResultBuilder = ImmutableList.builder();
    for (Index index : indices) {
      Optional<Set<String>> indexResult = index.lookup(query);
      if (indexResult.isPresent()) {
        if (indexResult.get().isEmpty()) {
          // An index matched the query but contained no results, therefore no matches in the store.
          return FluentIterable.from(ImmutableSet.<ScheduledTask>of());
        } else {
          indexResultBuilder.add(indexResult.get());
        }
      }
    }

    List<Set<String>> indexResults = indexResultBuilder.build();
    Iterable<ScheduledTask> scanItems;
    if (indexResults.isEmpty()) {
      // No index matches - a full scan is required.
      scanItems = tasks.values();
    } else {
      scanItems =
          Maps.filterKeys(tasks, Predicates.and(Lists.transform(indexResults, IN))).values();
    }

    // Apply the query against the working set.
    return FluentIterable.from(scanItems).filter(queryFilter(query));
  }

  private static final Function<Set<String>, Predicate<String>> IN =
      new Function<Set<String>, Predicate<String>>() {
        @Override public Predicate<String> apply(Set<String> set) {
          return Predicates.in(set);
        }
      };

  private interface Index {
    Optional<Set<String>> NO_INDEX_MATCH = Optional.absent();

    /**
     * Searches for task IDs in the index that match the query.
     * <p>
     * If the result is present, this means the query is applicable to the index, and any tasks
     * <b>not</b> present in the result should not be considered a match for the query.  If the
     * result is an empty set, then there are no tasks matching the query.  If the result is absent,
     * the query is not applicable to this index.
     *
     * @param query Query to perform against the index.
     * @return IDs of tasks matching this index, absent if the query does not apply to the index.
     */
    Optional<Set<String>> lookup(TaskQuery query);

    void update(ScheduledTask old, ScheduledTask updated);

    void insert(Set<ScheduledTask> tasks);

    void invalidate(Set<ScheduledTask> task);

    void invalidateAll();
  }

  private class IdIndex implements Index {
    @Override public Optional<Set<String>> lookup(TaskQuery query) {
      // There are two distinct but subtly-different conditions to handle specially here:
      //   - Task IDs unset: query did not specify an ID condition, which should match all tasks.
      //   - Task IDs empty: query specified an empty ID condition, which should have no matches.
      if (query.getTaskIds() == null) {
        return NO_INDEX_MATCH;
      }

      Set<String> matches = FluentIterable.from(tasks.keySet())
          .filter(Predicates.in(query.getTaskIds()))
          .toImmutableSet();
      return Optional.of(matches);
    }

    @Override public void update(ScheduledTask old, ScheduledTask updated) {
      // No-op.
    }

    @Override public void insert(Set<ScheduledTask> newTasks) {
      // No-op.
    }

    @Override public void invalidate(Set<ScheduledTask> task) {
      // No-op.
    }

    @Override public void invalidateAll() {
      // No-op.
    }
  }

  private static class StatusIndex implements Index {
    private final Multimap<ScheduleStatus, String> idsByStatus = HashMultimap.create();

    @Override public Optional<Set<String>> lookup(TaskQuery query) {
      if (query.getStatusesSize() == 0) {
        return NO_INDEX_MATCH;
      }

      Multimap<ScheduleStatus, String> matches =
          Multimaps.filterKeys(idsByStatus, Predicates.in(query.getStatuses()));
      return Optional.<Set<String>>of(ImmutableSet.copyOf(matches.values()));
    }

    @Override public void update(ScheduledTask old, ScheduledTask updated) {
      invalidate(ImmutableSet.of(old));
      insert(ImmutableSet.of(updated));
    }

    @Override public void insert(Set<ScheduledTask> tasks) {
      for (ScheduledTask task : tasks) {
        idsByStatus.put(task.getStatus(), Tasks.id(task));
      }
    }

    @Override public void invalidate(Set<ScheduledTask> tasks) {
      for (ScheduledTask task : tasks) {
        idsByStatus.remove(task.getStatus(), Tasks.id(task));
      }
    }

    @Override public void invalidateAll() {
      idsByStatus.clear();
    }
  }
}
