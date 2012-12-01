package com.twitter.mesos.scheduler.storage.mem;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.apache.commons.lang.StringUtils;

import com.twitter.common.base.Closure;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.storage.TaskStore;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An in-memory task store.
 *
 * TODO(William Farner): Once deployed, study performance to determine if (now gone) DbStorage's
 * IdComparedScheduledTask should be adopted here as well.
 * TODO(William Farner): With sufficient evidence from query patterns, use the task ID to
 * limit the working set for query scans.
 */
public class MemTaskStore implements TaskStore.Mutable.Transactioned {

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

  private final TransactionalMap<String, ScheduledTask> tasks =
      TransactionalMap.wrap(Maps.<String, ScheduledTask>newHashMap());

  @Override
  public void commit() {
    tasks.commit();
  }

  @Override
  public void rollback() {
    tasks.rollback();
  }

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
  }

  @Timed("mem_storage_delete_all_tasks")
  @Override
  public void deleteTasks() {
    tasks.clear();
  }

  @Timed("mem_storage_delete_tasks")
  @Override
  public void deleteTasks(Set<String> taskIds) {
    checkNotNull(taskIds);

    // TransactionalMap does not presently support keySet modification, requiring iteration here.
    for (String id : taskIds) {
      tasks.remove(id);
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
        if (query.getJobKey() != null) {
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
    // Apply the query against the working set.
    Iterable<ScheduledTask> from;
    if (query.isSetTaskIds()) {
      ImmutableList.Builder<ScheduledTask> matches = ImmutableList.builder();
      for (String id : query.getTaskIds()) {
        ScheduledTask match = tasks.get(id);
        if (match != null) {
          matches.add(match);
        }
      }
      from = matches.build();
    } else {
      from = tasks.values();
    }

    return FluentIterable.from(from).filter(queryFilter(query));
  }
}
