package com.twitter.mesos.scheduler;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.twitter.common.base.Closure;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

/**
 * Stores all tasks configured with the scheduler.
 *
 * TODO(wfarner): Make this the owner of SchedulerState, and persistence.
 *
 * @author wfarner
 */
public class MapTaskStore implements TaskStore {
  private final static Logger LOG = Logger.getLogger(MapTaskStore.class.getName());

  // Maps tasks by their task IDs.
  private final Map<String, TaskState> tasks = Maps.newHashMap();

  /**
   * Adds tasks to the store.  Tasks are copied internally, meaning that the tasks are stored in the
   * state they were in when the method is called, and further object modifications will not affect
   * the tasks.
   *
   * @param newTasks Tasks to add.
   */
  @Override
  public synchronized void add(Set<ScheduledTask> newTasks) throws IllegalStateException {
    Preconditions.checkState(!Iterables.any(newTasks,
        Predicates.compose(hasTaskId, Tasks.SCHEDULED_TO_ID)),
        "Proposed new tasks would create task ID collision.");
    Preconditions.checkState(
        Sets.newHashSet(transform(newTasks, Tasks.SCHEDULED_TO_ID)).size() == newTasks.size(),
        "Proposed new tasks would create task ID collision.");

    // Do a first pass to make sure all of the values are good.
    for (ScheduledTask task : newTasks) {
      Preconditions.checkNotNull(task.getAssignedTask(), "Assigned task may not be null.");
      Preconditions.checkNotNull(task.getAssignedTask().getTask(), "Task info may not be null.");
    }

    vars.tasksAdded.addAndGet(newTasks.size());

    for (ScheduledTask task : newTasks) {
      tasks.put(task.getAssignedTask().getTaskId(), new TaskState(task));
    }
  }

  /**
   * Removes tasks from the store.
   *
   * @param query The query whose matching tasks should be removed.
   */
  @Override
  public synchronized void remove(Query query) {
    Set<String> removedIds = ImmutableSet.copyOf(transform(query(query), Tasks.STATE_TO_ID));

    LOG.info("Removing tasks " + removedIds);
    vars.tasksRemoved.addAndGet(removedIds.size());
    tasks.keySet().removeAll(removedIds);
  }

  /**
   * Convenience function for {@link #remove(Query)} to remove by ID.
   *
   * @param taskIds IDs of tasks to remove.
   */
  @Override
  public synchronized void remove(Set<String> taskIds) {
    if (taskIds.isEmpty()) return;

    remove(Query.byId(taskIds));
  }

  /**
   * Offers temporary mutable access to tasks.  If a task ID is not found, it will be silently
   * skipped, and no corresponding task will be returned.
   *
   * @param query Query to match tasks against.
   * @param mutator The mutate operation.
   * @return Immutable copies of the mutated tasks.
   */
  @Override
  public synchronized ImmutableSet<TaskState> mutate(Query query, Closure<TaskState> mutator) {
    Iterable<TaskState> mutables = mutableQuery(query);
    for (TaskState mutable : mutables) {
      mutator.execute(mutable);
    }
    return ImmutableSet.copyOf(transform(mutables, STATE_COPY));
  }

  /**
   * Fetches a read-only view of tasks matching a query and filters.  The result will be sorted by
   * the default ordering, which is by task ID.
   *
   * @param query Query to identify tasks with.
   * @return A read-only view of matching tasks.
   */
  @Override
  public synchronized ImmutableSortedSet<TaskState> fetch(Query query) {
    return Query.sortTasks(query(query), Query.SORT_BY_TASK_ID);
  }

  /**
   * Convenience method to execute a query and only retrieve the IDs of the matching tasks.
   *
   * @param query Query to identify tasks with.
   * @return IDs of the matching tasks.
   */
  @Override
  public synchronized Set<String> fetchIds(Query query) {
    return ImmutableSet.copyOf(Iterables.transform(fetch(query), Tasks.STATE_TO_ID));
  }

  /**
   * Performs a query over the current task state, where modifications to the results will not
   * be reflected in the store.
   *
   * @param query The query to execute.
   * @return A copy of all the task states matching the query.
   */
  private ImmutableSet<TaskState> query(Query query) {
    // Copy before filtering, so that client code does not access mutable state.
    vars.queries.incrementAndGet();
    ImmutableSet<TaskState> results = ImmutableSet.copyOf(filterQuery(
        transform(getIntermediateResults(query.base()), STATE_COPY), query));
    vars.queryResults.incrementAndGet();
    return results;
  }

  /**
   * Performs a query over the current task state, where the results are mutable.
   *
   * @param query The query to execute.
   * @return A copy of all the task states matching the query.
   */
  private ImmutableSet<TaskState> mutableQuery(Query query) {
    vars.mutableQueries.incrementAndGet();
    ImmutableSet<TaskState> results =
        ImmutableSet.copyOf(filterQuery(getIntermediateResults(query.base()), query));
    vars.mutableResults.addAndGet(results.size());
    return results;
  }

  private static Iterable<TaskState> filterQuery(Iterable<TaskState> tasks, Query query) {
    return filter(tasks, Predicates.and(taskMatcher(query.base()), query.postFilter()));
  }

  /**
   * Gets the intermediate (pre-filtered) results for a query by using task IDs if specified.
   *
   * @param query The query being performed.
   * @return Intermediate results for the query.
   */
  private Iterable<TaskState> getIntermediateResults(TaskQuery query) {
    if (query.getTaskIdsSize() > 0) {
      return getStateById(query.getTaskIds());
    } else {
      vars.fullScanQueries.incrementAndGet();
      return tasks.values();
    }
  }

  private final Predicate<String> hasTaskId = new Predicate<String>() {
    @Override public boolean apply(String taskId) {
      return tasks.containsKey(taskId);
    }
  };

  private final Function<String, TaskState> getById = new Function<String, TaskState>() {
    @Override public TaskState apply(String taskId) {
      return tasks.get(taskId);
    }
  };

  private static final Function<TaskState, TaskState> STATE_COPY =
      new Function<TaskState, TaskState>() {
        @Override public TaskState apply(TaskState state) {
          return new TaskState(state);
        }
      };

  /**
   * Gets task states by ID, ommitting tasks not found.
   *
   * @param taskIds IDs of tasks to look up.
   * @return Tasks found that match the given task IDs.
   */
  private Set<TaskState> getStateById(Set<String> taskIds) {
    return ImmutableSet.copyOf(Iterables.filter(Iterables.transform(taskIds, getById),
        Predicates.notNull()));
  }

  /**
   * Returns a predicate that will match tasks against the given {@code query}.
   *
   * @param query The query to use for finding tasks.
   * @return A predicate that will match tasks meeting the criteria in the query.
   */
  private static Predicate<TaskState> taskMatcher(final TaskQuery query) {
    Preconditions.checkNotNull(query);
    return new Predicate<TaskState>() {
      private boolean matches(String query, String value) {
        return StringUtils.isEmpty(query) || (value != null && value.matches(query));
      }

      private <T> boolean matches(Collection<T> collection, T item) {
        return collection == null || collection.contains(item);
      }

      @Override public boolean apply(TaskState state) {
        AssignedTask assigned = Preconditions.checkNotNull(state.task.getAssignedTask());
        TwitterTaskInfo t = Preconditions.checkNotNull(assigned.getTask());
        return matches(query.getOwner(), t.getOwner())
            && matches(query.getJobName(), t.getJobName())
            && matches(query.getJobKey(), Tasks.jobKey(t))
            && matches(query.getTaskIds(), assigned.getTaskId())
            && matches(query.getShardIds(), assigned.getTask().getShardId())
            && matches(query.getStatuses(), state.task.getStatus())
            // TODO(wfarner): Might have to be smarter here so as to not be burned by different
            //    host names for the same machine. i.e. machine1, machine1.prod.twitter.com
            && matches(query.getSlaveHost(), assigned.getSlaveHost());
      }
    };
  }

  private class Vars {
    private final AtomicLong queries = Stats.exportLong("task_store_queries");
    private final AtomicLong queryResults = Stats.exportLong("task_store_query_results");
    private final AtomicLong mutableQueries = Stats.exportLong("task_store_mutable_queries");
    private final AtomicLong mutableResults = Stats.exportLong("task_store_mutable_query_results");
    private final AtomicLong fullScanQueries = Stats.exportLong("task_store_full_scan_queries");
    private final AtomicLong tasksAdded = Stats.exportLong("task_store_tasks_added");
    private final AtomicLong tasksRemoved = Stats.exportLong("task_store_tasks_removed");

    Vars() {
      Stats.export(new StatImpl<Integer>("task_store_size") {
          @Override public Integer read() { return tasks.size(); }
      });
    }
  }
  private final Vars vars = new Vars();
}
