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
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;

import java.util.Comparator;
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
public class TaskStore {
  private final static Logger LOG = Logger.getLogger(TaskStore.class.getName());

  // Maps tasks by their task IDs.
  private final Map<String, TaskState> tasks = Maps.newHashMap();

  public static class TaskState {
    public final ScheduledTask task;
    public final VolatileTaskState volatileState;

    public TaskState(ScheduledTask task) {
      this.task = new ScheduledTask(task);
      this.volatileState = new VolatileTaskState(task.getAssignedTask().getTaskId());
    }

    private TaskState(TaskState toCopy) {
      this.task = new ScheduledTask(toCopy.task);
      this.volatileState = new VolatileTaskState(toCopy.volatileState);
    }

    @Override
    public int hashCode() {
      return task.hashCode();
    }

    @Override
    public boolean equals(Object that) {
      return that instanceof TaskState && ((TaskState) that).task.equals(this.task);
    }
  }

  /**
   * Adds tasks to the store.  Tasks are copied internally, meaning that the tasks are stored in the
   * state they were in when the method is called, and further object modifications will not affect
   * the tasks.
   *
   * @param newTasks Tasks to add.
   */
  public synchronized void add(Set<ScheduledTask> newTasks) {
    Preconditions.checkArgument(!Iterables.any(newTasks,
        Predicates.compose(hasTaskId, Tasks.SCHEDULED_TO_ID)),
        "Proposed new tasks would create task ID collision.");
    Preconditions.checkArgument(
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
   * Convenience function to add an iterable of tasks.
   *
   * @param newTasks Tasks to add, which must be unique by task ID.
   */
  public synchronized void add(Iterable<ScheduledTask> newTasks) {
    Set<ScheduledTask> taskSet = ImmutableSet.copyOf(newTasks);
    Preconditions.checkArgument(taskSet.size() == Iterables.size(newTasks),
        "Tasks must be unique.");

    add(taskSet);
  }

  /**
   * Removes tasks from the store.
   *
   * @param query The query whose matching tasks should be removed.
   */
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
   * @param <E> Type of exception that the mutator may throw.
   * @return Immutable copies of the mutated tasks.
   * @throws E An exception, specified by the mutator.
   */
  public synchronized <E extends Exception> ImmutableSet<TaskState> mutate(Query query,
      final ExceptionalClosure<TaskState, E> mutator) throws E {
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
   * @param filters Additional filters to apply.
   * @return A read-only view of matching tasks.
   */
  public synchronized ImmutableSortedSet<TaskState> fetch(Query query,
      Predicate<TaskState>... filters) {
    return fetch(query, Query.SORT_BY_TASK_ID, filters);
  }

  /**
   * Fetches a read-only view of tasks matching a query and filters, with a specified sorting order.
   *
   * @param query Query to identify tasks with.
   * @param sortOrder Comparator to use when sorting returned tasks.
   * @param filters Additional filters to apply.
   * @return A read-only view of matching tasks, sorted according to the provided sort order.
   */
  public synchronized ImmutableSortedSet<TaskState> fetch(Query query,
      Comparator<TaskState> sortOrder, Predicate<TaskState>... filters) {
    return Query.sortTasks(filter(query(query), Predicates.and(filters)), sortOrder);
  }

  /**
   * Convenience method to execute a query and only retrieve the IDs of the matching tasks.
   *
   * @param query Query to identify tasks with.
   * @param filters Additional filters to apply.
   * @return IDs of the matching tasks.
   */
  public synchronized Set<String> fetchIds(Query query, Predicate<TaskState>... filters) {
    return ImmutableSet.copyOf(Iterables.transform(fetch(query, filters), Tasks.STATE_TO_ID));
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
    ImmutableSet<TaskState> results = ImmutableSet.copyOf(filter(
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
        ImmutableSet.copyOf(filter(getIntermediateResults(query.base()), query));
    vars.mutableResults.addAndGet(results.size());
    return results;
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
