package com.twitter.mesos.scheduler;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.mesos.gen.ScheduledTask;

import java.util.Set;

/**
 * Stores all tasks configured with the scheduler.
 *
 * @author jsirois
 */
public interface TaskStore {

  /**
   * Adds tasks to the store.  Tasks are copied internally, meaning that the tasks are stored in the
   * state they were in when the method is called, and further object modifications will not affect
   * the tasks.
   *
   * @param newTasks Tasks to add.
   */
  void add(Set<ScheduledTask> newTasks);

  /**
   * Removes tasks from the store.
   *
   * @param query The query whose matching tasks should be removed.
   */
  void remove(Query query);

  /**
   * Convenience function for {@link #remove(Query)} to remove by ID.
   *
   * @param taskIds IDs of tasks to remove.
   */
  void remove(Set<String> taskIds);

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
  <E extends Exception> ImmutableSet<TaskState> mutate(Query query,
      ExceptionalClosure<TaskState, E> mutator) throws E;

  /**
   * Fetches a read-only view of tasks matching a query and filters.  The result will be sorted by
   * the default ordering, which is by task ID.
   *
   * @param query Query to identify tasks with.
   * @return A read-only view of matching tasks.
   */
  ImmutableSortedSet<TaskState> fetch(Query query);

  /**
   * Convenience method to execute a query and only retrieve the IDs of the matching tasks.
   *
   * @param query Query to identify tasks with.
   * @return IDs of the matching tasks.
   */
  Set<String> fetchIds(Query query);

  static class TaskState {
    public final ScheduledTask task;
    public final VolatileTaskState volatileState;

    public TaskState(ScheduledTask task) {
      this.task = new ScheduledTask(task);
      this.volatileState = new VolatileTaskState(task.getAssignedTask().getTaskId());
    }

    public TaskState(TaskState toCopy) {
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
}
