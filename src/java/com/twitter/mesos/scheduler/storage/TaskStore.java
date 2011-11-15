package com.twitter.mesos.scheduler.storage;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import com.twitter.common.base.Closure;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.scheduler.Query;

/**
 * Stores all tasks configured with the scheduler.
 *
 * @author John Sirois
 */
public interface TaskStore {

  /**
   * Saves tasks to the store.  Tasks are copied internally, meaning that the tasks are stored in
   * the state they were in when the method is called, and further object modifications will not
   * affect the tasks.  If any of the tasks already exist in the store, they will be overwritten
   * by the supplied {@code newTasks}.
   *
   * @param tasks Tasks to add.
   */
  void saveTasks(Set<ScheduledTask> tasks);

  /**
   * Removes tasks from the store.
   *
   * @param query The query whose matching tasks should be removed.
   */
  void removeTasks(Query query);

  /**
   * Convenience function for {@link #removeTasks(Query)} to remove by ID.
   *
   * @param taskIds IDs of tasks to remove.
   */
  void removeTasks(Set<String> taskIds);

  /**
   * Offers temporary mutable access to tasks.  If a task ID is not found, it will be silently
   * skipped, and no corresponding task will be returned.
   *
   * @param query Query to match tasks against.
   * @param mutator The mutate operation.
   * @return Immutable copies of only the tasks that were mutated.
   */
  ImmutableSet<ScheduledTask> mutateTasks(Query query, Closure<ScheduledTask> mutator);

  /**
   * Fetches a read-only view of tasks matching a query and filters.
   *
   * @param query Query to identify tasks with.
   * @return A read-only view of matching tasks.
   */
  ImmutableSet<ScheduledTask> fetchTasks(Query query);

  /**
   * Convenience method to execute a query and only retrieve the IDs of the matching tasks.
   *
   * @param query Query to identify tasks with.
   * @return IDs of the matching tasks.
   */
  Set<String> fetchTaskIds(Query query);

  /**
   * Upgrade the task information in the task states table.
   */
  void upgradeTaskStorage();
}
