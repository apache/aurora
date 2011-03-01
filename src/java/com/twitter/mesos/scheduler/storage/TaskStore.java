package com.twitter.mesos.scheduler.storage;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.twitter.common.base.Closure;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.scheduler.Query;

import java.util.Set;

/**
 * Stores all tasks configured with the scheduler.
 *
 * @author John Sirois
 */
public interface TaskStore {

  /**
   * Adds tasks to the store.  Tasks are copied internally, meaning that the tasks are stored in the
   * state they were in when the method is called, and further object modifications will not affect
   * the tasks.
   *
   * @param newTasks Tasks to add.
   * @throws IllegalStateException if any of the tasks were already in the store
   */
  void add(Set<ScheduledTask> newTasks) throws IllegalStateException;

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
   * @return Immutable copies of the mutated tasks.
   */
  ImmutableSet<ScheduledTask> mutate(Query query, Closure<ScheduledTask> mutator);

  /**
   * Fetches a read-only view of tasks matching a query and filters.  The result will be sorted by
   * the default ordering, which is by task ID.
   *
   * @param query Query to identify tasks with.
   * @return A read-only view of matching tasks.
   */
  ImmutableSortedSet<ScheduledTask> fetch(Query query);

  /**
   * Convenience method to execute a query and only retrieve the IDs of the matching tasks.
   *
   * @param query Query to identify tasks with.
   * @return IDs of the matching tasks.
   */
  Set<String> fetchIds(Query query);
}
