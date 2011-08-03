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
   * @return Immutable copies of only the tasks that were mutated.
   */
  ImmutableSet<ScheduledTask> mutate(Query query, Closure<ScheduledTask> mutator);

  /**
   * Applies the given updates to the store as over-writes.  If any of the supplied {@code updates}
   * does not represent an existing stored task an exception will be thrown.
   *
   * @param updates The updates to apply
   */
  void update(Set<ScheduledTask> updates);

  /**
   * Fetches a read-only view of tasks matching a query and filters.
   *
   * @param query Query to identify tasks with.
   * @return A read-only view of matching tasks.
   */
  ImmutableSet<ScheduledTask> fetch(Query query);

  /**
   * Convenience method to execute a query and only retrieve the IDs of the matching tasks.
   *
   * @param query Query to identify tasks with.
   * @return IDs of the matching tasks.
   */
  Set<String> fetchIds(Query query);
}
