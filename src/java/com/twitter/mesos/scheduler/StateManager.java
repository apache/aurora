package com.twitter.mesos.scheduler;

import java.util.Set;

import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;

/**
 * Thin interface for the state manager.
 *
 * @author William Farner
 */
public interface StateManager {

  /**
   * Fetches tasks matching a query.
   *
   * @param query Query to perform.
   * @return Tasks found matching the query.
   */
  Set<ScheduledTask> fetchTasks(TaskQuery query);

  /**
   * Deletes tasks with the given task IDs.
   *
   * @param taskIds IDs of tasks to delete.
   */
  void deleteTasks(Set<String> taskIds);
}
