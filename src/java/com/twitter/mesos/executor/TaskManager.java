package com.twitter.mesos.executor;

import java.util.Set;

/**
 * Defines a task manager that provides information about running tasks and minimal mutating
 * operations.
 *
 * @author William Farner
 */
public interface TaskManager {

  /**
   * Gets the full list of tasks that are currently running.
   *
   * @return Running tasks.
   */
  Iterable<Task> getLiveTasks();

  /**
   * Checks whether the manager has a task with the given id.
   *
   * @param taskId The task to check for.
   * @return {@code true} if the manager has a task wth the id, {@code false} otherwise.
   */
  boolean hasTask(String taskId);

  /**
   * Checks if a task with {@code taskId} is running.
   *
   * @param taskId The task to look up.
   * @return {@code true} if the manager has a task with the id that is currently in a runnable
   *     state, {@code false} otherwise.
   */
  boolean isRunning(String taskId);

  /**
   * Deletes record of completed tasks.  It is expected that the referenced tasks not be currently
   * running (as defined by {@link #isRunning(String)}.
   *
   * @param taskIds Ids of the task to delete.
   */
  void deleteCompletedTasks(Set<String> taskIds);

  /**
   * Adjusts the locally-retained tasks to include only the specified task IDs.
   *
   * @param retainedTaskIds IDs of tasks to retain.
   */
  void adjustRetainedTasks(Set<String> retainedTaskIds);
}
