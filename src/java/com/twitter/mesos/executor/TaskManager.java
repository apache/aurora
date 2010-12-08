package com.twitter.mesos.executor;

/**
 * Defines a task manager that provides information about running tasks and minimal mutating
 * operations.
 *
 * @author wfarner
 */
public interface TaskManager {

  /**
   * Gets the full list of tasks that are currently running.
   *
   * @return Running tasks.
   */
  public Iterable<Task> getLiveTasks();

  /**
   * Checks whether the manager has a task with the given id.
   *
   * @param taskId The task to check for.
   * @return {@code true} if the manager has a task wth the id, {@code false} otherwise.
   */
  public boolean hasTask(int taskId);

  /**
   * Checks if a task with {@code taskId} is running.
   *
   * @param taskId The task to look up.
   * @return {@code true} if the manager has a task with the id that is currently in a runnable
   *     state, {@code false} otherwise.
   */
  public boolean isRunning(int taskId);

  /**
   * Deletes record of a completed task.  It is expected that the referenced task not be currently
   * running (as defined by {@link #isRunning(int)}.
   *
   * @param taskId Id of the task to delete.
   */
  public void deleteCompletedTask(int taskId);
}
