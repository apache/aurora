package com.twitter.mesos.scheduler;

import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;

import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;

/**
 * Scheduling core, stores scheduler state and makes decisions about which tasks to schedule when
 * a resource offer is made.
 *
 * When a job is submitted to the scheduler core, it will store the job configuration and offer
 * the job to all configured scheduler modules, which are responsible for triggering execution of
 * the job.  Until a job is triggered by a scheduler module, it is retained in the scheduler core
 * in the PENDING state.
 *
 * This interface imposes the following lifecycle:
 * <ol>
 * <li>[construct]
 * <li>{@link #prepare()}
 * <li>{@link #initialize()}
 * <li>{@link #start()}
 * <li>serve clients via other methods (normal usage)
 * <li>{@link #stop()}
 * </ol>
 *
 * @author William Farner
 */
public interface SchedulerCore
    extends Function<TaskQuery, Iterable<TwitterTaskInfo>>, TaskLauncher {

  /**
   * Prompts the scheduler to prepare for possible activation as the leading scheduler.  This
   * method should not block.
   */
  void prepare();

  /**
   * Initializes the scheduler's storage system and returns the last framework ID assigned to this
   * scheduler.
   *
   * @return The last assigned framework ID or {@code null} if none has been assigned yet.
   */
  @Nullable
  String initialize();

  /**
   * Prepares the scheduler for serving traffic.
   */
  void start();

  /**
   * Assigns a framework ID to the scheduler, should be called when the scheduler implementation
   * has received a successful registration signal.
   *
   * @param frameworkId Framework ID.
   */
  void registered(String frameworkId);

  /**
   * Fetches information about all registered tasks for a job.
   *
   * @param query The query to identify tasks.
   * @return A set of task objects.
   */
  Set<ScheduledTask> getTasks(TaskQuery query);

  /**
   * Creates a new job, whose tasks will become candidates for scheduling.
   *
   * @param job The configuration of the job to create tasks for.
   * @throws ScheduleException If there was an error scheduling a cron job.
   * @throws ConfigurationManager.TaskDescriptionException If an invalid task description was given.
   */
  void createJob(JobConfiguration job) throws ScheduleException,
      ConfigurationManager.TaskDescriptionException;

  /**
   * Starts a cron job immediately.
   *
   * @param role Owner of the job.
   * @param job Name of the job.
   * @throws ScheduleException If the specified job does not exist, or is not a cron job.
   */
  void startCronJob(String role, String job) throws ScheduleException;

  /**
   * Triggers execution of a job.  This should only be called by job managers.
   *
   * @param job Job to run.
   */
  void runJob(JobConfiguration job);

  /**
   * Registers an update for a job.
   *
   * @param job Updated job configuration.
   * @throws ScheduleException If there was an error in scheduling an update when no active tasks
   *     are found for a job or an update for the job is already in progress.
   * @throws ConfigurationManager.TaskDescriptionException If an invalid task description was given.
   * @return A unique string identifying the update.
   */
  String startUpdate(JobConfiguration job)
      throws ScheduleException, ConfigurationManager.TaskDescriptionException;

  /**
   * Initiates an update on shards within a job.
   * Requires that startUpdate was called for the job first.
   *
   * @param role The job owner.
   * @param jobName Job being updated.
   * @param shards Shards to be updated.
   * @param updateToken A unique string identifying the update, must be provided from
   *     {@link #startUpdate(JobConfiguration)}.
   * @throws ScheduleException If there was an error in updating the state to UPDATING.
   */
  void updateShards(String role, String jobName, Set<Integer> shards, String updateToken)
      throws ScheduleException;

  /**
   * Initiates a rollback of the specified shards.
   * Requires that startUpdate was called for the job first.
   *
   * @param role The job owner.
   * @param jobName Name of the job being updated.
   * @param shards Shards to be updated.
   * @param updateToken A unique string identifying the update, must be provided from
   *     {@link #startUpdate(JobConfiguration)}
   * @throws ScheduleException If there was an error in updating the state to ROLLBACK.
   */
  void rollbackShards(String role, String jobName, Set<Integer> shards, String updateToken)
      throws ScheduleException;

  /**
   * Completes an update.
   *
   * @param role The job owner.
   * @param jobName Name of the job being updated.
   * @param updateToken The update token provided from {@link #startUpdate(JobConfiguration)}, or
   *     not present if the update is being forcibly terminated.
   * @param result {@code true} if the update was successful, {@code false} otherwise.
   * @throws ScheduleException If an update for the job does not exist or if the update token is
   *     invalid.
   */
  void finishUpdate(String role, String jobName, Optional<String> updateToken, UpdateResult result)
      throws ScheduleException;

  /**
   * Assigns a new state to tasks.
   *
   * @param query The query to identify tasks
   * @param status The new state of the tasks.
   * @param message Additional information about the state transition.
   */
  void setTaskStatus(TaskQuery query, ScheduleStatus status, @Nullable String message);

  /**
   * Kills a specific set of tasks.
   *
   * @param query The query to identify tasks
   * @param user Name of the user performing the kill.
   * @throws ScheduleException If a problem occurs with the kill request.
   */
  void killTasks(TaskQuery query, String user) throws ScheduleException;

  /**
   * Preempts a task in favor of another.
   *
   * @param task Task being preempted.
   * @param preemptingTask Task we are preempting in favor of.
   * @throws ScheduleException If a problem occurs while trying to perform the preemption.
   */
  void preemptTask(AssignedTask task, AssignedTask preemptingTask) throws ScheduleException;

  /**
   * Thrown when a task restart failed.
   */
  class RestartException extends Exception {
    RestartException(String msg) {
      super(msg);
    }
  }

  /**
   * Schedules a restart on a set of tasks.
   *
   * @param taskIds The tasks to restart.
   * @throws RestartException If the restart request could not be honored.
   */
  void restartTasks(Set<String> taskIds) throws RestartException;

  /**
   * Indicates to the scheduler that tasks were deleted on the assigned host.
   *
   * @param taskIds IDs of tasks that were deleted.
   */
  void tasksDeleted(Set<String> taskIds);

  /**
   * Should be called to allow the scheduler to gracefully shut down.
   */
  void stop();
}
