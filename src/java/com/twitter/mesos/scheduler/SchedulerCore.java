package com.twitter.mesos.scheduler;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Optional;

import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ShardUpdateResult;
import com.twitter.mesos.gen.TaskQuery;
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
 */
public interface SchedulerCore extends RegisteredListener {

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
  @Override
  void registered(String frameworkId);

  /**
   * Creates a new job, whose tasks will become candidates for scheduling.
   *
   * @param parsedConfiguration The configuration of the job to create tasks for.
   * @throws ScheduleException If there was an error scheduling a cron job.
   * @throws ConfigurationManager.TaskDescriptionException If an invalid task description was given.
   */
  void createJob(ParsedConfiguration parsedConfiguration)
      throws ScheduleException, ConfigurationManager.TaskDescriptionException;

  /**
   * Starts a cron job immediately.
   *
   * @param role Owner of the job.
   * @param job Name of the job.
   * @throws ScheduleException If the specified job does not exist, or is not a cron job.
   */
  // TODO(ksweeney): refactor to use JobKey
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
   * @param parsedConfiguration Updated job configuration.
   * @throws ScheduleException If there was an error in scheduling an update when no active tasks
   *                           are found for a job or an update for the job is already in progress.
   * @return A unique update token if an update must be coordinated through
   *         {@link #updateShards(Identity, String, Set, String)}and
   *         {@link #finishUpdate(Identity, String, Optional, UpdateResult)}, or an absent value if
   * the update was completed in-place and no further action is necessary.
   */
  Optional<String> initiateJobUpdate(ParsedConfiguration parsedConfiguration)
      throws ScheduleException;

  /**
   * Initiates an update on shards within a job.
   * Requires that startUpdate was called for the job first.
   *
   * @param identity The job owner and invoking user.
   * @param jobName Job being updated.
   * @param shards Shards to be updated.
   * @param updateToken A unique string identifying the update, must be provided from
   *                    {@link #initiateJobUpdate(ParsedConfiguration)}.
   * @throws ScheduleException If there was an error in updating the state to UPDATING.
   * @return The action taken on each of the shards.
   */
  // TODO(ksweeney): Refactor this to take a JobKey
  Map<Integer, ShardUpdateResult> updateShards(
      Identity identity,
      String jobName,
      Set<Integer> shards,
      String updateToken) throws ScheduleException;

  /**
   * Initiates a rollback of the specified shards.
   * Requires that startUpdate was called for the job first.
   *
   * @param identity The job owner and invoking user.
   * @param jobName Name of the job being updated.
   * @param shards Shards to be updated.
   * @param updateToken A unique string identifying the update, must be provided from
   *                    {@link #initiateJobUpdate(ParsedConfiguration)}
   * @throws ScheduleException If there was an error in updating the state to ROLLBACK.
   * @return The action taken on each of the shards.
   */
  // TODO(ksweeney): Refactor this to take a JobKey
  Map<Integer, ShardUpdateResult> rollbackShards(
      Identity identity,
      String jobName,
      Set<Integer> shards,
      String updateToken) throws ScheduleException;

  /**
   * Completes an update.
   *
   * @param identity The job owner and invoking user.
   * @param jobName Name of the job being updated.
   * @param updateToken The update token provided from
   *                    {@link #initiateJobUpdate(ParsedConfiguration)},
   *                    or not present if the update is being forcibly terminated.
   * @param result {@code true} if the update was successful, {@code false} otherwise.
   * @throws ScheduleException If an update for the job does not exist or if the update token is
   *                           invalid.
   */
  // TODO(ksweeney): Refactor this to take a JobKey
  void finishUpdate(
      Identity identity,
      String jobName,
      Optional<String> updateToken,
      UpdateResult result) throws ScheduleException;

  /**
   * Assigns a new state to tasks.
   *
   * @param query The query to identify tasks
   * @param status The new state of the tasks.
   * @param message Additional information about the state transition.
   */
  void setTaskStatus(TaskQuery query, ScheduleStatus status, Optional<String> message);

  /**
   * Kills a specific set of tasks.
   *
   * @param query The query to identify tasks
   * @param user Name of the user performing the kill.
   * @throws ScheduleException If a problem occurs with the kill request.
   */
  void killTasks(TaskQuery query, String user) throws ScheduleException;

  /**
   * Initiates a restart of shards within an active job.
   *
   * @param role Role owning the shards to restart.
   * @param jobName Job containing the shards.
   * @param shards Shards to be restarted.
   * @param requestingUser User performing the restart action.
   * @throws ScheduleException If there are no matching active shards.
   */
  void restartShards(String role, String jobName, Set<Integer> shards, String requestingUser)
      throws ScheduleException;

  /**
   * Preempts a task in favor of another.
   *
   * @param task Task being preempted.
   * @param preemptingTask Task we are preempting in favor of.
   * @throws ScheduleException If a problem occurs while trying to perform the preemption.
   */
  void preemptTask(AssignedTask task, AssignedTask preemptingTask) throws ScheduleException;

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
