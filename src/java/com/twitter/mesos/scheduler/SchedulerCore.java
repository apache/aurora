package com.twitter.mesos.scheduler;

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;

import com.twitter.common.base.Closure;
import com.twitter.mesos.ExecutorKey;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.gen.comm.StateUpdateResponse;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

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
 * <li>{@link #initialize()}
 * <li>{@link #start(com.twitter.common.base.Closure)}
 * <li>serve clients via other methods (normal usage)
 * <li>{@link #stop()}
 * </ol>
 *
 * @author William Farner
 */
public interface SchedulerCore extends Function<Query, Iterable<TwitterTaskInfo>> {

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
   *
   * @param killTask A closure that will kill the task with the id passed to its execute method.
   */
  void start(Closure<String> killTask);

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
  Set<ScheduledTask> getTasks(Query query);

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
   *     {@code null} if the update is being forcibly terminated.
   * @param result {@code true} if the update was successful, {@code false} otherwise.
   * @throws ScheduleException If an update for the job does not exist or if the update token is
   *     invalid.
   */
  void finishUpdate(String role, String jobName, @Nullable String updateToken, UpdateResult result)
      throws ScheduleException;

  /**
   * Offers resources to the scheduler.  If the scheduler has a pending task that is satisfied by
   * the offer, it will return the task description.
   *
   * @param offer Resource offer received from the slave.
   * @param defaultExecutorId The default executor ID to use when replying to resource offers.
   * @return A task description that defines the task to run, or {@code null} if there are no
   *    pending tasks that are satisfied by the slave offer.
   * @throws ScheduleException If an error occurs while attempting to schedule a task.
   */
  @Nullable
  TwitterTask offer(Offer offer, ExecutorID defaultExecutorId) throws ScheduleException;

  /**
   * Assigns a new state to tasks.
   *
   * @param query The query to identify tasks
   * @param status The new state of the tasks.
   * @param message Additional information about the state transition.
   */
  void setTaskStatus(Query query, ScheduleStatus status, @Nullable String message);

  /**
   * Kills a specific set of tasks.
   *
   * @param query The query to identify tasks
   * @param user Name of the user performing the kill.
   * @throws ScheduleException If a problem occurs with the kill request.
   */
  void killTasks(Query query, String user) throws ScheduleException;

  /**
   * Preempts a task in favor of another.
   *
   * @param task Task being preempted.
   * @param preemptingTask Task we are preempting in favor of.
   * @throws ScheduleException If a problem occurs while trying to perform the preemption.
   */
  void preemptTask(AssignedTask task, AssignedTask preemptingTask) throws ScheduleException;

  class RestartException extends Exception {
    RestartException(String msg) { super(msg); }
  }

  /**
   * Schedules a restart on a set of tasks.
   *
   * @param taskIds The tasks to restart.
   * @throws RestartException If the restart request could not be honored.
   */
  void restartTasks(Set<String> taskIds) throws RestartException;

  void stateUpdate(ExecutorKey executorHost, StateUpdateResponse update);

  /**
   * Should be called to allow the scheduler to gracefully shut down.
   */
  void stop();

  class TwitterTask {
    public final String taskId;
    public final String slaveId;
    public final String taskName;
    public final List<Resource> resources;
    public final AssignedTask task;

    public TwitterTask(String taskId, String slaveId, String taskName, List<Resource> resources,
        AssignedTask task) {
      this.taskId = taskId;
      this.slaveId = checkNotBlank(slaveId);
      this.taskName = checkNotBlank(taskName);
      this.resources = checkNotBlank(resources);
      this.task = checkNotNull(task);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TwitterTask)) {
        return false;
      }

      TwitterTask other = (TwitterTask) o;

      return new EqualsBuilder()
          .append(taskId, other.taskId)
          .append(slaveId, other.slaveId)
          .append(taskName, other.taskName)
          .append(resources, other.resources)
          .append(task, other.task)
          .isEquals();
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this)
          .append(taskId)
          .append(slaveId)
          .append(taskName)
          .append(resources)
          .append(task)
          .toString();
    }

    public boolean isThermosTask() {
      return task.getTask().isSetThermosConfig();
    }
  }
}
