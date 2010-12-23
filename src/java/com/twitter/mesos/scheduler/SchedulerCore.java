package com.twitter.mesos.scheduler;

import com.google.common.base.Preconditions;
import com.twitter.common.base.MorePreconditions;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.scheduler.JobManager.JobUpdateResult;
import com.twitter.mesos.scheduler.TaskStore.TaskState;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;

import java.util.Map;
import java.util.Set;

/**
 * Scheduling core, stores scheduler state and makes decisions about which tasks to schedule when
 * a resource offer is made.
 *
 * When a job is submitted to the scheduler core, it will store the job configuration and offer
 * the job to all configured scheduler modules, which are responsible for triggering execution of
 * the job.  Until a job is triggered by a scheduler module, it is retained in the scheduler core
 * in the PENDING state.
 *
 * TODO(wfarner): Figure out how to handle permissions for modification of running tasks.  As it
 * stands it would be quite easy to accidentally modify something that does not belong to you.
 *
 * TODO(wfarner): Clean up persistence routine to ensure that persistence happens whenever state
 * is modified.
 *
 * TODO(wfarner): Add support for machine drains via an administrator interface.  This would accept
 * a machine host name (or slave ID) and a.) kill tasks running on the machine, b.) prevent tasks
 * from being scheduled on the machine.
 *
 * @author wfarner
 */
public interface SchedulerCore extends UpdateScheduler {

  /**
   * Assigns a framework ID to the scheduler, should be called when the scheduler implementation
   * has received a successful registration signal.
   *
   * @param frameworkId Framework ID.
   */
  public void registered(String frameworkId);

  /**
   * Fetches information about all registered tasks for a job.
   *
   * @param query The query to identify tasks.
   * @return A set of task objects.
   */
  public Set<TaskState> getTasks(Query query);

  /**
   * Creates a new job, whose tasks will become candidates for scheduling.
   *
   * @param job The configuration of the job to create tasks for.
   * @throws ScheduleException If there was an error scheduling a cron job.
   * @throws ConfigurationManager.TaskDescriptionException If an invalid task description was given.
   */
  public void createJob(JobConfiguration job) throws ScheduleException,
      ConfigurationManager.TaskDescriptionException;

  /**
   * Triggers execution of a job.  This should only be called by job managers.
   *
   * @param job Job to run.
   */
  public void runJob(JobConfiguration job);

  /**
   * Offers resources to the scheduler.  If the scheduler has a pending task that is satisfied by
   * the offer, it will return the task description.
   *
   * @param slaveId ID of the slave that resources are being offered on.
   * @param slaveHost Hostname of the slave.
   * @param offerParams Parameters of the available resources on the slave.
   * @return A task description that defines the task to run, or {@code null} if there are no
   *    pending tasks that are satisfied by the slave offer.
   * @throws ScheduleException If an error occurs while attempting to schedule a task.
   */
  public TwitterTask offer(String slaveId, String slaveHost,
      Map<String, String> offerParams) throws ScheduleException;

  /**
   * Assigns a new state to tasks.
   *
   * @param query The query to identify tasks
   * @param status The new state of the tasks.
   */
  public void setTaskStatus(Query query, ScheduleStatus status);

  /**
   * Kills a specific set of tasks.
   *
   * @param query The query to identify tasks
   * @throws ScheduleException If a problem occurs with the kill request.
   */
  public void killTasks(Query query) throws ScheduleException;

  public static class RestartException extends Exception {
    public RestartException(String msg) { super(msg); }
  }

  /**
   * Schedules a restart on a set of tasks.
   *
   * @param taskIds The tasks to restart.
   * @return The set of task IDs for tasks that a restart was requested for.  A task that was
   *    requested for restart may be rejected if it was not found, or was in a non-active state.
   * @throws RestartException If the restart request could not be honored.
   */
  public Set<String> restartTasks(Set<String> taskIds) throws RestartException;

  /**
   * Triggers an update to a job.
   *
   * @param updatedJob The updated job, which must correspond with an existing job.
   * @return A description of the action that was or will be taken to update the job.
   * @throws ScheduleException If the job could not be updated.
   * @throws TaskDescriptionException If the updated job configuration was invalid.
   */
  public JobUpdateResult updateJob(JobConfiguration updatedJob) throws ScheduleException,
      TaskDescriptionException;

  // TODO(wfarner): This makes the interface look ugly, and shows how the encapsulation between
  //    SchedulreCoreImpl and ImmediateJobManager is broken.
  public JobUpdateResult doJobUpdate(JobConfiguration updatedJob) throws ScheduleException;

  /**
   * Gets the framework ID that this scheduler is registered with, or {@code null} if the framework
   * is not yet registered.
   *
   * @return The framework id.
   */
  public String getFrameworkId();

  public void stop();

  public void updateRegisteredTasks(RegisteredTaskUpdate update);

  public static class TwitterTask {
    public final String taskId;
    public final String slaveId;
    public final String taskName;
    public final Map<String, String> params;
    public final AssignedTask task;

    public TwitterTask(String taskId, String slaveId, String taskName, Map<String, String> params,
        AssignedTask task) {
      this.taskId = taskId;
      this.slaveId = MorePreconditions.checkNotBlank(slaveId);
      this.taskName = MorePreconditions.checkNotBlank(taskName);
      this.params = Preconditions.checkNotNull(params);
      this.task = Preconditions.checkNotNull(task);
    }
  }
}
