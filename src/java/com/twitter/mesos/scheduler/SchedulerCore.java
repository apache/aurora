package com.twitter.mesos.scheduler;

import com.google.common.base.Predicate;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TrackedTask;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import mesos.SchedulerDriver;
import mesos.SlaveOffer;
import mesos.TaskDescription;

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
public interface SchedulerCore {

  /**
   * Assigns a framework ID to the scheduler, should be called when the scheduler implementation
   * has received a successful registration signal.
   *
   * @param driver The registered driver reference.
   * @param frameworkId Framework ID.
   */
  public void registered(SchedulerDriver driver, String frameworkId);

  /**
   * Fetches information about all registered tasks for a job.
   *
   * @param query The query to identify tasks.
   * @param filters Additional filters to apply.
   * @return An iterable of task objects.
   */
  public Iterable<TrackedTask> getTasks(final TaskQuery query, Predicate<TrackedTask>... filters);

  /**
   * Checks whether the scheduler has a job matching the owner/jobName.
   *
   * @param owner The owner to look up.
   * @param jobName The job name to look up.
   * @return {@code true} if the scheduler has a job for the given owner and job name,
   *    {@code false} otherwise.
   */
  public boolean hasActiveJob(final String owner, final String jobName);

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
   * @param slaveOffer The slave offer.
   * @return A task description that defines the task to run, or {@code null} if there are no
   *    pending tasks that are satisfied by the slave offer.
   * @throws ScheduleException If an error occurs while attempting to schedule a task.
   */
  public TaskDescription offer(final SlaveOffer slaveOffer) throws ScheduleException;

  /**
   * Assigns a new state to tasks.
   *
   * @param query The query to identify tasks
   * @param status The new state of the tasks.
   */
  public void setTaskStatus(TaskQuery query, final ScheduleStatus status);

  /**
   * Kills a specific set of tasks.
   *
   * @param query The query to identify tasks
   * @throws ScheduleException If a problem occurs with the kill request.
   */
  public void killTasks(final TaskQuery query) throws ScheduleException;

  /**
   * Schedules a restart on a set of tasks.
   *
   * @param query The query to identify tasks.
   */
  public void restartTasks(final TaskQuery query);

  /**
   * Gets the framework ID that this scheduler is registered with, or {@null} if the framework is
   * not yet registered.
   *
   * @return The framework id.
   */
  public String getFrameworkId();

  public void updateRegisteredTasks(RegisteredTaskUpdate update);
}
