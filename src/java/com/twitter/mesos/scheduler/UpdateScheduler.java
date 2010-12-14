package com.twitter.mesos.scheduler;

import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.scheduler.JobManager.JobUpdateResult;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;

import java.util.Set;

/**
 * Handles duties related to updating a job and receiving rolling updates.
 *
 * @author wfarner
 */
public interface UpdateScheduler {

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

  public static class UpdateException extends Exception {
    public UpdateException(String msg) { super(msg); }
  }

  public Set<String> updateShards(String updateToken, Set<Integer> restartShards, boolean rollBack)
      throws UpdateException;

  public void updateFinished(String updateToken) throws UpdateException;
}
