package com.twitter.mesos.scheduler;

import java.util.Set;

import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.UpdateConfigResponse;
import com.twitter.mesos.scheduler.JobManager.JobUpdateResult;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;

/**
 * Handles duties related to updating a job and receiving rolling updates.
 *
 * @author wfarner
 */
public interface UpdateScheduler {

  public UpdateConfigResponse getUpdateConfig(String updateToken) throws UpdateException;

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

  /**
   * Cancels an update by token.
   *
   * @param updateToken The token of the job update to cancel.
   * @throws UpdateException If an update was not found matching the token.
   */
  public void updateFinished(String updateToken) throws UpdateException;

  /**
   * Identical to {@link #updateFinished(String)}, but allows canceling by owner and job name.
   *
   * @param owner The owner of the job to cancel an update for.
   * @param jobName The name of the job to cancel an update for.
   * @throws UpdateException If an update was not found for the job spec.
   */
  public void updateFinished(String owner, String jobName) throws UpdateException;
}
