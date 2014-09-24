/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.updater;

import java.util.EnumSet;

import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

/**
 * A controller that exposes commands to initiate and modify active job updates.
 */
public interface JobUpdateController {

  /**
   * Different states that an active job update may be in.
   */
  EnumSet<JobUpdateStatus> ACTIVE_JOB_UPDATE_STATES =
      EnumSet.copyOf(apiConstants.ACTIVE_JOB_UPDATE_STATES);

  /**
   * Initiates an update.
   *
   * @param update Instructions for what job to update, and how to update it.
   * @param updatingUser User initiating the update.
   * @throws UpdateStateException If the update cannot be started, for example if the instructions
   *                              are invalid, or if there is already an in-progress update for the
   *                              job.
   */
  void start(IJobUpdate update, String updatingUser)
      throws UpdateStateException, UpdateConfigurationException;

  /**
   * Pauses an in-progress update.
   * <p>
   * A paused update may be resumed by invoking {@link #resume(IJobKey)}.
   *
   * @param job Job whose update should be paused.
   * @throws UpdateStateException If the job update is not in a state that may be paused.
   */
  void pause(IJobKey job) throws UpdateStateException;

  /**
   * Resumes a paused in-progress update.
   * <p>
   * The outcome of this call depends on the state the updater was in prior to the pause. If the
   * updater was rolling forward, it will resume rolling forward. If it was rolling back, it will
   * resume rolling back.
   *
   * @param job Job whose update should be resumed.
   * @throws UpdateStateException If the job update is not in a state that may be resumed.
   */
  void resume(IJobKey job) throws UpdateStateException;

  /**
   * Aborts an in-progress update.
   * <p>
   * This will abandon the update, and make no further modifications to the job on behalf of the
   * update. An aborted update may not be resumed.
   *
   * @param job Job whose update should be aborted.
   * @throws UpdateStateException If there is no active update for the job.
   */
  void abort(IJobKey job) throws UpdateStateException;

  /**
   * Notifies the updater that the state of an instance has changed. A state change could also mean
   * deletion.
   *
   * @param updatedTask The latest state for the task that changed.
   */
  void instanceChangedState(IScheduledTask updatedTask);

  /**
   * Notifies the updater that an instance was deleted.
   *
   * @param instance Identifier of the deleted instance.
   */
  void instanceDeleted(IInstanceKey instance);

  /**
   * Restores active updates that have been halted due to the scheduler restarting.
   * This is distinct from {@link #resume(IJobKey)} in that it does not change the state of
   * updates, but resumes after a restart of the scheduler process.
   */
  void systemResume();
}
