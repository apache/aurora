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

import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import org.apache.aurora.gen.JobUpdatePulseStatus;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.util.Objects.requireNonNull;

/**
 * A controller that exposes commands to initiate and modify active job updates.
 */
public interface JobUpdateController {

  /**
   * Metadata associated with a change to a job update.
   */
  class AuditData {
    @VisibleForTesting
    public static final int MAX_MESSAGE_LENGTH = 1024;

    private final String user;
    private final Optional<String> message;

    public AuditData(String user, Optional<String> message) {
      this.user = requireNonNull(user);
      if (message.isPresent()) {
        Preconditions.checkArgument(message.get().length() <= MAX_MESSAGE_LENGTH);
      }
      this.message = requireNonNull(message);
    }

    public String getUser() {
      return user;
    }

    public Optional<String> getMessage() {
      return message;
    }

    @Override
    public int hashCode() {
      return Objects.hash(user, message);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof AuditData)) {
        return false;
      }

      AuditData other = (AuditData) obj;
      return Objects.equals(user, other.user)
          && Objects.equals(message, other.message);
    }
  }

  /**
   * Initiates an update.
   *
   * @param update Instructions for what job to update, and how to update it.
   * @param auditData Details about the origin of this state change.
   * @throws UpdateStateException If the update cannot be started, for example if the instructions
   *                              are invalid, or if there is already an in-progress update for the
   *                              job.
   */
  void start(IJobUpdate update, AuditData auditData) throws UpdateStateException;

  /**
   * Thrown when {@link #assertNotUpdating(IJobKey)} is called and a job was updating.
   */
  class JobUpdatingException extends Exception {
    public JobUpdatingException(String msg) {
      super(msg);
    }
  }

  /**
   * Indicates whether a job is actively updating.  Note that this may include 'paused' update
   * states.
   *
   * @param job Job to check.
   * @throws JobUpdatingException if the job is actively updating.
   */
  void assertNotUpdating(IJobKey job) throws JobUpdatingException;

  /**
   * Pauses an in-progress update.
   * <p>
   * A paused update may be resumed by invoking {@link #resume(IJobUpdateKey, AuditData)}.
   *
   * @param key Update to pause.
   * @param auditData Details about the origin of this state change.
   * @throws UpdateStateException If the job update is not in a state that may be paused.
   */
  void pause(IJobUpdateKey key, AuditData auditData) throws UpdateStateException;

  /**
   * Resumes a paused in-progress update.
   * <p>
   * The outcome of this call depends on the state the updater was in prior to the pause. If the
   * updater was rolling forward, it will resume rolling forward. If it was rolling back, it will
   * resume rolling back.
   *
   * @param key Update to resume.
   * @param auditData Details about the origin of this state change.
   * @throws UpdateStateException If the job update is not in a state that may be resumed.
   */
  void resume(IJobUpdateKey key, AuditData auditData) throws UpdateStateException;

  /**
   * Aborts an in-progress update.
   * <p>
   * This will abandon the update, and make no further modifications to the job on behalf of the
   * update. An aborted update may not be resumed.
   *
   * @param key Update to abort.
   * @param auditData Details about the origin of this state change.
   * @throws UpdateStateException If there is no active update for the job.
   */
  void abort(IJobUpdateKey key, AuditData auditData) throws UpdateStateException;

  /**
   * Rollbacks an active job update.
   * <p>
   * This will rollback the update to its initial state effectively 'undoing' it.
   * The rollback is possible if update is in following states:
   * <ul>
   *    <li>ROLLING_FORWARD</li>
   *    <li>ROLL_BACK_PAUSED</li>
   *    <li>ROLL_BACK_AWAITING_PULSE</li>
   *    <li>ROLL_FORWARD_PAUSED</li>
   *    <li>ROLL_FORWARD_AWAITING_PULSE</li>
   * </ul>
   * has not reached its terminal state yet.
   *
   * @param key Update to rollback.
   * @param auditData Details about the origin of this state change.
   * @throws UpdateStateException If pre-condition is not met.
   */
  void rollback(IJobUpdateKey key, AuditData auditData) throws UpdateStateException;

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
   * This is distinct from {@link #resume(IJobUpdateKey, AuditData)} in that it does not change the
   * state of updates, but resumes after a restart of the scheduler process.
   */
  void systemResume();

  /**
   * Resets the update pulse timeout specified by the
   * {@link org.apache.aurora.gen.JobUpdateSettings#getBlockIfNoPulsesAfterMs}. Unblocks progress
   * if the update was previously blocked.
   *
   * @param key Update identifier.
   * @return Job update pulse status.
   * @throws UpdateStateException If there is no update found or update is not coordinated.
   */
  JobUpdatePulseStatus pulse(IJobUpdateKey key) throws UpdateStateException;
}
