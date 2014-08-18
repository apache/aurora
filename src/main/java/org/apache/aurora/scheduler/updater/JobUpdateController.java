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

import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;

/**
 * A controller that exposes commands to initiate and modify active job updates.
 */
interface JobUpdateController {

  /**
   * Initiates an update.
   *
   * @param update Instructions for what job to update, and how to update it.
   * @throws UpdateStateException If the update cannot be started, for example if the instructions
   *                              are invalid, or if there is already an in-progress update for the
   *                              job.
   */
  void start(IJobUpdate update) throws UpdateStateException;

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
   * Notifies the updater that an instance has changed state.
   *
   * @param instance Identifier fo the instance whose state has changed.
   * @param deleted {@code true} if the state change was a task deletion, otherwise {@code false}.
   */
  void handleStateChange(IInstanceKey instance, boolean deleted);
}
