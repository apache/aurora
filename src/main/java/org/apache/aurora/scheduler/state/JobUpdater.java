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
package org.apache.aurora.scheduler.state;

import org.apache.aurora.scheduler.storage.entities.IJobUpdateRequest;

/**
 * Coordinates job updates and exposes control primitives (start, pause, resume, abort).
 */
public interface JobUpdater {

  /**
   * Saves the job update request and starts the update process.
   *
   * @param request Job update request.
   * @param user User who initiated the update.
   * @param lockToken Token for the lock held for this update.
   * @return Saved job update ID.
   * @throws UpdaterException Throws if update fails to start for any reason.
   */
  String startJobUpdate(IJobUpdateRequest request, String user, String lockToken)
      throws UpdaterException;

  /**
   * Thrown when job update related operation failed.
   */
  class UpdaterException extends Exception {

    public UpdaterException(String message) {
      super(message);
    }

    public UpdaterException(String message, Throwable e) {
      super(message, e);
    }
  }
}
