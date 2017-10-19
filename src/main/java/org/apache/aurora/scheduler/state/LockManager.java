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

import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.ILock;

/**
 * Defines all {@link ILock} primitives like: acquire, release, validate.
 */
public interface LockManager {
  /**
   * Creates, saves and returns a new {@link ILock} with the specified {@link IJobKey}.
   * This method is not re-entrant, i.e. attempting to acquire a lock with the
   * same key would throw a {@link LockException}.
   *
   * @param job The job being locked.
   * @param user Name of the user requesting a lock.
   * @return A new ILock instance.
   * @throws LockException In case the lock with specified key already exists.
   */
  ILock acquireLock(IJobKey job, String user) throws LockException;

  /**
   * Releases (removes) the lock associated with {@code job} from the system.
   *
   * @param job the job to unlock.
   */
  void releaseLock(IJobKey job);

  /**
   * Thrown when {@link ILock} related operation failed.
   */
  class LockException extends Exception {
    public LockException(String msg) {
      super(msg);
    }
  }
}
