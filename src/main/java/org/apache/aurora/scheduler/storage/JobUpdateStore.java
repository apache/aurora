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
package org.apache.aurora.scheduler.storage;

import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;

/**
 * Stores all job updates and defines methods for saving, updating and fetching job updates.
 */
public interface JobUpdateStore {

  // TODO(maxim): define get and update event APIs.

  interface Mutable extends JobUpdateStore {

    /**
     * Saves a new job update.
     *
     * @param update Update to save.
     */
    void saveJobUpdate(IJobUpdate update);

    /**
     * Saves a new job update event.
     *
     * @param event Job update event to save.
     * @param updateId Job update ID.
     */
    void saveJobUpdateEvent(IJobUpdateEvent event, String updateId);

    /**
     * Saves a new job instance update event.
     *
     * @param event Job instance update event.
     * @param updateId Job update ID.
     */
    void saveJobInstanceUpdateEvent(IJobInstanceUpdateEvent event, String updateId);
  }
}
