/**
 *
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

import javax.annotation.Nullable;

/**
 * Stores data specific to the scheduler itself.
 */
public interface SchedulerStore {

  /**
   * Fetches the last saved framework id.  If none is saved, null can be returned.
   *
   * @return the last saved framework id
   */
  @Nullable String fetchFrameworkId();

  public interface Mutable extends SchedulerStore {
    /**
     * Stores the given framework id overwriting any previously saved id.
     *
     * @param frameworkId The framework id to store.
     */
    void saveFrameworkId(String frameworkId);
  }
}
