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

import com.google.common.base.Optional;

import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;

/**
 * Stores job configuration data.
 */
public interface JobStore {

  /**
   * Fetches all {@code JobConfiguration} instances.
   *
   * @return {@link Iterable} of job configurations.
   */
  Iterable<IJobConfiguration> fetchJobs();

  /**
   * Fetches the {@code JobConfiguration} for the specified {@code jobKey} if it exists.
   *
   * @param jobKey The jobKey identifying the job to be fetched.
   * @return the job configuration for the given {@code jobKey} or absent if none is found.
   */
  Optional<IJobConfiguration> fetchJob(IJobKey jobKey);

  interface Mutable extends JobStore {
    /**
     * Saves the job configuration for a job that has been accepted by the scheduler.
     * TODO(William Farner): Consider accepting SanitizedConfiguration here to require that
     * validation always happens for things entering storage.
     *
     * @param jobConfig The configuration of the accepted job.
     */
    void saveAcceptedJob(IJobConfiguration jobConfig);

    /**
     * Removes the job configuration for the job identified by {@code jobKey}.
     * If there is no stored configuration for the identified job, this method returns silently.
     *
     * @param jobKey the key identifying the job to delete.
     */
    void removeJob(IJobKey jobKey);

    /**
     * Deletes all jobs.
     */
    void deleteJobs();
  }
}
