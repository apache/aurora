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

import java.util.Set;

import com.google.common.base.Optional;

import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;

/**
 * Stores job configuration data.
 */
public interface JobStore {

  /**
   * Fetches all {@code JobConfiguration}s for jobs owned by the manager identified by
   * {@code managerId}; if there are none then an empty set is returned.
   *
   * @param managerId The unique identifier of the manager to find registered jobs for.
   * @return the set of job configurations owned by the specififed job manager
   */
  Iterable<IJobConfiguration> fetchJobs(String managerId);

  /**
   * Fetches the {@code JobConfiguration} for the specified {@code jobKey} if it exists.
   *
   * @param managerId The unique identifier of the manager that accepted the job.
   * @param jobKey The jobKey identifying the job to be fetched.
   * @return the job configuration for the given {@code jobKey} or absent if none is found.
   */
  Optional<IJobConfiguration> fetchJob(String managerId, IJobKey jobKey);

  /**
   * Fetches all the unique manager ids that are present in the job store.
   *
   * @return The IDs of all stored job managers.
   */
  Set<String> fetchManagerIds();

  public interface Mutable extends JobStore {
    /**
     * Saves the job configuration for a job that has been accepted by the scheduler. Acts as an
     * update if the managerId already exists.
     * TODO(William Farner): Consider accepting SanitizedConfiguration here to require that
     * validation always happens for things entering storage.
     *
     * @param managerId The unique id of the manager that accepted the job.
     * @param jobConfig The configuration of the accepted job.
     */
    void saveAcceptedJob(String managerId, IJobConfiguration jobConfig);

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
