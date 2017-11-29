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

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Optional;

import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;

import static org.apache.aurora.gen.JobUpdateStatus.ABORTED;
import static org.apache.aurora.gen.JobUpdateStatus.ERROR;
import static org.apache.aurora.gen.JobUpdateStatus.FAILED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_FORWARD;

/**
 * Stores all job updates and defines methods for saving, updating and fetching job updates.
 */
public interface JobUpdateStore {

  EnumSet<JobUpdateStatus> TERMINAL_STATES = EnumSet.of(
      ROLLED_FORWARD,
      ROLLED_BACK,
      ABORTED,
      FAILED,
      ERROR
  );

  IJobUpdateQuery MATCH_ALL = IJobUpdateQuery.build(new JobUpdateQuery());

  /**
   * Fetches a read-only view of job update details matching the {@code query}.
   *
   * @param query Query to identify job update details with.
   * @return A read-only view of job update details matching the query.
   */
  List<IJobUpdateDetails> fetchJobUpdates(IJobUpdateQuery query);

  /**
   * Fetches a read-only view of job update details.
   *
   * @param key Update identifier.
   * @return A read-only view of job update details.
   */
  Optional<IJobUpdateDetails> fetchJobUpdate(IJobUpdateKey key);

  interface Mutable extends JobUpdateStore {

    /**
     * Saves a job update.
     *
     * @param update Update to save.
     */
    void saveJobUpdate(IJobUpdate update);

    /**
     * Saves a new job update event.
     *
     * @param key Update identifier.
     * @param event Job update event to save.
     */
    void saveJobUpdateEvent(IJobUpdateKey key, IJobUpdateEvent event);

    /**
     * Saves a new job instance update event.
     *
     * @param key Update identifier.
     * @param event Job instance update event.
     */
    void saveJobInstanceUpdateEvent(IJobUpdateKey key, IJobInstanceUpdateEvent event);

    /**
     * Deletes job updates.
     *
     * @param keys Keys of the updates to delete.
     */
    void removeJobUpdates(Set<IJobUpdateKey> keys);

    /**
     * Deletes all updates from the store.
     */
    void deleteAllUpdates();
  }
}
