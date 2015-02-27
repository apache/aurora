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

import java.util.List;
import java.util.Set;

import com.google.common.base.Optional;

import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;

/**
 * Stores all job updates and defines methods for saving, updating and fetching job updates.
 */
public interface JobUpdateStore {

  /**
   * Fetches a read-only view of job update summaries.
   *
   * @param query Query to identify job update summaries with.
   * @return A read-only view of job update summaries.
   */
  List<IJobUpdateSummary> fetchJobUpdateSummaries(IJobUpdateQuery query);

  /**
   * Fetches a read-only view of job update details matching the {@code query}.
   *
   * @param query Query to identify job update details with.
   * @return A read-only list view of job update details matching the query.
   */
  List<IJobUpdateDetails> fetchJobUpdateDetails(IJobUpdateQuery query);

  /**
   * Fetches a read-only view of job update details.
   *
   * @param key Update identifier.
   * @return A read-only view of job update details.
   */
  Optional<IJobUpdateDetails> fetchJobUpdateDetails(IJobUpdateKey key);

  /**
   * Fetches a read-only view of a job update.
   *
   * @param key Update identifier.
   * @return A read-only view of job update.
   */
  Optional<IJobUpdate> fetchJobUpdate(IJobUpdateKey key);

  /**
   * Fetches a read-only view of the instructions for a job update.
   *
   * @param key Update identifier.
   * @return A read-only view of job update instructions.
   */
  Optional<IJobUpdateInstructions> fetchJobUpdateInstructions(IJobUpdateKey key);

  /**
   * Fetches a read-only view of all job update details available in the store.
   * TODO(wfarner): Generate immutable wrappers for storage.thrift structs, use an immutable object
   *                here.
   *
   * @return A read-only view of all job update details.
   */
  Set<StoredJobUpdateDetails> fetchAllJobUpdateDetails();

  /**
   * Fetches an update key based on the update ID.
   *
   * <p>
   * This is a compatibility shim for when updates were only identified by a string, and should
   * be removed in 0.9.0.
   *
   * @param updateId Update identifier.
   * @return The update key matching {@code updateId}.
   */
  Optional<IJobUpdateKey> fetchUpdateKey(String updateId);

  /**
   * Gets the lock token associated with a job update.
   *
   * @param key Update identifier.
   * @return the token associated with the update id, if it exists.
   */
  Optional<String> getLockToken(IJobUpdateKey key);

  /**
   * Fetches the events that have affected an instance within a job update.
   *
   * @param key Update identifier.
   * @param instanceId Instance to fetch events for.
   * @return Instance events in {@code key} that affected {@code instanceId}.
   */
  List<IJobInstanceUpdateEvent> fetchInstanceEvents(IJobUpdateKey key, int instanceId);

  interface Mutable extends JobUpdateStore {

    /**
     * Saves a new job update.
     *
     * <p>
     * Note: This call must be followed by the
     * {@link #saveJobUpdateEvent(IJobUpdateKey, IJobUpdateEvent)} before fetching a saved update as
     * it does not save the following required fields:
     * <ul>
     *   <li>{@link org.apache.aurora.gen.JobUpdateState#status}</li>
     *   <li>{@link org.apache.aurora.gen.JobUpdateState#createdTimestampMs}</li>
     *   <li>{@link org.apache.aurora.gen.JobUpdateState#lastModifiedTimestampMs}</li>
     * </ul>
     * The above fields are auto-populated from the update events and any attempt to fetch an update
     * without having at least one {@link IJobUpdateEvent} present in the store will return empty.
     *
     * @param update Update to save.
     * @param lockToken Optional UUID identifying the lock associated with this update.
     *                  The {@code lockToken} can be absent when terminal updates are re-inserted
     *                  during snapshot restore.
     */
    void saveJobUpdate(IJobUpdate update, Optional<String> lockToken);

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
     * Deletes all updates and update events from the store.
     */
    void deleteAllUpdatesAndEvents();

    /**
     * Prunes (deletes) old completed updates and events from the store.
     * <p>
     * At least {@code perJobRetainCount} last completed updates that completed less than
     * {@code historyPruneThreshold} ago will be kept for every job.
     *
     * @param perJobRetainCount Number of completed updates to retain per job.
     * @param historyPruneThresholdMs Earliest timestamp in the past to retain history.
     *                                Any completed updates created before this timestamp
     *                                will be pruned.
     * @return Set of pruned update keys.
     */
    Set<IJobUpdateKey> pruneHistory(int perJobRetainCount, long historyPruneThresholdMs);
  }
}
