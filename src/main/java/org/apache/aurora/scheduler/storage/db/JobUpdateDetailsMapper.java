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
package org.apache.aurora.scheduler.storage.db;

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Metadata;
import org.apache.aurora.gen.Range;
import org.apache.aurora.scheduler.storage.db.views.DbJobUpdate;
import org.apache.aurora.scheduler.storage.db.views.DbJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.db.views.DbStoredJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.ibatis.annotations.Param;

/**
 * MyBatis mapper class for JobUpdateDetailsMapper.xml
 *
 * See http://mybatis.github.io/mybatis-3/sqlmap-xml.html for more details.
 */
interface JobUpdateDetailsMapper {

  /**
   * Inserts new job update.
   *
   * @param jobUpdate Job update to insert.
   */
  void insert(JobUpdate jobUpdate);

  /**
   * Inserts an association between an update and a lock.
   *
   * @param key Unique update identifier.
   * @param lockToken Unique lock identifier, resulting from
   *        {@link org.apache.aurora.scheduler.storage.entities.ILock#getToken()}.
   */
  void insertLockToken(@Param("key") IJobUpdateKey key, @Param("lockToken") String lockToken);

  /**
   * Inserts a task configuration entry for an update.
   *
   * @param key Update to insert task configs for.
   * @param taskConfigRow task configuration row.
   * @param isNew Flag to identify if the task config is existing {@code false} or
   *              desired {@code true}.
   * @param result Container for auto-generated ID of the inserted job update row.
   */
  void insertTaskConfig(
      @Param("key") IJobUpdateKey key,
      @Param("taskConfigRow") long taskConfigRow,
      @Param("isNew") boolean isNew,
      @Param("result") InsertResult result);

  /**
   * Inserts the job update metadata entries for an update.
   *
   * @param key Update to insert task configs for.
   * @param metadata Set of metadata (key, value) pairs.
   */
  void insertJobUpdateMetadata(
      @Param("key") IJobUpdateKey key,
      @Param("metadata") Set<Metadata> metadata);

  /**
   * Maps inserted task config with a set of associated instance ranges.
   *
   * @param configId ID of the task config stored.
   * @param ranges Set of instance ID ranges.
   */
  void insertTaskConfigInstances(
      @Param("configId") long configId,
      @Param("ranges") Set<Range> ranges);

  /**
   * Maps update with an optional set of
   * {@link org.apache.aurora.gen.JobUpdateSettings#updateOnlyTheseInstances}.
   *
   * @param key Update to store overrides for.
   * @param ranges Instance ID ranges to associate with an update.
   */
  void insertInstanceOverrides(@Param("key") IJobUpdateKey key, @Param("ranges") Set<Range> ranges);

  /**
   * Maps update with a set of instance IDs in
   * {@link org.apache.aurora.gen.JobUpdateInstructions#desiredState}.
   *
   * @param key Update to store desired instances for.
   * @param ranges Desired instance ID ranges to associate with an update.
   */
  void insertDesiredInstances(@Param("key") IJobUpdateKey key, @Param("ranges") Set<Range> ranges);

  /**
   * Deletes all updates and events from the database.
   */
  void truncate();

  /**
   * Deletes all updates and events with update ID in {@code updates}.
   *
   * @param rowIds Row IDs of updates to delete.
   */
  void deleteCompletedUpdates(@Param("rowIds") Set<Long> rowIds);

  /**
   * Selects all distinct job key IDs associated with at least {@code perJobRetainCount} completed
   * updates or updates completed before {@code historyPruneThresholdMs}.
   *
   * @param perJobRetainCount Number of updates to keep per job.
   * @param historyPruneThresholdMs History pruning timestamp threshold.
   * @return Job key database row IDs.
   */
  Set<Long> selectJobKeysForPruning(
      @Param("retainCount") int perJobRetainCount,
      @Param("pruneThresholdMs") long historyPruneThresholdMs);

  /**
   * Groups all updates without a job lock in reverse chronological order of their created times
   * and deletes anything in excess of {@code perJobRetainCount} or older than
   * {@code historyPruneThresholdMs}.
   *
   * @param jobKeyId Job key ID to select pruning victims for.
   * @param perJobRetainCount Number of updates to keep per job.
   * @param historyPruneThresholdMs History pruning timestamp threshold.
   * @return Victims to prune.
   */
  Set<PruneVictim> selectPruneVictims(
      @Param("keyId") long jobKeyId,
      @Param("retainCount") int perJobRetainCount,
      @Param("pruneThresholdMs") long historyPruneThresholdMs);

  /**
   * Gets all job update summaries matching the provided {@code query}.
   * All {@code query} fields are ANDed together.
   *
   * @param query Query to filter results by.
   * @return Job update summaries matching the query.
   */
  List<JobUpdateSummary> selectSummaries(JobUpdateQuery query);

  /**
   * Gets details for the provided {@code key}.
   *
   * @param key Update to get.
   * @return Job update details for the provided update ID, if it exists.
   */
  @Nullable
  DbStoredJobUpdateDetails selectDetails(@Param("key") IJobUpdateKey key);

  /**
   * Gets all job update details matching the provided {@code query}.
   * All {@code query} fields are ANDed together.
   *
   * @param query Query to filter results by.
   * @return Job update details matching the query.
   */
  List<DbStoredJobUpdateDetails> selectDetailsList(JobUpdateQuery query);

  /**
   * Gets job update for the provided {@code update}.
   *
   * @param key Update to select by.
   * @return Job update for the provided update ID, if it exists.
   */
  @Nullable
  DbJobUpdate selectUpdate(@Param("key") IJobUpdateKey key);

  /**
   * Gets job update instructions for the provided {@code update}.
   *
   * @param key Update to select by.
   * @return Job update instructions for the provided update ID, if it exists.
   */
  @Nullable
  DbJobUpdateInstructions selectInstructions(@Param("key") IJobUpdateKey key);

  /**
   * Gets all stored job update details.
   *
   * @return All stored job update details.
   */
  Set<DbStoredJobUpdateDetails> selectAllDetails();

  /**
   * Gets the token associated with an update.
   *
   * @param key Update identifier.
   * @return The associated lock token, or {@code null} if no association exists.
   */
  @Nullable
  String selectLockToken(@Param("key") IJobUpdateKey key);

  /**
   * Gets job instance update events for a specific instance within an update.
   *
   * @param key Update identifier.
   * @param instanceId Instance to fetch events for.
   * @return Instance events affecting {@code instanceId} within {@code key}.
   */
  List<JobInstanceUpdateEvent> selectInstanceUpdateEvents(
      @Param("key") IJobUpdateKey key,
      @Param("instanceId") int instanceId);
}
