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

import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.TaskConfig;
import org.apache.ibatis.annotations.Param;

/**
 * MyBatis mapper class for JobUpdateDetailsMapper.xml
 *
 * See http://mybatis.github.io/mybatis-3/sqlmap-xml.html for more details.
 */
interface JobUpdateDetailsMapper {

  /**
   * Inserts new {@link JobUpdate}.
   *
   * @param jobUpdate Job update to insert.
   */
  void insert(JobUpdate jobUpdate);

  /**
   * Inserts {@link TaskConfig} entries associated with the current update.
   *
   * @param updateId Update ID to insert task configs for.
   * @param taskConfig {@link TaskConfig} to insert.
   * @param isNew Flag to identify if the task config is existing {@code false} or
   *              desired {@code true}.
   * @param result Container for auto-generated ID of the inserted job update row.
   */
  void insertTaskConfig(
      @Param("updateId") String updateId,
      @Param("config") TaskConfig taskConfig,
      @Param("isNew") boolean isNew,
      @Param("result") InsertResult result);

  /**
   * Maps inserted task config with a set of associated instance ranges.
   *
   * @param configId ID of the {@link TaskConfig} stored.
   * @param ranges Set of instance ID ranges.
   */
  void insertTaskConfigInstances(
      @Param("configId") long configId,
      @Param("ranges") Set<Range> ranges);

  /**
   * Maps update with an optional set of
   * {@link org.apache.aurora.gen.JobUpdateSettings#updateOnlyTheseInstances}.
   *
   * @param updateId Update ID to store overrides for.
   * @param ranges Instance ID ranges to associate with a task configuration.
   */
  void insertInstanceOverrides(
      @Param("updateId") String updateId,
      @Param("ranges") Set<Range> ranges);

  /**
   * Deletes all updates and events from the database.
   */
  void truncate();

  /**
   * Gets all {@link JobUpdateSummary} matching the provided {@code query}.
   * All {@code query} fields are ANDed together.
   *
   * @param query Query to filter results by.
   * @return Job update summaries matching the query.
   */
  List<JobUpdateSummary> selectSummaries(JobUpdateQuery query);

  /**
   * Gets {@link JobUpdateDetails} for the provided {@code updateId}.
   *
   * @param updateId Update ID to get.
   * @return {@link JobUpdateDetails} instance, if it exists.
   */
  @Nullable
  JobUpdateDetails selectDetails(String updateId);

  /**
   * Gets all stored {@link JobUpdateDetails}.
   *
   * @return All stored job update details.
   */
  List<JobUpdateDetails> selectAllDetails();
}
