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

import javax.annotation.Nullable;

import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateDetails;

/**
 * MyBatis mapper class for JobUpdateDetailsMapper.xml
 *
 * See http://mybatis.github.io/mybatis-3/sqlmap-xml.html for more details.
 */
interface JobUpdateDetailsMapper {

  /**
   * Saves the job update, modifies the existing value if one exists.
   *
   * @param jobUpdate Job update to save/modify.
   */
  void merge(JobUpdate jobUpdate);

  /**
   * Deletes all updates and events from the database.
   */
  void truncate();

  /**
   * Gets {@link JobUpdateDetails} for the provided {@code updateId}.
   *
   * @param updateId Update ID to get.
   * @return {@link JobUpdateDetails} instance, if it exists.
   */
  @Nullable
  JobUpdateDetails selectDetails(String updateId);
}
