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
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.storage.db.views.DBResourceAggregate;
import org.apache.aurora.scheduler.storage.db.views.DBSaveQuota;
import org.apache.ibatis.annotations.Param;

/**
 * MyBatis mapper class for QuotaMapper.xml.
 */
interface QuotaMapper {
  /**
   * Inserts the quota for the given {@code role}.
   *
   * @param role Role to insert quota for.
   * @param quota Quota value to store.
   * @param result Container for auto-generated ID of the inserted row.
   */
  void insert(
      @Param("role") String role,
      @Param("quota") ResourceAggregate quota,
      @Param("result") InsertResult result);

  /**
   * Insert quota resources.
   *
   * @param quotaId Quota ID to merge resources for.
   * @param values Resources to merge.
   */
  void insertResources(
      @Param("quotaId") long quotaId,
      @Param("values") Map<Integer, String> values);

  /**
   * Gets the quota assigned to a role.
   *
   * @param role Role to select quota for.
   * @return The previously-saved quota for the role, if it exists.
   */
  @Nullable
  DBResourceAggregate select(String role);

  /**
   * Gets all saved quotas.
   *
   * @return All quotas stored in the database.
   */
  List<DBSaveQuota> selectAll();

  /**
   * Removes the quota stored for a role.
   *
   * @param role Role to delete the quota entry for, if one exists.
   */
  void delete(String role);

  /**
   * Removes all stored quota records.
   */
  void truncate();
}
