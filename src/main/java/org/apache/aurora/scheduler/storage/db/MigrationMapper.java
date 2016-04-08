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

import java.math.BigDecimal;
import java.util.List;

import org.apache.aurora.scheduler.storage.db.views.MigrationChangelogEntry;
import org.apache.ibatis.annotations.Param;

interface MigrationMapper {
  /**
   * Creates the changelog table if it does not already exist.
   */
  void bootstrapChangelog();

  /**
   * Saves the downgrade script for the supplied change id into the changelog.
   *
   * @param changeId The id of the change.
   * @param downgradeScript The script to be run when a change is rolled back.
   */
  void saveDowngradeScript(
      @Param("changeId") BigDecimal changeId,
      @Param("downgradeScript") byte[] downgradeScript);

  /**
   * Select all applied changes from the changelog.
   *
   * @return A list of changelog entries mapping only their ids and downgrade scripts.
   */
  List<MigrationChangelogEntry> selectAll();

  /**
   * Deletes the specified change from the changelog.
   *
   * @param changeId The id of the change to delete.
   */
  void delete(@Param("changeId") BigDecimal changeId);
}
