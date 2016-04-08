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

import java.sql.SQLException;

/**
 * Manage schema migrations.
 */
public interface MigrationManager {
  /**
   * Perform a migration, upgrading the schema if there are unapplied changes or downgrading it if
   * there are applies changes which do not exist in the current version.
   *
   * @throws SQLException In the event of a problem performing the migration.
   */
  void migrate() throws SQLException;
}
