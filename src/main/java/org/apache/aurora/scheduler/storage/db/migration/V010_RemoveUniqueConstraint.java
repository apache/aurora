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
package org.apache.aurora.scheduler.storage.db.migration;

import java.math.BigDecimal;

import org.apache.ibatis.migration.MigrationScript;

public class V010_RemoveUniqueConstraint implements MigrationScript {
  @Override
  public BigDecimal getId() {
    return BigDecimal.valueOf(10L);
  }

  @Override
  public String getDescription() {
    return "Remove unique constraint in task_config_volumes";
  }

  @Override
  public String getUpScript() {
    // The constraint name is taken from the schema so it is always constant.
    return "ALTER TABLE IF EXISTS task_config_volumes DROP CONSTRAINT IF EXISTS CONSTRAINT_654B;";
  }

  @Override
  public String getDownScript() {
    return "ALTER TABLE IF EXISTS task_config_volumes ADD UNIQUE(task_config_id);";
  }
}
