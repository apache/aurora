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

public class V006_PopulateTierField implements MigrationScript {

  private static final String PREFERRED_TIER_NAME = "preferred";
  private static final String PREEMPTIBLE_TIER_NAME = "preemptible";

  @Override
  public BigDecimal getId() {
    return BigDecimal.valueOf(6L);
  }

  @Override
  public String getDescription() {
    return "Populate task_configs.tier field.";
  }

  @Override
  public String getUpScript() {
    return "UPDATE task_configs "
        + String.format(
            "SET tier = CASEWHEN(production = 1, '%s', '%s') ",
            PREFERRED_TIER_NAME,
            PREEMPTIBLE_TIER_NAME)
        + "WHERE tier IS NULL;";
  }

  @Override
  public String getDownScript() {
    return "UPDATE task_configs "
        + "SET production = 1 "
        + String.format("WHERE tier = '%s';", PREFERRED_TIER_NAME);
  }
}
