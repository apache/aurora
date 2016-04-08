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
package org.apache.aurora.scheduler.storage.db.testmigration;

import java.math.BigDecimal;

import org.apache.ibatis.migration.MigrationScript;

public class V002_TestMigration2 implements MigrationScript {
  @Override
  public BigDecimal getId() {
    return BigDecimal.valueOf(2L);
  }

  @Override
  public String getDescription() {
    return "A second test migration";
  }

  @Override
  public String getUpScript() {
    return "CREATE TABLE V002_test_table2(id int);";
  }

  @Override
  public String getDownScript() {
    return "DROP TABLE V002_test_table2;";
  }
}
