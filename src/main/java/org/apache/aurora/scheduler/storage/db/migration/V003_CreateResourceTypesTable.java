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
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.ibatis.migration.MigrationScript;

public class V003_CreateResourceTypesTable implements MigrationScript {
  @Override
  public BigDecimal getId() {
    return BigDecimal.valueOf(3L);
  }

  @Override
  public String getDescription() {
    return "Create the resource_types table.";
  }
  @Override
  public String getUpScript() {
    return "CREATE TABLE IF NOT EXISTS resource_types("
        + "id INT PRIMARY KEY,"
        + "name VARCHAR NOT NULL,"
        + "UNIQUE(name)"
        + ");\n"
        + populateScript();
  }

  @Override
  public String getDownScript() {
    return "DROP TABLE IF EXISTS resource_types;";
  }

  private static String populateScript() {
    return Arrays.stream(ResourceType.values())
        .map(e -> String.format(
            "MERGE INTO resource_types(id, name) KEY(name) VALUES (%d, '%s');",
            e.getValue(),
            e.name()))
        .collect(Collectors.joining("\n"));
  }
}
