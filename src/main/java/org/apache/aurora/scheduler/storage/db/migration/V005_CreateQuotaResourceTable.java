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

import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.ibatis.migration.MigrationScript;

public class V005_CreateQuotaResourceTable implements MigrationScript {
  @Override
  public BigDecimal getId() {
    return BigDecimal.valueOf(5L);
  }

  @Override
  public String getDescription() {
    return "Create the quota_resource table.";
  }

  @Override
  public String getUpScript() {
    return "CREATE TABLE IF NOT EXISTS quota_resource("
        + "id IDENTITY,"
        + "quota_id BIGINT NOT NULL REFERENCES quotas(id) ON DELETE CASCADE,"
        + "type_id INT NOT NULL REFERENCES resource_types(id),"
        + "value VARCHAR NOT NULL,"
        + "UNIQUE(quota_id, type_id)"
        + ");\n"
        + migrateScript();
  }

  @Override
  public String getDownScript() {
    return "DROP TABLE IF EXISTS quota_resource;";
  }

  private static String migrateScript() {
    return "MERGE INTO quota_resource(quota_id, type_id, value) "
        + "KEY(quota_id, type_id, value) "
        + "SELECT q.id, rt.id, q.num_cpus FROM quotas q "
        + "JOIN resource_types rt ON rt.name = "
        + String.format("'%s';%n", ResourceType.CPUS.name())

        + "MERGE INTO quota_resource(quota_id, type_id, value) "
        + "KEY(quota_id, type_id, value) "
        + "SELECT q.id, rt.id, q.ram_mb FROM quotas q "
        + "JOIN resource_types rt ON rt.name = "
        + String.format("'%s';%n", ResourceType.RAM_MB.name())

        + "MERGE INTO quota_resource(quota_id, type_id, value) "
        + "KEY(quota_id, type_id, value) "
        + "SELECT q.id, rt.id, q.disk_mb FROM quotas q "
        + "JOIN resource_types rt ON rt.name = "
        + String.format("'%s';%n", ResourceType.DISK_MB.name());
  }
}
