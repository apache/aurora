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

public class V004_CreateTaskResourceTable implements MigrationScript {
  @Override
  public BigDecimal getId() {
    return BigDecimal.valueOf(4L);
  }

  @Override
  public String getDescription() {
    return "Create the task_resource table.";
  }

  @Override
  public String getUpScript() {
    return "CREATE TABLE IF NOT EXISTS task_resource("
        + "id IDENTITY,"
        + "task_config_id BIGINT NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,"
        + "type_id INT NOT NULL REFERENCES resource_types(id),"
        + "value VARCHAR NOT NULL,"
        + "UNIQUE(task_config_id, type_id, value)"
        + ");\n"
        + migrateScript();
  }

  @Override
  public String getDownScript() {
    return "DROP TABLE IF EXISTS task_resource;";
  }

  private static String migrateScript() {
    return "MERGE INTO task_resource(task_config_id, type_id, value) "
        + "KEY(task_config_id, type_id, value) "
        + "SELECT t.id, rt.id, t.num_cpus FROM task_configs t "
        + "JOIN resource_types rt ON rt.name = "
        + String.format("'%s';%n", ResourceType.CPUS.name())

        + "MERGE INTO task_resource(task_config_id, type_id, value) "
        + "KEY(task_config_id, type_id, value) "
        + "SELECT t.id, rt.id, t.ram_mb FROM task_configs t "
        + "JOIN resource_types rt ON rt.NAME = "
        + String.format("'%s';%n", ResourceType.RAM_MB.name())

        + "MERGE INTO task_resource(task_config_id, type_id, value) "
        + "KEY(task_config_id, type_id, value) "
        + "SELECT t.id, rt.id, t.disk_mb FROM task_configs t "
        + "JOIN resource_types rt ON rt.NAME = "
        + String.format("'%s';%n", ResourceType.DISK_MB.name())

        + "MERGE INTO task_resource(task_config_id, type_id, value) "
        + "KEY(task_config_id, type_id, value) "
        + "SELECT tcrp.task_config_id, rt.id, tcrp.port_name FROM task_config_requested_ports tcrp "
        + "JOIN resource_types rt ON rt.NAME = "
        + String.format("'%s';%n", ResourceType.PORTS.name());
  }
}
