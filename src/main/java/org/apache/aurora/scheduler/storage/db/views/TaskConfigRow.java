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
package org.apache.aurora.scheduler.storage.db.views;

import java.util.Objects;

import org.apache.aurora.gen.TaskConfig;

/**
 * Representation of a row in the task_configs table.
 */
public class TaskConfigRow {
  private final long id;
  private final TaskConfig config;

  private TaskConfigRow() {
    // Required for mybatis.
    this(-1, null);
  }

  public TaskConfigRow(long id, TaskConfig config) {
    this.id = id;
    this.config = config;
  }

  public long getId() {
    return id;
  }

  public TaskConfig getConfig() {
    return config;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TaskConfigRow)) {
      return false;
    }

    TaskConfigRow other = (TaskConfigRow) obj;
    return Objects.equals(id, other.id)
        && Objects.equals(config, other.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, config);
  }
}
