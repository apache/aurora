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

/**
 * Representation of a row in the task_ports table.
 */
public class AssignedPort {
  private final String name;
  private final int port;

  private AssignedPort() {
    // Needed by mybatis.
    this(null, -1);
  }

  public AssignedPort(String name, int port) {
    this.name = name;
    this.port = port;
  }

  public String getName() {
    return name;
  }

  public int getPort() {
    return port;
  }
}
