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

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import org.apache.aurora.gen.AssignedTask;

public final class DbAssignedTask {
  private String taskId;
  private String slaveId;
  private String slaveHost;
  private DbTaskConfig task;
  private List<DbAssginedPort> assignedPorts;
  private int instanceId;

  private DbAssignedTask() {
  }

  AssignedTask toThrift() {
    Map<String, Integer> ports = Maps.newHashMap();
    for (DbAssginedPort port: assignedPorts) {
      ports.put(port.getName(), port.getPort());
    }

    return new AssignedTask()
        .setTaskId(taskId)
        .setSlaveId(slaveId)
        .setSlaveHost(slaveHost)
        .setTask(task.toThrift())
        .setAssignedPorts(ports)
        .setInstanceId(instanceId);
  }
}
