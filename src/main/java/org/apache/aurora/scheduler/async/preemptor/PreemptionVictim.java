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
package org.apache.aurora.scheduler.async.preemptor;

import java.util.Objects;

import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * A victim to be considered as a candidate for preemption.
 */
public final class PreemptionVictim {
  private final String slaveHost;
  private final boolean production;
  private final String role;
  private final int priority;
  private final ResourceSlot resources;
  private final String taskId;

  private PreemptionVictim(
      String slaveHost,
      boolean production,
      String role,
      int priority,
      ResourceSlot resources,
      String taskId) {

    this.slaveHost = slaveHost;
    this.production = production;
    this.role = role;
    this.priority = priority;
    this.resources = resources;
    this.taskId = taskId;
  }

  public static PreemptionVictim fromTask(IAssignedTask task) {
    ITaskConfig config = task.getTask();
    return new PreemptionVictim(
        task.getSlaveHost(),
        config.isProduction(),
        config.getOwner().getRole(),
        config.getPriority(),
        ResourceSlot.from(config),
        task.getTaskId());
  }

  public String getSlaveHost() {
    return slaveHost;
  }

  public boolean isProduction() {
    return production;
  }

  public String getRole() {
    return role;
  }

  public int getPriority() {
    return priority;
  }

  public ResourceSlot getResources() {
    return resources;
  }

  public String getTaskId() {
    return taskId;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PreemptionVictim)) {
      return false;
    }

    PreemptionVictim other = (PreemptionVictim) o;
    return Objects.equals(getSlaveHost(), other.getSlaveHost())
        && Objects.equals(isProduction(), other.isProduction())
        && Objects.equals(getRole(), other.getRole())
        && Objects.equals(getPriority(), other.getPriority())
        && Objects.equals(getResources(), other.getResources())
        && Objects.equals(getTaskId(), other.getTaskId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(slaveHost, production, role, priority, resources, taskId);
  }

  @Override
  public String toString() {
    return com.google.common.base.Objects.toStringHelper(this)
        .add("slaveHost", getSlaveHost())
        .add("production", isProduction())
        .add("role", getRole())
        .add("priority", getPriority())
        .add("resources", getResources())
        .add("taskId", getTaskId())
        .toString();
  }
}
