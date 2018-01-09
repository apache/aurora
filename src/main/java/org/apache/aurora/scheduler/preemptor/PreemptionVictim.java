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
package org.apache.aurora.scheduler.preemptor;

import java.util.Objects;

import com.google.common.base.MoreObjects;

import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.util.Objects.requireNonNull;

/**
 * A victim to be considered as a candidate for preemption.
 */
public final class PreemptionVictim {
  private final IAssignedTask task;

  private PreemptionVictim(IAssignedTask task) {
    this.task = requireNonNull(task);
  }

  public static PreemptionVictim fromTask(IAssignedTask task) {
    return new PreemptionVictim(task);
  }

  public String getSlaveHost() {
    return task.getSlaveHost();
  }

  public boolean isProduction() {
    return task.getTask().isProduction();
  }

  public String getRole() {
    return task.getTask().getJob().getRole();
  }

  public int getPriority() {
    return task.getTask().getPriority();
  }

  public ResourceBag getResourceBag(ExecutorSettings executorSettings) {
    return ResourceManager.bagFromTask(task.getTask(), executorSettings);
  }

  public String getTaskId() {
    return task.getTaskId();
  }

  public ITaskConfig getConfig() {
    return task.getTask();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PreemptionVictim)) {
      return false;
    }

    PreemptionVictim other = (PreemptionVictim) o;
    return Objects.equals(task, other.task);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(task);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("task", task)
        .toString();
  }
}
