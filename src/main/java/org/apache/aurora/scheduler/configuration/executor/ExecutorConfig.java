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
package org.apache.aurora.scheduler.configuration.executor;

import java.util.List;
import java.util.Objects;

import com.google.common.base.MoreObjects;

import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Volume;

import static java.util.Objects.requireNonNull;

/**
 * Executor-related configuration used to populate task descriptions.
 */
public class ExecutorConfig {

  private final ExecutorInfo executor;
  private final List<Volume> volumeMounts;
  private final String taskPrefix;

  public ExecutorConfig(
      ExecutorInfo executor,
      List<Volume> volumeMounts,
      String taskPrefix) {
    this.executor = requireNonNull(executor);
    this.volumeMounts = requireNonNull(volumeMounts);
    this.taskPrefix = requireNonNull(taskPrefix);
  }

  public ExecutorInfo getExecutor() {
    return executor;
  }

  public List<Volume> getVolumeMounts() {
    return volumeMounts;
  }

  public String getTaskPrefix() {
    return taskPrefix;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ExecutorConfig)) {
      return false;
    }

    ExecutorConfig other = (ExecutorConfig) obj;
    return Objects.equals(executor, other.executor)
        && Objects.equals(volumeMounts, other.volumeMounts)
        && Objects.equals(taskPrefix, other.taskPrefix);
  }

  @Override
  public int hashCode() {
    return Objects.hash(executor, volumeMounts, taskPrefix);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("executor", executor)
        .add("volumeMounts", volumeMounts)
        .add("taskPrefix", taskPrefix)
        .toString();
  }
}
