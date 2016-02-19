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
package org.apache.aurora.scheduler;

import java.util.Objects;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.aurora.common.util.Clock;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * A function that generates universally-unique (not guaranteed, but highly confident) task IDs.
 */
public interface TaskIdGenerator {

  /**
   * Generates a universally-unique ID for the task.  This is not necessarily a repeatable
   * operation, two subsequent invocations with the same object need not return the same value.
   *
   * @param task Configuration of the task to create an ID for.
   * @param instanceId Instance ID for the task.
   * @return A universally-unique ID for the task.
   */
  String generate(ITaskConfig task, int instanceId);

  class TaskIdGeneratorImpl implements TaskIdGenerator {
    private final Clock clock;

    @Inject
    TaskIdGeneratorImpl(Clock clock) {
      this.clock = Objects.requireNonNull(clock);
    }

    @Override
    public String generate(ITaskConfig task, int instanceId) {
      String sep = "-";
      return new StringBuilder()
          .append(task.getJob().getRole())
          .append(sep)
          .append(task.getJob().getEnvironment())
          .append(sep)
          .append(task.getJob().getName())
          .append(sep)
          .append(instanceId)
          .append(sep)
          .append(UUID.randomUUID())
          .toString().replaceAll("[^\\w-]", sep);  // Constrain character set.
    }
  }
}
