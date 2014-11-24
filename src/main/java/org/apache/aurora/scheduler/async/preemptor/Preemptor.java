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

import com.google.common.base.Optional;

import org.apache.aurora.scheduler.filter.AttributeAggregate;

/**
 * Preempts active tasks in favor of higher priority tasks.
 */
public interface Preemptor {

  /**
   * Preempts active tasks in favor of the input task.
   *
   * @param taskId ID of the preempting task.
   * @param attributeAggregate Attribute information for tasks in the job containing {@code task}.
   * @return ID of the slave where preemption occured.
   */
  Optional<String> findPreemptionSlotFor(String taskId, AttributeAggregate attributeAggregate);
}
