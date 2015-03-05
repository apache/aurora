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
package org.apache.aurora.scheduler.async;

import java.util.Queue;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.aurora.scheduler.base.TaskGroupKey;

/**
 * A group of task IDs that are eligible for scheduling, but may be waiting for a backoff to expire.
 */
class TaskGroup {
  private final TaskGroupKey key;
  private long penaltyMs;
  private final Queue<String> tasks;

  TaskGroup(TaskGroupKey key, String initialTaskId) {
    this.key = key;
    this.penaltyMs = 0;
    this.tasks = Lists.newLinkedList();
    this.tasks.add(initialTaskId);
  }

  synchronized TaskGroupKey getKey() {
    return key;
  }

  synchronized Optional<String> peek() {
    return Optional.fromNullable(tasks.peek());
  }

  synchronized boolean hasMore() {
    return !tasks.isEmpty();
  }

  synchronized void remove(String taskId) {
    tasks.remove(taskId);
  }

  synchronized void offer(String taskId) {
    tasks.offer(taskId);
  }

  synchronized void setPenaltyMs(long penaltyMs) {
    this.penaltyMs = penaltyMs;
  }

  // Begin methods used for debug interfaces.

  public synchronized String getName() {
    return key.toString();
  }

  public synchronized Set<String> getTaskIds() {
    return ImmutableSet.copyOf(tasks);
  }

  public synchronized long getPenaltyMs() {
    return penaltyMs;
  }
}
