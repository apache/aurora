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

/**
 * A work queue that only executes pending work when flushed.
 */
public interface GatedWorkQueue {

  /**
   * Closes the gate on the work queue for the duration of an operation.
   *
   * @param operation Operation to execute while keeping the gate closed.
   * @param <T> Operation return type.
   * @param <E> Operation exception type.
   * @return The value returned by the {@code operation}.
   * @throws E Exception thrown by the {@code operation}.
   */
  <T, E extends Exception> T closeDuring(GatedOperation<T, E> operation) throws E;

  /**
   * Operation prevents new items from being executed on the work queue.
   *
   * @param <T> Operation return type.
   * @param <E> Operation exception type.
   */
  interface GatedOperation<T, E extends Exception> {
    T doWithGateClosed() throws E;
  }
}
