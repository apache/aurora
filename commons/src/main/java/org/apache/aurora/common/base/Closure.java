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
package org.apache.aurora.common.base;

/**
 * A closure that does not throw any checked exceptions.
 *
 * @param <T> Closure value type.
 *
 * @author John Sirois
 */
@FunctionalInterface
public interface Closure<T> {
  // convenience typedef

  /**
   * Performs a unit of work on item
   *
   * @param item the item to perform work against
   */
  void execute(T item);
}
