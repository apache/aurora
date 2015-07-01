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
package org.apache.aurora.scheduler.updater.strategy;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;

/**
 * An update strategy that will only add more work when the current active group is empty.
 *
 * @param <T> Instance type.
 */
public class BatchStrategy<T extends Comparable<T>> extends ActiveLimitedStrategy<T> {

  /**
   * Creates a batch strategy that will allow a limited number of active entries.
   *
   * @param maxActive The maximum number of active entries.
   */
  public BatchStrategy(Ordering<T> ordering, int maxActive) {
    super(ordering, maxActive);
  }

  @Override
  Set<T> doGetNextGroup(Set<T> idle, Set<T> active) {
    return active.isEmpty() ? idle : ImmutableSet.of();
  }
}
