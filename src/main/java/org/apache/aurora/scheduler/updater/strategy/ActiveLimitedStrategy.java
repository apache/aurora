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

import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Ordering;

/**
 * A strategy that limits the number of instances selected by the subclass.
 *
 * @param <T> Instance type.
 */
abstract class ActiveLimitedStrategy<T extends Comparable<T>> implements UpdateStrategy<T> {
  private final Ordering<T> ordering;
  protected final int maxActive;

  /**
   * Creates an active-limited strategy that applies an upper bound to all results.
   *
   * @param maxActive Maximum number of values to return from
   * {@link #getNextGroup(java.util.Set, java.util.Set)}.
   */
  protected ActiveLimitedStrategy(Ordering<T> ordering, int maxActive) {
    this.ordering = Objects.requireNonNull(ordering);
    Preconditions.checkArgument(maxActive > 0);
    this.maxActive = maxActive;
  }

  @Override
  public final Set<T> getNextGroup(Set<T> idle, Set<T> active) {
    return FluentIterable
        .from(ordering.sortedCopy(doGetNextGroup(idle, active)))
        .limit(Math.max(0, maxActive - active.size()))
        .toSet();
  }

  /**
   * Delegate function for the subclass. The implementation may return as many or few results
   * as they wish. If the result is larger than {@link #maxActive}, it will be truncated.
   *
   * @param idle Idle instances, candidate for being updated.
   * @param active Instances currently being updated.
   * @return A subset of {@code idle}, instances to start updating.
   */
  abstract Set<T> doGetNextGroup(Set<T> idle, Set<T> active);
}
