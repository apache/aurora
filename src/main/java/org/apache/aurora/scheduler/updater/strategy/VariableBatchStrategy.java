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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An update strategy that will only add more work when the current active group is empty.
 * Size of the groups are picked from the supplied list.
 * Last element is picked multiple times if necessary.
 *
 * @param <T> Instance type.
 */
public class VariableBatchStrategy<T extends Comparable<T>> implements UpdateStrategy<T> {
  private final Ordering<T> ordering;
  protected final ImmutableList<Integer> groupSizes;
  private final boolean rollingForward;
  private Optional<Integer> totalModInstanceCount;

  private static final Logger LOG = LoggerFactory.getLogger(VariableBatchStrategy.class);

  /**
   * Creates a variable active-limited strategy that applies an upper bound to all results.
   *
   * @param maxActiveGroups  List of Maximum group sizes. Each group size represents a step.
   * {@link #getNextGroup(Set, Set)}.
   */
  public VariableBatchStrategy(
      Ordering<T> ordering,
      List<Integer> maxActiveGroups,
      boolean rollingForward) {

    this.ordering = Objects.requireNonNull(ordering);
    this.rollingForward = rollingForward;

    maxActiveGroups.forEach(x -> Preconditions.checkArgument(x > 0));

    this.groupSizes = ImmutableList.copyOf(maxActiveGroups);
    this.totalModInstanceCount = Optional.empty();
  }

  // Determine how far we're into the update based upon how many instances are waiting
  // to be modified.
  private int determineCurGroupSize(int remaining) {
    // Calculate which groupIndex we are in by finding out how many instances we have left to update
    int modified = totalModInstanceCount.get() - remaining;
    int finalGroupSize = Iterables.getLast(groupSizes);

    LOG.debug("Variable Batch Update progress: {} instances have been modified, "
            + "{} instances remain unmodified, and {} overall instances to be modified.",
        modified,
        remaining,
        totalModInstanceCount.get());

    if (rollingForward) {
      int sum = 0;
      for (Integer groupSize : groupSizes) {
        sum += groupSize;

        if (sum > modified) {
          return groupSize;
        }
      }
      // Return last step when number of instances > sum of all groups
      return finalGroupSize;
    } else {
      // To perform the update in reverse, we use the number of remaining tasks left to update
      // instead of using the number of already modified instances. In a rollback, the remaining
      // count represents the number of instances that were already modified while rolling forward
      // and need to be reverted.
      int curGroupSize = remaining;

      for (Integer groupSize : groupSizes) {
        // This handles an in between step. i.e.: updated instances = 4, update groups = [2,3]
        // which results in update groups 2 and 2 rolling forward at the time of failure.
        if (curGroupSize <= groupSize) {
          return curGroupSize;
        }

        curGroupSize -= groupSize;
      }

      // Handle the case where number of instances update were
      // greater than the sum of all update groups
      // Calculate the size of the last update group size performed while rolling forward.
      curGroupSize = curGroupSize % finalGroupSize;
      if (curGroupSize == 0) {
        return finalGroupSize;
      } else {
        return curGroupSize;
      }
    }
  }

  @Override
  public final Set<T> getNextGroup(Set<T> idle, Set<T> active) {
    // Get the size for the idle set on the first run only. This is representative of the number
    // of overall instance modifications this update will trigger.
    if (!totalModInstanceCount.isPresent()) {
      totalModInstanceCount = Optional.of(idle.size());
    }

    // Limit group size to the current size of the group minus the number of instances currently
    // being modified.
    return ordering.sortedCopy(doGetNextGroup(idle, active)).stream()
            .limit(Math.max(0, determineCurGroupSize(idle.size()) - active.size()))
            .collect(Collectors.toSet());
  }

  /**
   * Return a list of instances to be updated.
   * Returns an empty list if the current active group has not completed.
   *
   * @param idle Idle instances, candidate for being updated.
   * @param active Instances currently being updated.
   * @return all idle instances to start updating.
   */
  Set<T> doGetNextGroup(Set<T> idle, Set<T> active) {
    return active.isEmpty() ? idle : ImmutableSet.of();
  }
}
