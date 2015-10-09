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
package org.apache.aurora.scheduler.base;

import java.util.Set;

import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.scheduler.storage.entities.IRange;

/**
 * Utility class for working with numbers.
 */
public final class Numbers {

  private Numbers() {
    // Utility class.
  }

  /**
   * Converts a set of integers into a set of contiguous closed ranges that equally represent the
   * input integers.
   * <p>
   * The resulting ranges will be in ascending order.
   * <p>
   * TODO(wfarner): Change this to return a canonicalized RangeSet.
   *
   * @param values Values to transform to ranges.
   * @return Closed ranges with identical members to the input set.
   */
  public static Set<Range<Integer>> toRanges(Iterable<Integer> values) {
    ImmutableSet.Builder<Range<Integer>> builder = ImmutableSet.builder();

    PeekingIterator<Integer> iterator =
        Iterators.peekingIterator(Sets.newTreeSet(values).iterator());

    // Build ranges until there are no numbers left.
    while (iterator.hasNext()) {
      // Start a new range.
      int start = iterator.next();
      int end = start;
      // Increment the end until the range is non-contiguous.
      while (iterator.hasNext() && iterator.peek() == end + 1) {
        end++;
        iterator.next();
      }

      builder.add(Range.closed(start, end));
    }

    return builder.build();
  }

  /**
   * Convert between range types.
   *
   * @param range Range to convert.
   * @return A closed range from the first to last of {@code range}.
   */
  public static Range<Integer> toRange(IRange range) {
    return Range.closed(range.getFirst(), range.getLast());
  }

  /**
   * Performs {@link #toRange(IRange)} for a collection of ranges, and convert the result to a set
   * of integers.
   *
   * @param ranges Ranges to convert.
   * @return A set representing {@code ranges}.
   */
  public static Set<Integer> rangesToInstanceIds(Iterable<IRange> ranges) {
    ImmutableRangeSet.Builder<Integer> instanceIds = ImmutableRangeSet.builder();
    for (IRange range : ranges) {
      instanceIds.add(toRange(range));
    }

    return instanceIds.build().asSet(DiscreteDomain.integers());
  }

  /**
   * Converts set of instance ranges to a set of {@link IRange}.
   *
   * @param ranges Instance ranges to convert.
   * @return A set of {@link IRange}.
   */
  public static Set<IRange> convertRanges(Set<Range<Integer>> ranges) {
    return ranges.stream()
        .map(range -> IRange.build(new org.apache.aurora.gen.Range(
            range.lowerEndpoint(),
            range.upperEndpoint())))
        .collect(GuavaUtils.toImmutableSet());
  }
}
