/**
 * Copyright 2013 Apache Software Foundation
 *
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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

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
}
