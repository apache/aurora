package com.twitter.mesos.scheduler;

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
      while (iterator.hasNext() && (iterator.peek() == (end + 1))) {
        end++;
        iterator.next();
      }

      builder.add(Range.closed(start, end));
    }

    return builder.build();
  }
}
