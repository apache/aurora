package com.twitter.aurora.scheduler.base;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NumbersTest {

  @Test
  public void testToRanges() {
    assertEquals(ImmutableSet.<Range<Integer>>of(), Numbers.toRanges(ImmutableList.<Integer>of()));
    assertEquals(ImmutableSet.of(Range.closed(1, 1)), Numbers.toRanges(ImmutableList.of(1)));
    assertEquals(ImmutableSet.of(Range.closed(0, 3)),
        Numbers.toRanges(ImmutableList.of(0, 1, 2, 3)));
    assertEquals(ImmutableSet.of(Range.closed(0, 3), Range.closed(5, 5)),
        Numbers.toRanges(ImmutableList.of(0, 1, 2, 3, 5)));
    assertEquals(
        ImmutableSet.of(
            Range.closed(0, 1),
            Range.closed(5, 6),
            Range.closed(9, 9),
            Range.closed(100, 100)),
        Numbers.toRanges(ImmutableList.of(0, 1, 5, 6, 9, 100)));
  }
}
