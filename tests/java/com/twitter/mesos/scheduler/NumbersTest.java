package com.twitter.mesos.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NumbersTest {

  @Test
  public void testToRanges() {
    assertEquals(ImmutableSet.<Range<Integer>>of(), Numbers.toRanges(ImmutableList.<Integer>of()));
    assertEquals(ImmutableSet.of(Ranges.closed(1, 1)), Numbers.toRanges(ImmutableList.of(1)));
    assertEquals(ImmutableSet.of(Ranges.closed(0, 3)),
        Numbers.toRanges(ImmutableList.of(0, 1, 2, 3)));
    assertEquals(ImmutableSet.of(Ranges.closed(0, 3), Ranges.closed(5, 5)),
        Numbers.toRanges(ImmutableList.of(0, 1, 2, 3, 5)));
    assertEquals(
        ImmutableSet.of(
            Ranges.closed(0, 1),
            Ranges.closed(5, 6),
            Ranges.closed(9, 9),
            Ranges.closed(100, 100)),
        Numbers.toRanges(ImmutableList.of(0, 1, 5, 6, 9, 100)));
  }
}
