package com.twitter.mesos.scheduler;

import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.Range;
import org.apache.mesos.Protos.Resource.Type;
import org.junit.Test;

import com.twitter.common.collections.Pair;

import static org.junit.Assert.assertEquals;

/**
 * @author William Farner
 */
public class ResourcesTest {

  private static final String NAME = "resource_name";

  @Test
  public void testRangeResourceEmpty() {
    expectRanges(ImmutableSet.<Pair<Long,Long>>of(), ImmutableSet.<Integer>of());
  }

  @Test
  public void testRangeResourceOneEntry() {
    expectRanges(ImmutableSet.of(Pair.of(5L, 5L)), ImmutableSet.of(5));
    expectRanges(ImmutableSet.of(Pair.of(0L, 0L)), ImmutableSet.of(0));
  }

  @Test
  public void testRangeResourceNonContiguous() {
    expectRanges(ImmutableSet.of(Pair.of(1L, 1L), Pair.of(3L, 3L), Pair.of(5L, 5L)),
        ImmutableSet.of(5, 1, 3));
  }

  @Test
  public void testRangeResourceContiguous() {
    expectRanges(ImmutableSet.of(Pair.of(1L, 2L), Pair.of(4L, 5L), Pair.of(7L, 9L)),
        ImmutableSet.of(8, 2, 4, 5, 7, 9, 1));
  }

  private void expectRanges(Set<Pair<Long, Long>> expected, Set<Integer> values) {
    Resource resource = Resources.makeMesosRangeResource(NAME, values);
    assertEquals(Type.RANGES, resource.getType());
    assertEquals(NAME, resource.getName());

    Set<Pair<Long, Long>> actual = ImmutableSet.copyOf(Iterables.transform(
        resource.getRanges().getRangeList(),
        new Function<Range, Pair<Long, Long>>() {
          @Override public Pair<Long, Long> apply(Range range) {
            return Pair.of(range.getBegin(), range.getEnd());
          }
        }));
    assertEquals(expected, actual);
  }
}
