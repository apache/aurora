package com.twitter.mesos.scheduler.log;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;

import org.junit.Test;

import com.twitter.mesos.scheduler.log.Log.Position;
import com.twitter.mesos.scheduler.log.Log.Position.Base;

import static junit.framework.Assert.assertEquals;

/**
 * @author John Sirois
 */
public class LogTest {

  @Test
  public void testBeforeAndAfter() {
    ImmutableList<Position> sorted =
        ImmutableList.of(Position.BEGINNING, Position.BEGINNING, Position.END);

    ImmutableList<Position> unsorted =
        ImmutableList.of(Position.BEGINNING, Position.END, Position.BEGINNING);

    assertEquals("BEGINNING and END do not implement compareTo properly",
        sorted,
        Ordering.natural().immutableSortedCopy(unsorted));

    assertEquals("BEGINNING and END do implement equals properly",
        ImmutableSet.copyOf(sorted),
        ImmutableSet.copyOf(Ordering.natural().sortedCopy(unsorted)));
  }

  static class PerversePosition extends Base<PerversePosition> {
    private final int position;

    PerversePosition(int position) {
      super(PerversePosition.class);
      this.position = position;
    }

    @Override
    public int compareWith(PerversePosition other) {
      return -1;
    }

    @Override
    public String toString() {
      return String.valueOf(position);
    }

    @Override
    protected boolean equalTo(PerversePosition other) {
      return true;
    }

    @Override
    public int hashCode() {
      return position;
    }
  }

  static Position position(int position) {
    return new PerversePosition(position);
  }

  @Test
  public void testBase() {
    ImmutableList<Position> sorted =
        ImmutableList.of(Position.BEGINNING, position(1), position(2), Position.END);

    ImmutableList<Position> unsorted =
        ImmutableList.of(position(2), Position.END, Position.BEGINNING, position(1));

    assertEquals("Base should ensure subclasses never sort before BEGINNING or after END",
        sorted, Ordering.natural().immutableSortedCopy(unsorted));

    assertEquals("Base should ensure subclasses instances should never be equal",
        ImmutableSet.copyOf(sorted),
        ImmutableSet.copyOf(Ordering.natural().immutableSortedCopy(unsorted)));
  }
}
