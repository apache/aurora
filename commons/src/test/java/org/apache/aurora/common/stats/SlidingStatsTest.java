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
package org.apache.aurora.common.stats;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.SlidingStats.Timeable;
import org.apache.aurora.common.util.testing.FakeClock;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class SlidingStatsTest {

  private static final long OPERATION_TIME = 5L;
  private static final Amount<Long, Time> OPERATION_TIME_AMOUNT =
      Amount.of(OPERATION_TIME, Time.NANOSECONDS);

  private FakeClock clock;
  private TimedActions actions;
  private SlidingStats stat;

  @Before
  public void setUp() {
    clock = new FakeClock();
    actions = new TimedActions(clock);
    stat = new SlidingStats("a", "nanos", 1, clock);
  }

  @Test
  public void testAccumulate() {
    assertEquals(0L, stat.getEventCounter().get());
    assertEquals(0L, stat.getTotalCounter().get());

    stat.accumulate(100L);

    assertEquals(1L, stat.getEventCounter().get());
    assertEquals(100L, stat.getTotalCounter().get());
  }

  @Test
  public void testTimeable() {
    String value = stat.time(() -> actions.quietAction("World"));
    assertEquals("HelloWorld", value);
    assertEquals(1L, stat.getEventCounter().get());
    assertEquals(OPERATION_TIME, stat.getTotalCounter().get());
  }

  @Test
  public void testNoResultQuietTimeable() {
    stat.time((Timeable.NoResult.Quiet) actions::noResultQuietAction);
    assertEquals(1L, stat.getEventCounter().get());
    assertEquals(OPERATION_TIME, stat.getTotalCounter().get());
  }

  @Test
  public void testTimeableThrowsException() {
    String value = null;
    try {
      value = stat.time(actions::action);
      fail("Should have thrown exception.");
    } catch (Exception e) {
      assertEquals("Expected!", e.getMessage());
    }
    assertNull(value);
    assertEquals(1L, stat.getEventCounter().get());
    assertEquals(OPERATION_TIME, stat.getTotalCounter().get());
  }

  @Test
  public void testNoResultTimeableThrowsException() {
    try {
      stat.time((Timeable.NoResult<Exception>) actions::noResultAction);
      fail("Should have thrown exception.");
    } catch (Exception e) {
      assertEquals("Expected!", e.getMessage());
    }
    assertEquals(1L, stat.getEventCounter().get());
    assertEquals(OPERATION_TIME, stat.getTotalCounter().get());
  }

  @Test
  public void testQuietTimeableThrowsRuntimeException() {
    String value = null;
    try {
      value = stat.time(actions::quietExceptionalAction);
      fail("Should have thrown exception.");
    } catch (RuntimeException e) {
      assertEquals("Expected!", e.getMessage());
    }
    assertNull(value);
    assertEquals(1L, stat.getEventCounter().get());
    assertEquals(OPERATION_TIME, stat.getTotalCounter().get());
  }

  private static class TimedActions {
    private FakeClock clock;

    public TimedActions(FakeClock clock) {
      this.clock = clock;
    }

    String action() throws Exception{
      clock.advance(OPERATION_TIME_AMOUNT);
      throw new Exception("Expected!");
    }

    String quietAction(String input) {
      clock.advance(OPERATION_TIME_AMOUNT);
      return "Hello" + input;
    }

    String quietExceptionalAction() {
      clock.advance(OPERATION_TIME_AMOUNT);
      throw new RuntimeException("Expected!");
    }

    void noResultAction() throws Exception {
      clock.advance(OPERATION_TIME_AMOUNT);
      throw new Exception("Expected!");
    }

    void noResultQuietAction() {
      clock.advance(OPERATION_TIME_AMOUNT);
      System.gc();
    }
  }
}
