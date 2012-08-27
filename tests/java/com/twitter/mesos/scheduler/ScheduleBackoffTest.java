package com.twitter.mesos.scheduler;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.common.util.testing.FakeTicker;
import com.twitter.mesos.scheduler.events.TaskPubsubEvent.Deleted;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ScheduleBackoffTest extends EasyMockTest {

  private static final String TASK_A = "task_a";

  private FakeTicker ticker;
  private BackoffStrategy strategy;
  private ScheduleBackoff backoff;

  @Before
  public void setUp() throws Exception {
    ticker = new FakeTicker();
    strategy = createMock(BackoffStrategy.class);
    backoff = new ScheduleBackoff(ticker, strategy);
  }

  @Test
  public void testNoPenalty() {
    control.replay();
    assertSchedulable();
  }

  @Test
  public void testPenaltyWait() {
    expectBackoffCalculation(0, 2);

    control.replay();

    rescheduled();
    assertNotSchedulable();
    tick(1);
    assertNotSchedulable();
    tick(1);
    assertSchedulable();
  }

  @Test
  public void testPenaltyCapped() {
    expectBackoffCalculation(0, 2);
    expectBackoffCalculation(2, 10);
    expectBackoffCalculation(10, ScheduleBackoff.MAX_PENALTY.as(Time.NANOSECONDS) * 2);

    control.replay();

    rescheduled();
    assertNotSchedulable();
    tick(1);
    assertNotSchedulable();
    rescheduled();
    assertNotSchedulable();
    tick(1);
    assertNotSchedulable();
    rescheduled();
    assertNotSchedulable();
    tick(ScheduleBackoff.MAX_PENALTY.as(Time.NANOSECONDS) - 1);
    assertNotSchedulable();
    tick(1);
    assertSchedulable();
  }

  @Test
  public void testPenaltyExpires() {
    expectBackoffCalculation(0, 2);
    expectBackoffCalculation(0, 2);

    control.replay();

    rescheduled();
    tick(ScheduleBackoff.MAX_PENALTY.as(Time.NANOSECONDS));
    assertSchedulable();
    rescheduled();
    assertNotSchedulable();
  }

  @Test
  public void testTaskDeleted() {
    expectBackoffCalculation(0, 2);

    control.replay();

    rescheduled();
    deleted();
    assertSchedulable();
  }

  private void expectBackoffCalculation(long oldBackoff, long newBackoff) {
    expect(strategy.calculateBackoffMs(oldBackoff)).andReturn(newBackoff);
  }

  private void tick(long nanos) {
    ticker.advance(Amount.of(nanos, Time.NANOSECONDS));
  }

  private void rescheduled() {
    backoff.onRescheduled(TASK_A);
  }

  private void deleted() {
    backoff.onDeleted(new Deleted(ImmutableSet.of(TASK_A)));
  }

  private void assertSchedulable() {
    assertTrue(backoff.isSchedulable(TASK_A));
  }

  private void assertNotSchedulable() {
    assertFalse(backoff.isSchedulable(TASK_A));
  }
}
