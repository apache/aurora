package com.twitter.mesos.scheduler;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.common.util.testing.FakeTicker;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.ScheduleBackoff.ScheduleBackoffImpl;
import com.twitter.mesos.scheduler.events.TaskPubsubEvent.Rescheduled;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ScheduleBackoffImplTest extends EasyMockTest {

  private static final Amount<Long, Time> MAX_PENALTY = Amount.of(1L, Time.MINUTES);
  private static final long MAX_PENALTY_NANOS = MAX_PENALTY.as(Time.NANOSECONDS);
  private static final String ROLE_A = "role_a";
  private static final String JOB_A = "job_a";
  private static final int SHARD_0 = 0;
  private static final TwitterTaskInfo TASK_A = new TwitterTaskInfo()
      .setOwner(new Identity(ROLE_A, ROLE_A))
      .setJobName(JOB_A)
      .setShardId(SHARD_0);

  private FakeTicker ticker;
  private BackoffStrategy strategy;
  private ScheduleBackoffImpl backoff;

  @Before
  public void setUp() throws Exception {
    ticker = new FakeTicker();
    strategy = createMock(BackoffStrategy.class);
    backoff = new ScheduleBackoffImpl(ticker, strategy, MAX_PENALTY);
  }

  @Test
  public void testNoPenalty() {
    control.replay();
    assertSchedulable();
  }

  private void expectSchedulableEdge(long tickNanos) {
    assertNotSchedulable();
    tick(tickNanos);
    assertNotSchedulable();
    tick(1);
    assertSchedulable();
  }

  @Test
  public void testPenaltyWait() {
    expectBackoffCalculation(0, 2);

    control.replay();

    rescheduled();
    expectSchedulableEdge(2);
  }

  @Test
  public void testPenaltyCapped() {
    expectBackoffCalculation(0, 2);
    expectBackoffCalculation(2, 10);
    expectBackoffCalculation(10, MAX_PENALTY_NANOS * 2);
    expectBackoffCalculation(MAX_PENALTY_NANOS, MAX_PENALTY_NANOS);

    control.replay();

    // penalty
    rescheduled();
    expectSchedulableEdge(2);

    // watch period
    tick(1);
    rescheduled();

    // penalty increase
    expectSchedulableEdge(10);

    // watch period
    tick(9);
    rescheduled();

    // capped penalty increase
    expectSchedulableEdge(MAX_PENALTY_NANOS);

    // watch period
    assertSchedulable();
    tick(MAX_PENALTY_NANOS - 1);
    rescheduled();
  }

  @Test
  public void testPenaltyExpires() {
    expectBackoffCalculation(0, 2);
    expectBackoffCalculation(0, 2);

    control.replay();

    rescheduled();
    tick(MAX_PENALTY_NANOS);
    assertSchedulable();
    rescheduled();
    assertNotSchedulable();
  }

  private void expectBackoffCalculation(long oldBackoff, long newBackoff) {
    expect(strategy.calculateBackoffMs(oldBackoff)).andReturn(newBackoff);
  }

  private void tick(long nanos) {
    ticker.advance(Amount.of(nanos, Time.NANOSECONDS));
  }

  private void rescheduled() {
    backoff.onRescheduled(new Rescheduled(ROLE_A, JOB_A, SHARD_0));
  }

  private void assertSchedulable() {
    assertTrue(backoff.isSchedulable(TASK_A));
  }

  private void assertNotSchedulable() {
    assertFalse(backoff.isSchedulable(TASK_A));
  }
}
