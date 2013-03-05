package com.twitter.mesos.scheduler.metadata;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.testing.FakeTicker;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.scheduler.SchedulingFilter.Veto;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.events.PubsubEvent.TasksDeleted;
import com.twitter.mesos.scheduler.events.PubsubEvent.Vetoed;

import static org.junit.Assert.assertEquals;

public class NearestFitTest {

  private static final Veto ALMOST = new Veto("Almost", 1);
  private static final Veto NOPE = new Veto("Nope", 5);
  private static final Veto NO_CHANCE = new Veto("No chance", 1000);
  private static final Veto KERNEL = Veto.constraintMismatch("2.6.39");

  private static final String TASK = "taskId";

  private FakeTicker ticker;
  private NearestFit nearest;

  @Before
  public void setUp() {
    ticker = new FakeTicker();
    nearest = new NearestFit(ticker);
  }

  @Test
  public void testNoReason() {
    assertNearest();
  }

  @Test
  public void testMultipleVetoes() {
    vetoed(ALMOST, NOPE);
    assertNearest(ALMOST, NOPE);
    // Even though the aggregate score for NO_CHANCE is higher than ALMOST and NOPE,
    // NO_CHANCE becomes the pending reason since we consider one vector smaller than two
    // (regardless of magnitude).
    vetoed(NO_CHANCE);
    assertNearest(NO_CHANCE);
  }

  @Test
  public void testScoring() {
    vetoed(NO_CHANCE);
    assertNearest(NO_CHANCE);
    vetoed(ALMOST);
    assertNearest(ALMOST);
    vetoed(NO_CHANCE);
    assertNearest(ALMOST);
  }

  @Test
  public void testRemove() {
    vetoed(NO_CHANCE);
    nearest.remove(new TasksDeleted(ImmutableSet.of(makeTask())));
    assertNearest();
  }

  private ScheduledTask makeTask() {
    return new ScheduledTask().setAssignedTask(new AssignedTask().setTaskId(TASK));
  }

  @Test
  public void testExpiration() {
    vetoed(ALMOST);
    assertNearest(ALMOST);
    ticker.advance(NearestFit.EXPIRATION);
    ticker.advance(Amount.of(1L, Time.SECONDS));
    assertNearest();
  }

  @Test
  public void testStateChanged() {
    vetoed(ALMOST);
    assertNearest(ALMOST);
    ScheduledTask task = new ScheduledTask()
        .setStatus(ScheduleStatus.ASSIGNED)
        .setAssignedTask(new AssignedTask().setTaskId(TASK));
    nearest.stateChanged(new TaskStateChange(task, ScheduleStatus.PENDING));
    assertNearest();
  }

  @Test
  public void testConstraintMismatch() {
    vetoed(KERNEL);
    assertNearest(KERNEL);
    vetoed(ALMOST);
    assertNearest(ALMOST);
    vetoed(KERNEL);
    assertNearest(ALMOST);
  }

  private Set<Veto> vetoes(Veto... vetoes) {
    return ImmutableSet.<Veto>builder().add(vetoes).build();
  }

  private void vetoed(Veto... vetoes) {
    nearest.vetoed(new Vetoed(TASK, ImmutableSet.<Veto>builder().add(vetoes).build()));
  }

  private void assertNearest(Veto... vetoes) {
    assertEquals(vetoes(vetoes), nearest.getNearestFit(TASK));
  }
}
