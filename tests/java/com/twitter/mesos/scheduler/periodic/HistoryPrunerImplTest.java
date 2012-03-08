package com.twitter.mesos.scheduler.periodic;

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.periodic.HistoryPruner.HistoryPrunerImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;

/**
 * @author William Farner
 */
public class HistoryPrunerImplTest {

  private static final Amount<Long, Time> ONE_DAY = Amount.of(1L, Time.DAYS);
  private static final Amount<Long, Time> ONE_HOUR = Amount.of(1L, Time.HOURS);

  private static final String JOB_A = "jobA";
  private static final String JOB_B = "jobB";

  private static final int PER_JOB_HISTORY = 2;

  private final AtomicInteger taskIdCounter = new AtomicInteger();

  private FakeClock clock;

  private HistoryPruner pruner;

  @Before
  public void setUp() {
    clock = new FakeClock();
    pruner = new HistoryPrunerImpl(clock, ONE_DAY, PER_JOB_HISTORY);
  }

  @Test
  public void testNoTasks() {
    assertTrue(pruner.apply(ImmutableSet.<ScheduledTask>of()).isEmpty());
  }

  @Test
  public void testNoPruning() {
    ScheduledTask a = makeTask(JOB_A);
    changeState(a, RUNNING);
    clock.advance(ONE_HOUR);
    changeState(a, FAILED);
    clock.advance(ONE_HOUR);

    assertTrue(pruner.apply(ImmutableSet.of(a)).isEmpty());
  }

  @Test
  public void testActiveTaskCandidate() {
    for (ScheduleStatus status : Tasks.ACTIVE_STATES) {
      ScheduledTask a = makeTask(JOB_A);
      changeState(a, status);
      clock.advance(ONE_DAY);
      clock.advance(ONE_DAY);

      try {
        pruner.apply(ImmutableSet.of(a));
        fail("Should have thrown for state " + status);
      } catch (IllegalArgumentException e) {
        // Expected.
      }
    }
  }

  @Test
  public void testPruneJobThreshold() {
    ScheduledTask a = makeTask(JOB_A);
    changeState(a, RUNNING);
    clock.advance(ONE_HOUR);

    ScheduledTask b = makeTask(JOB_A);
    changeState(b, FINISHED);
    clock.advance(ONE_HOUR);

    ScheduledTask c = makeTask(JOB_A);
    changeState(c, KILLED);
    clock.advance(ONE_HOUR);

    // Despite being the oldest task, task a had the most recent state change and should
    // not be pruned.
    changeState(a, FINISHED);
    clock.advance(ONE_HOUR);

    assertEquals(ImmutableSet.of(b), pruner.apply(ImmutableSet.of(a, b, c)));
  }

  @Test
  public void testPruneAgeThreshold() {
    ScheduledTask a = makeTask(JOB_A);
    changeState(a, RUNNING);
    clock.advance(ONE_HOUR);
    changeState(a, FINISHED);
    clock.advance(ONE_DAY);
    clock.advance(ONE_HOUR);

    ScheduledTask b = makeTask(JOB_A);
    changeState(b, RUNNING);
    clock.advance(ONE_DAY);
    clock.advance(ONE_HOUR);
    changeState(b, FINISHED);
    clock.advance(ONE_HOUR);

    assertEquals(ImmutableSet.of(a), pruner.apply(ImmutableSet.of(a, b)));
  }

  private ScheduledTask makeTask(String jobName) {
    return new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTaskId("task-" + taskIdCounter.get())
            .setTask(new TwitterTaskInfo()
                .setJobName(jobName)
                .setOwner(new Identity().setRole("role").setUser("user"))));
  }

  private ScheduledTask changeState(ScheduledTask task, ScheduleStatus status) {
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), status));
    task.setStatus(status);
    return task;
  }
}
