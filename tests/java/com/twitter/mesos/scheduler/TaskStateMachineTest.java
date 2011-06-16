package com.twitter.mesos.scheduler;

import java.util.EnumSet;
import java.util.Set;

import com.google.common.base.Supplier;

import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.StateMachine;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.TaskStateMachine.WorkSink;

import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.INIT;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED_BY_CLIENT;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RESTARTING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static com.twitter.mesos.gen.ScheduleStatus.UNKNOWN;
import static com.twitter.mesos.scheduler.WorkCommand.DELETE;
import static com.twitter.mesos.scheduler.WorkCommand.INCREMENT_FAILURES;
import static com.twitter.mesos.scheduler.WorkCommand.KILL;
import static com.twitter.mesos.scheduler.WorkCommand.RESCHEDULE;
import static com.twitter.mesos.scheduler.WorkCommand.UPDATE_STATE;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author William Farner
 */
public class TaskStateMachineTest extends EasyMockTest {

  private static final Amount<Long, Time> MISSING_GRACE_PERIOD = Amount.of(1L, Time.MINUTES);

  private TaskStateMachine stateMachine;

  private Supplier<ScheduledTask> taskReader;
  private WorkSink workSink;
  private FakeClock clock;

  @Before
  public void setUp() {
    taskReader = createMock(new Clazz<Supplier<ScheduledTask>>() {});
    workSink = createMock(WorkSink.class);
    clock = new FakeClock();
    stateMachine = makeStateMachine("test");
  }

  private TaskStateMachine makeStateMachine(String taskId) {
    return new TaskStateMachine(taskId, taskReader, workSink, MISSING_GRACE_PERIOD, clock, INIT);
  }

  @Test
  public void testSimpleTransition() {
    expectWork(UPDATE_STATE).times(5);
    expect(taskReader.get()).andReturn(makeTask(false));
    expectWork(DELETE);

    control.replay();

    stateMachine.updateState(PENDING)
        .updateState(ASSIGNED)
        .updateState(STARTING)
        .updateState(RUNNING)
        .updateState(FINISHED)
        .updateState(UNKNOWN);
  }

  @Test
  public void testDaemonRescheduled() {
    expectWork(UPDATE_STATE).times(5);
    expect(taskReader.get()).andReturn(makeTask(true));
    expectWork(RESCHEDULE);

    control.replay();

    stateMachine.updateState(PENDING)
        .updateState(ASSIGNED)
        .updateState(STARTING)
        .updateState(RUNNING)
        .updateState(FINISHED);
  }

  @Test
  public void testPostTerminalTransitionDenied() {
    Set<ScheduleStatus> terminalStates = EnumSet.of(
        FINISHED, FAILED, KILLED, KILLED_BY_CLIENT, LOST);

    for (ScheduleStatus endState : terminalStates) {
      expectWork(UPDATE_STATE).times(5);

      switch (endState) {
        case FAILED:
          expectWork(INCREMENT_FAILURES);
          expect(taskReader.get()).andReturn(makeTask(false));
          break;

        case FINISHED:
          expect(taskReader.get()).andReturn(makeTask(false));
          break;

        case KILLED:
          expect(taskReader.get()).andReturn(makeTask(false));
          break;

        case LOST:
          expectWork(RESCHEDULE);
          break;

        case KILLED_BY_CLIENT:
          expectWork(KILL);
      }

      control.replay();

      stateMachine.updateState(PENDING)
          .updateState(ASSIGNED)
          .updateState(STARTING)
          .updateState(RUNNING)
          .updateState(endState);

      for (ScheduleStatus badTransition : terminalStates) {
        stateMachine.updateState(badTransition);
      }

      control.verify();
      control.reset();
      stateMachine =
          new TaskStateMachine("test", taskReader, workSink, MISSING_GRACE_PERIOD, clock, INIT);
    }

    control.replay();  // Needed so the teardown verify doesn't break.
  }

  @Test
  public void testUnknownTask() {
    expectWork(KILL);

    control.replay();

    stateMachine.updateState(UNKNOWN)
        .updateState(RUNNING);
  }

  @Test
  public void testLostTask() {
    expectWork(UPDATE_STATE).times(5);
    expectWork(RESCHEDULE);

    control.replay();

    stateMachine.updateState(PENDING)
        .updateState(ASSIGNED)
        .updateState(STARTING)
        .updateState(RUNNING)
        .updateState(LOST);
  }

  @Test
  public void testKilledPending() {
    expectWork(UPDATE_STATE);
    expectWork(DELETE);

    control.replay();

    stateMachine.updateState(PENDING)
        .updateState(KILLED_BY_CLIENT);
  }

  @Test
  public void testReschedulesMissing() {
    expectWork(UPDATE_STATE).times(3);

    control.replay();

    stateMachine.updateState(PENDING)
        .updateState(ASSIGNED)
        .updateState(STARTING);
  }

  @Test
  public void testMissingAssignedRescheduledAfterGracePeriod() {
    ScheduledTask task = makeTask(false);
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.PENDING, null));

    expectWork(UPDATE_STATE).times(3);
    expectWork(RESCHEDULE);

    clock.waitFor(10);
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.ASSIGNED, null));
    expect(taskReader.get()).andReturn(task).times(3);

    control.replay();

    stateMachine.updateState(PENDING)
        .updateState(ASSIGNED)
        .updateState(UNKNOWN);
    clock.waitFor(MISSING_GRACE_PERIOD.as(Time.MILLISECONDS) - 1);
    stateMachine.updateState(UNKNOWN);
    assertThat(stateMachine.getState(), is(ScheduleStatus.ASSIGNED));
    clock.waitFor(2);
    stateMachine.updateState(UNKNOWN);
    assertThat(stateMachine.getState(), is(ScheduleStatus.LOST));
  }

  @Test
  public void testMissingStartingRescheduledImmediately() {
    ScheduledTask task = makeTask(false);
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.PENDING, null));
    expectWork(UPDATE_STATE).times(4);
    expectWork(RESCHEDULE);

    control.replay();

    stateMachine.updateState(PENDING)
        .updateState(ASSIGNED)
        .updateState(STARTING);
    stateMachine.updateState(UNKNOWN);
    assertThat(stateMachine.getState(), is(ScheduleStatus.LOST));
  }

  @Test
  public void testRogueRestartedTask() {
    expectWork(UPDATE_STATE).times(5);
    expectWork(KILL).times(2);

    control.replay();

    stateMachine.updateState(PENDING)
        .updateState(ASSIGNED)
        .updateState(STARTING)
        .updateState(RUNNING)
        .updateState(RESTARTING)
        .updateState(RUNNING);
  }

  @Test
  public void testAllowsSkipStartingAndRunning() {
    expectWork(UPDATE_STATE).times(3);
    expect(taskReader.get()).andReturn(makeTask(false));

    control.replay();

    stateMachine.updateState(PENDING)
        .updateState(ASSIGNED)
        .updateState(FINISHED);
  }

  @Test
  public void testAllowsSkipRunning() {
    expectWork(UPDATE_STATE).times(4);
    expect(taskReader.get()).andReturn(makeTask(false));

    control.replay();

    stateMachine.updateState(PENDING)
        .updateState(ASSIGNED)
        .updateState(STARTING)
        .updateState(FINISHED);
  }

  @Test
  public void testHonorsMaxFailures() {
    ScheduledTask task = makeTask(false);
    task.getAssignedTask().getTask().setMaxTaskFailures(10);
    task.setFailureCount(8);

    expectWork(UPDATE_STATE).times(5);
    expect(taskReader.get()).andReturn(task);
    expectWork(RESCHEDULE);
    expectWork(INCREMENT_FAILURES);

    TaskStateMachine rescheduledMachine = makeStateMachine("test2");
    ScheduledTask rescheduled = task.deepCopy();
    rescheduled.setFailureCount(9);
    expectWork(UPDATE_STATE, rescheduledMachine).times(5);
    expect(taskReader.get()).andReturn(rescheduled);
    expectWork(INCREMENT_FAILURES, rescheduledMachine);

    control.replay();

    stateMachine.updateState(PENDING)
        .updateState(ASSIGNED)
        .updateState(STARTING)
        .updateState(RUNNING)
        .updateState(FAILED);

    rescheduledMachine.updateState(PENDING)
        .updateState(ASSIGNED)
        .updateState(STARTING)
        .updateState(RUNNING)
        .updateState(FAILED);
  }

  @Test
  public void testHonorsUnlimitedFailures() {
    ScheduledTask task = makeTask(false);
    task.getAssignedTask().getTask().setMaxTaskFailures(-1);
    task.setFailureCount(1000);

    expectWork(UPDATE_STATE).times(5);
    expect(taskReader.get()).andReturn(task);
    expectWork(RESCHEDULE);
    expectWork(INCREMENT_FAILURES);

    control.replay();

    stateMachine.updateState(PENDING)
        .updateState(ASSIGNED)
        .updateState(STARTING)
        .updateState(RUNNING)
        .updateState(FAILED);
  }

  private IExpectationSetters<Void> expectWork(WorkCommand work) {
    return expectWork(work, stateMachine);
  }

  private IExpectationSetters<Void> expectWork(WorkCommand work, TaskStateMachine machine) {
    workSink.addWork(eq(work), eq(machine), EasyMock.<Closure<ScheduledTask>>anyObject());
    return expectLastCall();
  }

  private static ScheduledTask makeTask(boolean daemon) {
    return new ScheduledTask()
        .setAssignedTask(
            new AssignedTask()
                .setTask(
                    new TwitterTaskInfo()
                        .setIsDaemon(daemon)));
  }
}
