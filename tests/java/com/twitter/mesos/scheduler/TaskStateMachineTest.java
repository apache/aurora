package com.twitter.mesos.scheduler;

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
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.TaskStateMachine.TransitionListener;
import com.twitter.mesos.scheduler.TaskStateMachine.WorkSink;

import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.INIT;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLING;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RESTARTING;
import static com.twitter.mesos.gen.ScheduleStatus.ROLLBACK;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static com.twitter.mesos.gen.ScheduleStatus.UNKNOWN;
import static com.twitter.mesos.gen.ScheduleStatus.UPDATING;
import static com.twitter.mesos.scheduler.WorkCommand.DELETE;
import static com.twitter.mesos.scheduler.WorkCommand.INCREMENT_FAILURES;
import static com.twitter.mesos.scheduler.WorkCommand.KILL;
import static com.twitter.mesos.scheduler.WorkCommand.RESCHEDULE;
import static com.twitter.mesos.scheduler.WorkCommand.UPDATE;
import static com.twitter.mesos.scheduler.WorkCommand.UPDATE_STATE;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author William Farner
 */
public class TaskStateMachineTest extends EasyMockTest {

  private static final Amount<Long, Time> MISSING_GRACE_PERIOD = Amount.of(1L, Time.MINUTES);

  private TaskStateMachine stateMachine;

  private Supplier<ScheduledTask> taskReader;
  private Supplier<Boolean> isJobUpdating;
  private TransitionListener transitionListener;
  private WorkSink workSink;
  private FakeClock clock;

  @Before
  public void setUp() {
    taskReader = createMock(new Clazz<Supplier<ScheduledTask>>() {});
    isJobUpdating = createMock(new Clazz<Supplier<Boolean>>() {});
    transitionListener = createMock(TransitionListener.class);
    workSink = createMock(WorkSink.class);
    clock = new FakeClock();
    stateMachine = makeStateMachine("test");
  }

  private TaskStateMachine makeStateMachine(String taskId) {
    return new TaskStateMachine(taskId, taskReader, isJobUpdating, workSink, MISSING_GRACE_PERIOD,
        clock, INIT, transitionListener);
  }

  @Test
  public void testSimpleTransition() {
    expectTransitionCallbacks(INIT, PENDING, ASSIGNED, STARTING, RUNNING, FINISHED, UNKNOWN);
    expectWork(UPDATE_STATE).times(5);
    expect(taskReader.get()).andReturn(makeTask(false));
    expectWork(DELETE);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, FINISHED, UNKNOWN);
  }

  @Test
  public void testDaemonRescheduled() {
    expectTransitionCallbacks(INIT, PENDING, ASSIGNED, STARTING, RUNNING, FINISHED);
    expectWork(UPDATE_STATE).times(5);
    expect(taskReader.get()).andReturn(makeTask(true));
    expectWork(RESCHEDULE);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, FINISHED);
  }

  @Test
  public void testPostTerminalTransitionDenied() {
    Set<ScheduleStatus> terminalStates = Tasks.TERMINAL_STATES;

    for (ScheduleStatus endState : terminalStates) {
      expectTransitionCallbacks(INIT, PENDING, ASSIGNED, STARTING, RUNNING);
      expectWork(UPDATE_STATE).times(5);

      switch (endState) {
        case FAILED:
          expectWork(INCREMENT_FAILURES);
          expect(taskReader.get()).andReturn(makeTask(false));
          expectTransitionCallbacks(RUNNING, endState);
          break;

        case FINISHED:
          expect(taskReader.get()).andReturn(makeTask(false));
          expectTransitionCallbacks(RUNNING, endState);
          break;

        case KILLED:
        case LOST:
          expectWork(RESCHEDULE);
          expectTransitionCallbacks(RUNNING, endState);
          break;

        case KILLING:
          expectWork(KILL);
          expectTransitionCallbacks(RUNNING, endState);
      }

      control.replay();

      transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, endState);

      for (ScheduleStatus badTransition : terminalStates) {
        transition(stateMachine, badTransition);
      }

      control.verify();
      control.reset();
      stateMachine = new TaskStateMachine("test", taskReader, isJobUpdating, workSink,
          MISSING_GRACE_PERIOD, clock, INIT, transitionListener);
    }

    control.replay();  // Needed so the teardown verify doesn't break.
  }

  @Test
  public void testUnknownTask() {
    expectTransitionCallbacks(INIT, UNKNOWN);
    expectWork(KILL);

    control.replay();

    transition(stateMachine, UNKNOWN, RUNNING);
  }

  @Test
  public void testLostTask() {
    expectTransitionCallbacks(INIT, PENDING, ASSIGNED, STARTING, RUNNING, LOST);
    expectWork(UPDATE_STATE).times(5);
    expectWork(RESCHEDULE);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, LOST);
  }

  @Test
  public void testKilledPending() {
    expectTransitionCallbacks(INIT, PENDING, KILLING);
    expectWork(UPDATE_STATE);
    expectWork(DELETE);

    control.replay();

    transition(stateMachine, PENDING, KILLING);
  }

  @Test
  public void testMissingAssignedRescheduledAfterGracePeriod() {
    expectTransitionCallbacks(INIT, PENDING, ASSIGNED, LOST);
    expectWork(UPDATE_STATE).times(3);
    expectWork(RESCHEDULE);

    ScheduledTask task = makeTask(false);
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.PENDING, null));

    clock.waitFor(10);
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.ASSIGNED, null));
    expect(taskReader.get()).andReturn(task).times(3);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, UNKNOWN);
    clock.waitFor(MISSING_GRACE_PERIOD.as(Time.MILLISECONDS) - 1);
    transition(stateMachine, UNKNOWN);
    assertThat(stateMachine.getState(), is(ScheduleStatus.ASSIGNED));
    clock.waitFor(2);
    transition(stateMachine, UNKNOWN);
    assertThat(stateMachine.getState(), is(ScheduleStatus.LOST));
  }

  @Test
  public void testMissingStartingRescheduledImmediately() {
    transitionListener.transitioned(INIT, PENDING);
    transitionListener.transitioned(PENDING, ASSIGNED);
    transitionListener.transitioned(ASSIGNED, STARTING);
    transitionListener.transitioned(STARTING, LOST);

    ScheduledTask task = makeTask(false);
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.PENDING, null));
    expectWork(UPDATE_STATE).times(4);
    expectWork(RESCHEDULE);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, UNKNOWN);
    assertThat(stateMachine.getState(), is(ScheduleStatus.LOST));
  }

  @Test
  public void testMissingRunningRescheduledImmediately() {
    expectTransitionCallbacks(INIT, PENDING, ASSIGNED, STARTING, RUNNING, LOST);
    expectWork(UPDATE_STATE).times(5);
    expectWork(RESCHEDULE);

    ScheduledTask task = makeTask(false);
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.PENDING, null));

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, UNKNOWN);
    assertThat(stateMachine.getState(), is(ScheduleStatus.LOST));
  }

  @Test
  public void testRogueRestartedTask() {
    expectTransitionCallbacks(INIT, PENDING, ASSIGNED, STARTING, RUNNING, RESTARTING);
    expectWork(UPDATE_STATE).times(5);
    expectWork(KILL).times(2);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, RESTARTING, RUNNING);
  }

  @Test
  public void testAllowsSkipStartingAndRunning() {
    expectTransitionCallbacks(INIT, PENDING, ASSIGNED, FINISHED);
    expectWork(UPDATE_STATE).times(3);
    expect(taskReader.get()).andReturn(makeTask(false));

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, FINISHED);
  }

  @Test
  public void testAllowsSkipRunning() {
    expectTransitionCallbacks(INIT, PENDING, ASSIGNED, STARTING, FINISHED);
    expectWork(UPDATE_STATE).times(4);
    expect(taskReader.get()).andReturn(makeTask(false));

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, FINISHED);
  }

  @Test
  public void testHonorsMaxFailures() {
    expectTransitionCallbacks(INIT, PENDING, ASSIGNED, STARTING, RUNNING, FAILED);
    expectTransitionCallbacks(INIT, PENDING, ASSIGNED, STARTING, RUNNING, FAILED);

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

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, FAILED);
    transition(rescheduledMachine, PENDING, ASSIGNED, STARTING, RUNNING, FAILED);
  }

  @Test
  public void testHonorsUnlimitedFailures() {
    expectTransitionCallbacks(INIT, PENDING, ASSIGNED, STARTING, RUNNING, FAILED);

    ScheduledTask task = makeTask(false);
    task.getAssignedTask().getTask().setMaxTaskFailures(-1);
    task.setFailureCount(1000);

    expectWork(UPDATE_STATE).times(5);
    expect(taskReader.get()).andReturn(task);
    expectWork(RESCHEDULE);
    expectWork(INCREMENT_FAILURES);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, FAILED);
  }

  @Test
  public void testUpdate() {
    expectTransitionCallbacks(INIT, PENDING, ASSIGNED, STARTING, RUNNING, UPDATING, KILLED);
    expectWork(UPDATE_STATE).times(6);
    expect(isJobUpdating.get()).andReturn(true);
    expectWork(UPDATE);
    expectWork(KILL);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, UPDATING, KILLED);
  }

  @Test
  public void testRollback() {
    expectTransitionCallbacks(
        INIT, PENDING, ASSIGNED, STARTING, RUNNING, UPDATING, ROLLBACK, KILLED);
    expectWork(UPDATE_STATE).times(7);
    expect(isJobUpdating.get()).andReturn(true);
    expectWork(WorkCommand.ROLLBACK);
    expectWork(KILL).times(2);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, UPDATING, ROLLBACK, KILLED);
  }

  @Test
  public void testIllegalUpdate() {
    expectTransitionCallbacks(INIT, PENDING, ASSIGNED, STARTING, RUNNING);
    expectWork(UPDATE_STATE).times(4);
    expect(isJobUpdating.get()).andReturn(false);

    control.replay();

    try {
      transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, UPDATING);
      fail("Call should have failed.");
    } catch (IllegalStateException e) {
    //Expected
    }
  }

  @Test
  public void testKillingRequest() {
    expectTransitionCallbacks(INIT, PENDING, ASSIGNED, STARTING, RUNNING, KILLING, KILLED);
    expectWork(UPDATE_STATE).times(6);
    expectWork(KILL);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, KILLING, KILLED);
  }

  private static void transition(TaskStateMachine stateMachine, ScheduleStatus... states) {
    for (ScheduleStatus status : states) {
      stateMachine.updateState(status);
    }
  }

  private void expectTransitionCallbacks(ScheduleStatus... states) {
    ScheduleStatus previous = null;
    for (ScheduleStatus status : states) {
      if (previous != null) {
        transitionListener.transitioned(previous, status);
      }

      previous = status;
    }
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
