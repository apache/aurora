package com.twitter.mesos.scheduler;

import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;

import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.TaskStateMachine.WorkSink;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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

/**
 * @author William Farner
 */
public class TaskStateMachineTest extends EasyMockTest {

  private Supplier<Boolean> isJobUpdating;
  private Predicate<Iterable<TaskEvent>> taskTimeoutFilter;
  private WorkSink workSink;
  private FakeClock clock;
  private TaskStateMachine stateMachine;

  @Before
  public void setUp() {
    isJobUpdating = createMock(new Clazz<Supplier<Boolean>>() { });
    taskTimeoutFilter = createMock(new Clazz<Predicate<Iterable<TaskEvent>>>() { });
    workSink = createMock(WorkSink.class);
    clock = new FakeClock();
    stateMachine = makeStateMachine("test", makeTask(false));
  }

  private TaskStateMachine makeStateMachine(String taskId, ScheduledTask task) {
    return new TaskStateMachine(taskId, taskId /* Job key */, task,
        isJobUpdating, workSink, taskTimeoutFilter, clock, INIT);
  }

  @Test
  public void testSimpleTransition() {
    expectWork(UPDATE_STATE).times(5);
    expectWork(DELETE);

    control.replay();

    transition(stateMachine, PENDING);
    assertEquals(INIT, stateMachine.getPreviousState());
    transition(stateMachine, ASSIGNED);
    assertEquals(PENDING, stateMachine.getPreviousState());
    transition(stateMachine, STARTING);
    assertEquals(ASSIGNED, stateMachine.getPreviousState());
    transition(stateMachine, RUNNING);
    assertEquals(STARTING, stateMachine.getPreviousState());
    transition(stateMachine, FINISHED);
    assertEquals(RUNNING, stateMachine.getPreviousState());
    transition(stateMachine, UNKNOWN);
    assertEquals(FINISHED, stateMachine.getPreviousState());
  }

  @Test
  public void testDaemonRescheduled() {
    stateMachine = makeStateMachine("test", makeTask(true));
    expectWork(UPDATE_STATE).times(5);
    expectWork(RESCHEDULE);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, FINISHED);
  }

  @Test
  public void testPostTerminalTransitionDenied() {
    Set<ScheduleStatus> terminalStates = Tasks.TERMINAL_STATES;

    for (ScheduleStatus endState : terminalStates) {
      stateMachine = makeStateMachine("test", makeTask(false));
      expectWork(UPDATE_STATE).times(5);

      switch (endState) {
        case FAILED:
          expectWork(INCREMENT_FAILURES);
          break;

        case FINISHED:
          break;

        case KILLED:
        case LOST:
          expectWork(RESCHEDULE);
          break;

        case KILLING:
          expectWork(KILL);
          break;

        default:
          fail("Unknown state " + endState);
      }

      control.replay();

      transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, endState);

      for (ScheduleStatus badTransition : terminalStates) {
        transition(stateMachine, badTransition);
      }

      control.verify();
      control.reset();
    }

    control.replay();  // Needed so the teardown verify doesn't break.
  }

  @Test
  public void testUnknownTask() {
    expectWork(KILL);

    control.replay();

    transition(stateMachine, UNKNOWN, RUNNING);
  }

  @Test
  public void testLostTask() {
    expectWork(UPDATE_STATE).times(5);
    expectWork(RESCHEDULE);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, LOST);
  }

  @Test
  public void testKilledPending() {
    expectWork(UPDATE_STATE);
    expectWork(DELETE);

    control.replay();

    transition(stateMachine, PENDING, KILLING);
  }

  @Test
  public void testMissingAssignedRescheduledAfterGracePeriod() {
    expectWork(UPDATE_STATE).times(3);
    expect(taskTimeoutFilter.apply(EasyMock.<Iterable<TaskEvent>>anyObject()))
        .andReturn(false)
        .times(2);
    expect(taskTimeoutFilter.apply(EasyMock.<Iterable<TaskEvent>>anyObject())).andReturn(true);
    expectWork(RESCHEDULE);

    control.replay();

    // Move the task into ASSIGNED, and simulate two attempts to move it to UNKNOWN.
    // This could be triggered by an executor sending a full state update that does not include
    // the task, for example if the executor had not received the task when the message was sent.
    transition(stateMachine, PENDING, ASSIGNED, UNKNOWN);
    transition(stateMachine, UNKNOWN);
    assertThat(stateMachine.getState(), is(ScheduleStatus.ASSIGNED));
    // The filter was configured to mark the task as timed out on the third call, the task is
    // missing.
    transition(stateMachine, UNKNOWN);
    assertThat(stateMachine.getState(), is(ScheduleStatus.LOST));
  }

  @Test
  public void testMissingStartingRescheduledImmediately() {
    ScheduledTask task = makeTask(false);
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.PENDING));
    stateMachine = makeStateMachine("test", task);

    expectWork(UPDATE_STATE).times(4);
    expectWork(RESCHEDULE);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, UNKNOWN);
    assertThat(stateMachine.getState(), is(ScheduleStatus.LOST));
  }

  @Test
  public void testMissingRunningRescheduledImmediately() {
    ScheduledTask task = makeTask(false);
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.PENDING));
    stateMachine = makeStateMachine("test", task);

    expectWork(UPDATE_STATE).times(5);
    expectWork(RESCHEDULE);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, UNKNOWN);
    assertThat(stateMachine.getState(), is(ScheduleStatus.LOST));
  }

  @Test
  public void testRogueRestartedTask() {
    expectWork(UPDATE_STATE).times(5);
    expectWork(KILL).times(2);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, RESTARTING, RUNNING);
  }

  @Test
  public void testAllowsSkipStartingAndRunning() {
    expectWork(UPDATE_STATE).times(3);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, FINISHED);
  }

  @Test
  public void testAllowsSkipRunning() {
    expectWork(UPDATE_STATE).times(4);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, FINISHED);
  }

  @Test
  public void testHonorsMaxFailures() {
    ScheduledTask task = makeTask(false);
    task.getAssignedTask().getTask().setMaxTaskFailures(10);
    task.setFailureCount(8);
    stateMachine = makeStateMachine("test", task);

    expectWork(UPDATE_STATE).times(5);
    expectWork(RESCHEDULE);
    expectWork(INCREMENT_FAILURES);

    ScheduledTask rescheduled = task.deepCopy();
    TaskStateMachine rescheduledMachine = makeStateMachine("test2", rescheduled);
    rescheduled.setFailureCount(9);
    expectWork(UPDATE_STATE, rescheduledMachine).times(5);
    expectWork(INCREMENT_FAILURES, rescheduledMachine);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, FAILED);

    transition(rescheduledMachine, PENDING, ASSIGNED, STARTING, RUNNING, FAILED);
  }

  @Test
  public void testHonorsUnlimitedFailures() {
    ScheduledTask task = makeTask(false);
    task.getAssignedTask().getTask().setMaxTaskFailures(-1);
    task.setFailureCount(1000);
    stateMachine = makeStateMachine("test", task);

    expectWork(UPDATE_STATE).times(5);
    expectWork(RESCHEDULE);
    expectWork(INCREMENT_FAILURES);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, FAILED);
  }

  @Test
  public void testUpdate() {
    expectWork(UPDATE_STATE).times(6);
    expect(isJobUpdating.get()).andReturn(true);
    expectWork(UPDATE);
    expectWork(KILL);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, UPDATING, KILLED);
  }

  @Test
  public void testRollback() {
    expectWork(UPDATE_STATE).times(7);
    expect(isJobUpdating.get()).andReturn(true);
    expectWork(WorkCommand.ROLLBACK);
    expectWork(KILL).times(2);

    control.replay();

    transition(stateMachine, PENDING, ASSIGNED, STARTING, RUNNING, UPDATING, ROLLBACK, KILLED);
  }

  @Test
  public void testIllegalUpdate() {
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
