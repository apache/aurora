package com.twitter.mesos.scheduler;

import java.util.EnumSet;
import java.util.Set;

import com.google.common.base.Supplier;

import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
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
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED_BY_CLIENT;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static com.twitter.mesos.gen.ScheduleStatus.UNKNOWN;
import static com.twitter.mesos.scheduler.WorkItem.CREATE_TASK;
import static com.twitter.mesos.scheduler.WorkItem.DELETE;
import static com.twitter.mesos.scheduler.WorkItem.INCREMENT_FAILURES;
import static com.twitter.mesos.scheduler.WorkItem.KILL;
import static com.twitter.mesos.scheduler.WorkItem.RESCHEDULE;
import static com.twitter.mesos.scheduler.WorkItem.UPDATE_STATE;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

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
    stateMachine = new TaskStateMachine("test", taskReader, workSink, MISSING_GRACE_PERIOD, clock);
  }

  @Test
  public void testSimpleTransition() {
    expectWork(CREATE_TASK);
    expectWork(UPDATE_STATE).times(5);
    expect(taskReader.get()).andReturn(makeTask(false));
    expectWork(DELETE);

    control.replay();

    stateMachine.setState(PENDING);
    stateMachine.setState(ASSIGNED);
    stateMachine.setState(STARTING);
    stateMachine.setState(RUNNING);
    stateMachine.setState(FINISHED);
    stateMachine.setState(UNKNOWN);
  }

  @Test
  public void testDaemonRescheduled() {
    expectWork(CREATE_TASK);
    expectWork(UPDATE_STATE).times(5);
    expect(taskReader.get()).andReturn(makeTask(true));
    expectWork(RESCHEDULE);

    control.replay();

    stateMachine.setState(PENDING);
    stateMachine.setState(ASSIGNED);
    stateMachine.setState(STARTING);
    stateMachine.setState(RUNNING);
    stateMachine.setState(FINISHED);
  }

  @Test
  public void testPostTerminalTransitionDenied() {
    Set<ScheduleStatus> terminalStates = EnumSet.of(
        FINISHED, FAILED, KILLED, KILLED_BY_CLIENT, LOST);

    for (ScheduleStatus endState : terminalStates) {
      expectWork(CREATE_TASK);
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

      stateMachine.setState(PENDING);
      stateMachine.setState(ASSIGNED);
      stateMachine.setState(STARTING);
      stateMachine.setState(RUNNING);
      stateMachine.setState(endState);

      for (ScheduleStatus badTransition : terminalStates) {
        stateMachine.setState(badTransition);
      }

      control.verify();
      control.reset();
      stateMachine =
          new TaskStateMachine("test", taskReader, workSink, MISSING_GRACE_PERIOD, clock);
    }

    control.replay();  // Needed so the teardown verify doesn't break.
  }

  @Test
  public void testUnknownTask() {
    expectWork(KILL);

    control.replay();

    stateMachine.setState(UNKNOWN);
    stateMachine.setState(RUNNING);
  }

  @Test
  public void testLostTask() {
    expectWork(CREATE_TASK);
    expectWork(UPDATE_STATE).times(5);
    expectWork(RESCHEDULE);

    control.replay();

    stateMachine.setState(PENDING);
    stateMachine.setState(ASSIGNED);
    stateMachine.setState(STARTING);
    stateMachine.setState(RUNNING);
    stateMachine.setState(LOST);
  }

  @Test
  public void testKilledPending() {
    expectWork(CREATE_TASK);
    expectWork(UPDATE_STATE).times(2);

    control.replay();

    stateMachine.setState(PENDING);
    stateMachine.setState(KILLED_BY_CLIENT);
  }

  @Test
  public void testReschedulesMissing() {
    expectWork(CREATE_TASK);
    expectWork(UPDATE_STATE).times(3);

    control.replay();

    stateMachine.setState(PENDING);
    stateMachine.setState(ASSIGNED);
    stateMachine.setState(STARTING);
  }

  @Test
  public void testMissingAssignedRescheduledAfterGracePeriod() {
    ScheduledTask task = makeTask(false);
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.PENDING, null));

    expectWork(CREATE_TASK);
    expectWork(UPDATE_STATE).times(2);

    clock.waitFor(10);
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.ASSIGNED, null));
    expect(taskReader.get()).andReturn(task).times(3);
    expectWork(KILL);
    expectWork(RESCHEDULE);

    control.replay();

    stateMachine.setState(PENDING);
    stateMachine.setState(ASSIGNED);
    stateMachine.setState(UNKNOWN);
    clock.waitFor(MISSING_GRACE_PERIOD.as(Time.MILLISECONDS) - 1);
    stateMachine.setState(UNKNOWN);
    clock.waitFor(2);
    stateMachine.setState(UNKNOWN);
  }

  @Test
  public void testMissingStartingRescheduledImmediately() {
    expectWork(CREATE_TASK);
    expectWork(UPDATE_STATE).times(3);
    expectWork(RESCHEDULE);

    control.replay();

    stateMachine.setState(PENDING);
    stateMachine.setState(ASSIGNED);
    stateMachine.setState(STARTING);
    stateMachine.setState(UNKNOWN);

  }

  private IExpectationSetters<Boolean> expectWork(WorkItem work) {
    workSink.addWork(work, stateMachine);
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
