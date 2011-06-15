package com.twitter.mesos.executor;

import java.io.File;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;

import com.google.common.base.Function;

import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.BuildInfo;
import com.twitter.mesos.Message;
import com.twitter.mesos.executor.ExecutorCore.StateChange;
import com.twitter.mesos.executor.Task.TaskRunException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TwitterTaskInfo;

import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.fail;

/**
 * @author William Farner
 */
public class ExecutorCoreTest extends EasyMockTest {
  private static final String USER_A = "user-a";
  private static final String ROLE_A = "role-a";
  private static final Identity OWNER_A = new Identity(ROLE_A, USER_A);
  private static final String JOB_A = "job-a";

  private Function<AssignedTask, Task> taskFactory;
  private ExecutorService taskExecutor;
  private Function<Message, Integer> messageHandler;
  private Closure<ExecutorCore.StateChange> stateChangeCallback;
  private Task runningTask;

  private ExecutorCore executor;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    taskFactory = createMock(Function.class);
    taskExecutor = createMock(ExecutorService.class);
    messageHandler = createMock(Function.class);
    stateChangeCallback = createMock(Closure.class);
    runningTask = createMock(Task.class);

    executor = new ExecutorCore(
        new File("/dev/null"),
        new BuildInfo(),
        taskFactory,
        taskExecutor,
        messageHandler);
  }

  @Test
  public void testRunTask() throws Exception {
    AssignedTask task = makeTask(OWNER_A, JOB_A);

    expect(taskFactory.apply(task)).andReturn(runningTask);
    stateChange(STARTING);
    runningTask.stage();
    stateChange(RUNNING);
    runningTask.run();
    expect(runningTask.blockUntilTerminated()).andReturn(FINISHED);
    Capture<Runnable> taskCapture = new Capture<Runnable>();
    taskExecutor.execute(capture(taskCapture));
    stateChange(FINISHED);

    control.replay();

    executor.executeTask(task, stateChangeCallback);
    taskCapture.getValue().run();
  }

  @Test
  public void testTaskFails() throws Exception {
    AssignedTask task = makeTask(OWNER_A, JOB_A);

    expect(taskFactory.apply(task)).andReturn(runningTask);
    stateChange(STARTING);
    runningTask.stage();
    stateChange(RUNNING);
    runningTask.run();
    expect(runningTask.blockUntilTerminated()).andReturn(FAILED);
    Capture<Runnable> taskCapture = new Capture<Runnable>();
    taskExecutor.execute(capture(taskCapture));
    stateChange(FAILED);

    control.replay();

    executor.executeTask(task, stateChangeCallback);
    taskCapture.getValue().run();
  }

  @Test
  public void testStagingFails() throws Exception {
    AssignedTask task = makeTask(OWNER_A, JOB_A);

    expect(taskFactory.apply(task)).andReturn(runningTask);
    stateChange(STARTING);
    runningTask.stage();
    expectLastCall().andThrow(new TaskRunException("Staging failed."));
    stateChange(FAILED, "Staging failed.");
    expect(runningTask.isRunning()).andReturn(false);
    runningTask.terminate(FAILED);

    control.replay();

    executor.executeTask(task, stateChangeCallback);
  }

  @Test
  public void testRunFails() throws Exception {
    AssignedTask task = makeTask(OWNER_A, JOB_A);

    expect(taskFactory.apply(task)).andReturn(runningTask);
    stateChange(STARTING);
    runningTask.stage();
    stateChange(RUNNING);
    runningTask.run();
    expectLastCall().andThrow(new TaskRunException("Failed to start."));
    stateChange(FAILED, "Failed to start.");
    expect(runningTask.isRunning()).andReturn(false);
    runningTask.terminate(FAILED);

    control.replay();

    executor.executeTask(task, stateChangeCallback);
  }

  @Test
  public void testDeleteActiveTask() throws Exception {
    AssignedTask task = makeTask(OWNER_A, JOB_A);

    expect(taskFactory.apply(task)).andReturn(runningTask);
    stateChange(STARTING);
    runningTask.stage();
    stateChange(RUNNING);
    runningTask.run();
    expect(runningTask.isRunning()).andReturn(true);
    expect(runningTask.blockUntilTerminated()).andReturn(FINISHED);
    Capture<Runnable> taskCapture = new Capture<Runnable>();
    taskExecutor.execute(capture(taskCapture));
    stateChange(FINISHED);

    control.replay();

    executor.executeTask(task, stateChangeCallback);

    try {
      executor.deleteCompletedTask(task.getTaskId());
      fail("Deletion of active task should be disallowed.");
    } catch (IllegalStateException e) {
      // Expected.
    }
    taskCapture.getValue().run();
  }

  private static AssignedTask makeTask(Identity owner, String jobName) {
    TwitterTaskInfo task = new TwitterTaskInfo()
        .setOwner(owner)
        .setJobName(jobName);

    return new AssignedTask()
        .setTaskId(String.valueOf(new Random().nextInt(10000)))
        .setTask(task);
  }

  private void stateChange(ScheduleStatus status) {
    stateChange(status, null);
  }

  private void stateChange(ScheduleStatus status, @Nullable String message) {
    stateChangeCallback.execute(new StateChange(status, message));
  }
}
