package com.twitter.mesos.executor;

import com.google.common.base.Function;
import com.twitter.common.base.Closure;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.BuildInfo;
import com.twitter.mesos.Message;
import com.twitter.mesos.executor.Task.TaskRunException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TwitterTaskInfo;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.fail;

/**
 * @author William Farner
 */
public class ExecutorCoreTest extends EasyMockTest {

  private static final String USER_A = "user-a";
  private static final String JOB_A = "job-a";

  private Function<AssignedTask, Task> taskFactory;
  private ExecutorService taskExecutor;
  private Function<Message, Integer> messageHandler;
  private Closure<ScheduleStatus> completedCallback;
  private Task runningTask;

  private ExecutorCore executor;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    taskFactory = createMock(Function.class);
    taskExecutor = createMock(ExecutorService.class);
    messageHandler = createMock(Function.class);
    completedCallback = createMock(Closure.class);
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
    AssignedTask task = makeTask(USER_A, JOB_A);

    expect(taskFactory.apply(task)).andReturn(runningTask);
    runningTask.stage();
    runningTask.run();
    expect(runningTask.blockUntilTerminated()).andReturn(FINISHED);
    Capture<Runnable> taskCapture = new Capture<Runnable>();
    taskExecutor.execute(capture(taskCapture));
    completedCallback.execute(FINISHED);

    control.replay();

    executor.executeTask(task, completedCallback);
    taskCapture.getValue().run();
  }

  @Test
  public void testTaskFails() throws Exception {
    AssignedTask task = makeTask(USER_A, JOB_A);

    expect(taskFactory.apply(task)).andReturn(runningTask);
    runningTask.stage();
    runningTask.run();
    expect(runningTask.blockUntilTerminated()).andReturn(FAILED);
    Capture<Runnable> taskCapture = new Capture<Runnable>();
    taskExecutor.execute(capture(taskCapture));
    completedCallback.execute(FAILED);

    control.replay();

    executor.executeTask(task, completedCallback);
    taskCapture.getValue().run();
  }

  @Test(expected = TaskRunException.class)
  public void testStagingFails() throws Exception {
    AssignedTask task = makeTask(USER_A, JOB_A);

    expect(taskFactory.apply(task)).andReturn(runningTask);
    runningTask.stage();
    expectLastCall().andThrow(new TaskRunException("Staging failed."));
    expect(runningTask.isRunning()).andReturn(false);
    runningTask.terminate(FAILED, "Staging failed.");

    control.replay();

    executor.executeTask(task, completedCallback);
  }

  @Test(expected = TaskRunException.class)
  public void testRunFails() throws Exception {
    AssignedTask task = makeTask(USER_A, JOB_A);

    expect(taskFactory.apply(task)).andReturn(runningTask);
    runningTask.stage();
    runningTask.run();
    expectLastCall().andThrow(new TaskRunException("Failed to start."));
    expect(runningTask.isRunning()).andReturn(false);
    runningTask.terminate(FAILED, "Failed to start.");

    control.replay();

    executor.executeTask(task, completedCallback);
  }

  @Test
  public void testDeleteActiveTask() throws Exception {
    AssignedTask task = makeTask(USER_A, JOB_A);

    expect(taskFactory.apply(task)).andReturn(runningTask);
    runningTask.stage();
    runningTask.run();
    expect(runningTask.isRunning()).andReturn(true);
    expect(runningTask.blockUntilTerminated()).andReturn(FINISHED);
    Capture<Runnable> taskCapture = new Capture<Runnable>();
    taskExecutor.execute(capture(taskCapture));
    completedCallback.execute(FINISHED);

    control.replay();

    executor.executeTask(task, completedCallback);

    try {
      executor.deleteCompletedTask(task.getTaskId());
      fail("Deletion of active task should be disallowed.");
    } catch (IllegalStateException e) {
      // Expected.
    }
    taskCapture.getValue().run();
  }

  private static AssignedTask makeTask(String owner, String jobName) {
    TwitterTaskInfo task = new TwitterTaskInfo()
        .setOwner(owner)
        .setJobName(jobName);

    return new AssignedTask()
        .setTaskId(String.valueOf(new Random().nextInt(10000)))
        .setTask(task);
  }
}
