package com.twitter.mesos.executor;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.io.FileUtils;
import com.twitter.mesos.executor.HealthChecker.HealthCheckException;
import com.twitter.mesos.executor.ProcessKiller.KillCommand;
import com.twitter.mesos.executor.ProcessKiller.KillException;
import com.twitter.mesos.executor.Task.TaskRunException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TwitterTaskInfo;

import static com.twitter.mesos.executor.LiveTask.AuditedStatus;
import static com.twitter.mesos.executor.LiveTask.PIDFILE_NAME;
import static com.twitter.mesos.executor.LiveTask.RUN_SCRIPT_NAME;
import static com.twitter.mesos.executor.LiveTask.SANDBOX_DIR_NAME;
import static com.twitter.mesos.executor.LiveTask.STDERR_CAPTURE_FILE;
import static com.twitter.mesos.executor.LiveTask.STDOUT_CAPTURE_FILE;
import static com.twitter.mesos.executor.TaskOnDisk.TASK_DUMP_FILE;
import static com.twitter.mesos.executor.TaskOnDisk.TASK_STATUS_FILE;
import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createControl;
import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LiveTaskTest {

  private static final int PID = 12345;

  private File executorRoot;

  // Simple task to create a file.
  private static final String TASK_ID_A = "my-fake-task-id";
  private static final int SHARD_ID_A = 5;
  private static final TwitterTaskInfo TASK_A = new TwitterTaskInfo()
      .setOwner(new Identity(System.getProperty("user.name"), System.getProperty("user.name")))
      .setJobName("JOB_A")
      .setStartCommand("touch a.txt")
      .setShardId(SHARD_ID_A)
      .setHdfsPath("/fake/path");
  private AssignedTask taskObj;

  private IMocksControl control;
  private ExceptionalFunction<Integer, Boolean, HealthCheckException> healthChecker;
  private ExceptionalClosure<KillCommand, KillException> processKiller;
  private ExceptionalFunction<File, Integer, FileToInt.FetchException> pidFetcher;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    control = createControl();
    healthChecker = control.createMock(ExceptionalFunction.class);
    processKiller = control.createMock(ExceptionalClosure.class);
    pidFetcher = control.createMock(ExceptionalFunction.class);

    executorRoot = FileUtils.createTempDir();
    taskObj = new AssignedTask().setTask(TASK_A);
  }

  @After
  public void tearDown() throws Exception {
    /*
    try {
      if (executorRoot.exists()) org.apache.commons.io.FileUtils.deleteDirectory(executorRoot);
    } catch (Throwable t) {
      // TODO(William Farner): Figure out why this fails occasionally.
    }
    */
  }

  @Test
  public void testStage() throws Exception {
    taskObj.getTask().setStartCommand("touch a.txt");
    LiveTask taskA = makeTask(taskObj, TASK_ID_A);
    taskA.stage();
    assertDirContents(executorRoot, String.valueOf(TASK_ID_A));
    assertDirContents(taskA.taskRoot, SANDBOX_DIR_NAME, TASK_DUMP_FILE);
  }

  @Test
  public void testLaunchCreatesOutputFile() throws Exception {
    expect(pidFetcher.apply((File) anyObject())).andReturn(PID);

    control.replay();

    taskObj.getTask().setStartCommand("touch a.txt");
    LiveTask taskA = makeTask(taskObj, TASK_ID_A);
    taskA.stage();
    taskA.run();
    assertThat(taskA.blockUntilTerminated(), is(new AuditedStatus(FINISHED)));
    assertDirContents(taskA.taskRoot, SANDBOX_DIR_NAME, PIDFILE_NAME, TASK_DUMP_FILE,
        TASK_STATUS_FILE);
    assertDirContents(taskA.sandboxDir, "a.txt", RUN_SCRIPT_NAME, STDERR_CAPTURE_FILE,
        STDOUT_CAPTURE_FILE);
  }

  @Test
  public void testLaunchCapturesStdout() throws Exception {
    expect(pidFetcher.apply((File) anyObject())).andReturn(PID);

    control.replay();

    taskObj.getTask().setStartCommand("echo \"hello world\"");
    LiveTask taskA = makeTask(taskObj, TASK_ID_A);
    taskA.stage();
    taskA.run();
    assertThat(taskA.blockUntilTerminated(), is(new AuditedStatus(FINISHED)));
    assertDirContents(taskA.taskRoot, SANDBOX_DIR_NAME, PIDFILE_NAME, TASK_DUMP_FILE,
        TASK_STATUS_FILE);
    assertDirContents(taskA.sandboxDir, RUN_SCRIPT_NAME, STDERR_CAPTURE_FILE, STDOUT_CAPTURE_FILE);

    assertThat(Files.readLines(new File(taskA.sandboxDir, STDOUT_CAPTURE_FILE), Charsets.UTF_8),
        is(Arrays.asList("hello world")));
  }

  @Test
  public void testKillWhileStaging() throws Exception {
    final CountDownLatch stagingStarted = new CountDownLatch(1);
    final CountDownLatch stagingDone = new CountDownLatch(1);
    FileCopier copier = new FileCopier() {
      @Override public File apply(FileCopyRequest request) {
        try {
          stagingStarted.countDown();
          stagingDone.await();

          File payload = new File(request.getDestPath(), "payload");
          Files.write("Payload data".getBytes(), payload);
          return payload;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };

    expect(pidFetcher.apply((File) anyObject())).andReturn(PID);
    processKiller.execute(EasyMock.<KillCommand>anyObject());

    control.replay();

    taskObj.setTaskId(TASK_ID_A);
    final LiveTask task = new LiveTask(healthChecker, processKiller, pidFetcher,
        new File(executorRoot, String.valueOf(taskObj.getTaskId())), taskObj, copier, false);

    Thread stage = new Thread() {
      @Override public void run() {
        try {
          task.stage();
        } catch (TaskRunException e) {
          fail(e.getMessage());
        }
      }
    };

    final AuditedStatus status = new AuditedStatus(ScheduleStatus.FAILED, "Fail!");
    final CountDownLatch terminateStarted = new CountDownLatch(1);
    final CountDownLatch terminateDone = new CountDownLatch(1);
    Thread terminate = new Thread() {
      @Override public void run() {
        terminateStarted.countDown();
        task.terminate(status);
        terminateDone.countDown();
      }
    };

    stage.start();

    stagingStarted.await();
    terminate.start();
    terminateStarted.await();
    assertEquals(ScheduleStatus.STARTING, task.getStatus().getStatus());
    stagingDone.countDown();
    terminateDone.await();

    try {
      task.run();
      fail("Run should have failed.");
    } catch (IllegalStateException e) {
      // Expected.
    }
    assertThat(task.blockUntilTerminated(), is(status));
  }

  @Test
  public void testLaunchCapturesStderr() throws Exception {
    expect(pidFetcher.apply((File) anyObject())).andReturn(PID);

    control.replay();

    taskObj.getTask().setStartCommand("echo \"hello world\" >&2");
    LiveTask taskA = makeTask(taskObj, TASK_ID_A);
    taskA.stage();
    taskA.run();
    assertThat(taskA.blockUntilTerminated(), is(new AuditedStatus(FINISHED)));
    assertDirContents(taskA.taskRoot, SANDBOX_DIR_NAME, PIDFILE_NAME, TASK_DUMP_FILE,
        TASK_STATUS_FILE);
    assertDirContents(taskA.sandboxDir, RUN_SCRIPT_NAME, STDERR_CAPTURE_FILE, STDOUT_CAPTURE_FILE);

    assertThat(Files.readLines(new File(taskA.sandboxDir, STDERR_CAPTURE_FILE), Charsets.UTF_8),
        is(Arrays.asList("hello world")));
  }

  @Test
  public void testLaunchErrorCode() throws Exception {
    expect(pidFetcher.apply((File) anyObject())).andReturn(PID);

    control.replay();

    taskObj.getTask().setStartCommand("exit 2");
    LiveTask taskA = makeTask(taskObj, TASK_ID_A);
    taskA.stage();
    taskA.run();
    assertFalse(taskA.isCompleted());
    assertThat(taskA.blockUntilTerminated(), is(new AuditedStatus(FAILED)));
    assertThat(taskA.getExitCode(), is(2));
    assertTrue(taskA.isCompleted());
  }

  @Test
  public void testShutdown() throws Exception {
    expect(pidFetcher.apply((File) anyObject())).andReturn(PID);
    processKiller.execute(EasyMock.<KillCommand>anyObject());

    control.replay();

    taskObj.getTask().setStartCommand("sleep 1");
    final LiveTask taskA = makeTask(taskObj, TASK_ID_A);
    taskA.stage();
    taskA.run();

    assertFalse(taskA.isCompleted());
    AuditedStatus status = new AuditedStatus(KILLED, "Killed.");
    taskA.terminate(status);
    assertThat(taskA.blockUntilTerminated(), is(status));
  }

  @Test
  public void testHealthCheckFailure() throws Exception {
    final int customPort = 4634;
    expect(pidFetcher.apply((File) anyObject())).andReturn(PID);
    expect(healthChecker.apply(customPort)).andThrow(new HealthCheckException("Timeout."))
        .atLeastOnce();

    control.replay();

    taskObj.getTask().setHealthCheckIntervalSecs(1)
        .setStartCommand("sleep 40");
    taskObj.putToAssignedPorts(LiveTask.HEALTH_CHECK_PORT_NAME, customPort);
    LiveTask taskA = makeTask(taskObj, TASK_ID_A);
    taskA.stage();
    taskA.run();

    AuditedStatus status = taskA.blockUntilTerminated();
    assertThat(status.getStatus(), is(FAILED));
    assertThat(status.getMessage(), is(Optional.of(LiveTask.HEALTH_CHECK_FAIL_MSG)));
  }

  private LiveTask makeTask(AssignedTask task, String taskId) {
    task.setTaskId(taskId);
    return new LiveTask(healthChecker, processKiller, pidFetcher,
        new File(executorRoot, String.valueOf(task.getTaskId())), task, COPIER, false);
  }

  private void assertDirContents(File dir, String... children) {
    assertThat(dir.exists(), is(true));
    assertThat(dir.isDirectory(), is(true));
    assertThat(Sets.newHashSet(dir.list()), is(Sets.newHashSet(children)));
  }

  private static final FileCopier COPIER = new FileCopier() {
      @Override public File apply(FileCopyRequest copy) {
        return new File(copy.getDestPath());
      }
  };
}
