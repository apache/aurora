package com.twitter.mesos.executor;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.twitter.common.collections.Pair;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.io.FileUtils;
import com.twitter.mesos.executor.HealthChecker.HealthCheckException;
import com.twitter.mesos.executor.ProcessKiller.KillCommand;
import com.twitter.mesos.executor.ProcessKiller.KillException;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TwitterTaskInfo;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.easymock.EasyMock.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for RunningTask.
 *
 * @author wfarner
 */
public class RunningTaskTest {

  private static final int PID = 12345;
  private static final int HTTP_PORT = 6789;

  private File executorRoot;

  // Simple task to create a file.
  private static final int TASK_ID_A = 1;
  private static final TwitterTaskInfo TASK_A = new TwitterTaskInfo()
      .setOwner("OWNER_A")
      .setJobName("JOB_A")
      .setStartCommand("touch a.txt")
      .setHdfsPath("/fake/path");
  private TwitterTaskInfo taskObj;

  private IMocksControl control;
  private SocketManager socketManager;
  private ExceptionalFunction<Integer, Boolean, HealthCheckException> healthChecker;
  private ExceptionalClosure<KillCommand, KillException> processKiller;
  private ExceptionalFunction<File, Integer, FileToInt.FetchException> pidFetcher;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    control = createControl();
    socketManager = control.createMock(SocketManager.class);
    healthChecker = control.createMock(ExceptionalFunction.class);
    processKiller = control.createMock(ExceptionalClosure.class);
    pidFetcher = control.createMock(ExceptionalFunction.class);

    executorRoot = FileUtils.createTempDir();
    taskObj = new TwitterTaskInfo(TASK_A);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (executorRoot.exists()) org.apache.commons.io.FileUtils.deleteDirectory(executorRoot);
    } catch (Throwable t) {
      // TODO(wfarner): Figure out why this fails occasionally.
    }
  }

  @Test
  public void testStage() throws Exception {
    taskObj.setStartCommand("touch a.txt");
    RunningTask taskA = makeTask(taskObj, TASK_ID_A);
    taskA.stage();
    assertDirContents(executorRoot, String.valueOf(TASK_ID_A));
    assertDirContents(taskA.getRootDir(), "sandbox", "task.dump");
  }

  @Test
  public void testLaunchCreatesOutputFile() throws Exception {
    expect(pidFetcher.apply((File) anyObject())).andReturn(PID);

    control.replay();

    taskObj.setStartCommand("touch a.txt");
    RunningTask taskA = makeTask(taskObj, TASK_ID_A);
    taskA.stage();
    taskA.run();
    assertThat(taskA.waitFor(), is(ScheduleStatus.FINISHED));
    assertDirContents(taskA.getRootDir(), "sandbox", "pidfile", "task.dump", "task.status");
    assertDirContents(taskA.sandboxDir, "a.txt", "run.sh", "stderr", "stdout");
  }

  @Test
  public void testLaunchCapturesStdout() throws Exception {
    expect(pidFetcher.apply((File) anyObject())).andReturn(PID);

    control.replay();

    taskObj.setStartCommand("echo \"hello world\"");
    RunningTask taskA = makeTask(taskObj, TASK_ID_A);
    taskA.stage();
    taskA.run();
    assertThat(taskA.waitFor(), is(ScheduleStatus.FINISHED));
    assertDirContents(taskA.getRootDir(), "sandbox", "pidfile", "task.dump", "task.status");
    assertDirContents(taskA.sandboxDir, "run.sh", "stderr", "stdout");

    assertThat(Files.readLines(new File(taskA.sandboxDir, "stdout"), Charsets.UTF_8),
        is(Arrays.asList("hello world")));
  }

  @Test
  public void testLaunchErrorCode() throws Exception {
    expect(pidFetcher.apply((File) anyObject())).andReturn(PID);

    control.replay();

    taskObj.setStartCommand("exit 2");
    RunningTask taskA = makeTask(taskObj, TASK_ID_A);
    taskA.stage();
    taskA.run();
    assertThat(taskA.waitFor(), is(ScheduleStatus.FAILED));
    assertThat(taskA.getExitCode(), is(2));
  }

  @Test
  // TODO(wfarner): This test is flaky when running from the command line - figure out
  // a better way to do this.
  public void testKill() throws Exception {
    /** TODO(wfarner): Fix this flaky test.
    taskObj.setStartCommand("touch b.txt; sleep 10");
    taskA.stage();
    taskA.launch();

    final AtomicReference<TaskState> state = new AtomicReference<TaskState>();

    final RunningTask taskCopy = taskA;
    Thread waitThread = new Thread() {
      @Override public void run() {
        state.set(taskCopy.waitFor());
      }
    };
    waitThread.start();

    // Wait for the task to start running.
    while (!taskA.isRunning()) {
      Thread.sleep(100);
    }

    taskA.kill();
    waitThread.join(1000);
    assertThat(waitThread.isAlive(), is(false));
    assertThat(state.get(), is(TaskState.TASK_KILLED));
    assertDirContents(taskA.getRootDir(), "b.txt", "stderr", "stdout", "pidfile");
    */
  }

  @Test
  public void testHealthCheckNoResponse() throws Exception {
    // TODO(wfarner): Change this into something that can actually be run as a unit test.
    /*
    taskObj.setStartCommand("echo '%port:health%'; sleep 45");
    taskA.socketManager = new SocketManager(10000, 11000);
    taskA.stage();
    taskA.launch();
    assertThat(taskA.waitFor(), is(TaskState.TASK_FAILED));
    assertThat(taskA.getExitCode(), is(2));
    */
  }

  @Test
  public void testReleasesPortsNormalShutdown() throws Exception {
    final int customPort = 4634;
    expect(socketManager.leaseSocket()).andReturn(customPort);
    expect(pidFetcher.apply((File) anyObject())).andReturn(PID);
    socketManager.returnSocket(customPort);

    control.replay();

    taskObj.setStartCommand("echo '%port:myport%'");
    RunningTask taskA = makeTask(taskObj, TASK_ID_A);
    taskA.stage();
    taskA.run();

    assertThat(taskA.waitFor(), is(ScheduleStatus.FINISHED));
  }

  @Test
  public void testReleasesPortsKill() throws Exception {
    final int customPort = 4634;
    expect(socketManager.leaseSocket()).andReturn(customPort);
    expect(pidFetcher.apply((File) anyObject())).andReturn(PID);
    processKiller.execute(new KillCommand(PID));
    socketManager.returnSocket(customPort);

    control.replay();

    taskObj.setStartCommand("echo '%port:myport%'; sleep 10");
    RunningTask taskA = makeTask(taskObj, TASK_ID_A);
    taskA.stage();
    taskA.run();

    taskA.terminate(ScheduleStatus.KILLED);
  }

  @Test
  public void testLeasePort() throws Exception {
    expect(socketManager.leaseSocket()).andReturn(HTTP_PORT);

    control.replay();

    taskObj.setStartCommand("echo '%port:http%'");
    RunningTask taskA = makeTask(taskObj, TASK_ID_A);

    Pair<String, Map<String, Integer>> expanded = taskA.expandCommandLine();

    assertThat(expanded.getFirst().matches("echo '\\d+'"), is(true));

    Map<String, Integer> expectedPorts = ImmutableMap.of("http", HTTP_PORT);
    assertThat(expanded.getSecond(), is(expectedPorts));
  }

  @Test
  public void testLeasePorts() throws Exception {
    final int thriftPort = 10000;
    final int mailPort = 10001;

    expect(socketManager.leaseSocket()).andReturn(HTTP_PORT);
    expect(socketManager.leaseSocket()).andReturn(thriftPort);
    expect(socketManager.leaseSocket()).andReturn(mailPort);

    control.replay();

    taskObj.setStartCommand("echo '%port:http%'; echo '%port:thrift%'; echo '%port:mail%'");
    RunningTask taskA = makeTask(taskObj, TASK_ID_A);

    Pair<String, Map<String, Integer>> expanded = taskA.expandCommandLine();

    assertThat(expanded.getFirst().matches("echo '\\d+'; echo '\\d+'; echo '\\d+'"), is(true));
    Map<String, Integer> expectedPorts = ImmutableMap.of(
        "http", HTTP_PORT,
        "thrift", thriftPort,
        "mail", mailPort);
    assertThat(expanded.getSecond(), is(expectedPorts));
  }

  @Test(expected = RunningTask.TaskRunException.class)
  public void testLeasePortsDuplicateName() throws Exception {
    taskObj.setStartCommand("echo '%port:http%'; echo '%port:http%'");
    makeTask(taskObj, TASK_ID_A).expandCommandLine();
  }

  @Test(expected = SocketManager.SocketLeaseException.class)
  public void testLeasePortsNoneAvailable() throws Exception {
    expect(socketManager.leaseSocket()).andReturn(HTTP_PORT);
    expect(socketManager.leaseSocket()).andReturn(10);
    expect(socketManager.leaseSocket()).andThrow(new SocketManager.SocketLeaseException("Empty"));

    control.replay();

    taskObj.setStartCommand("echo '%port:http%'; echo '%port:thrift%'; echo '%port:mail%'");
    makeTask(taskObj, TASK_ID_A).expandCommandLine();
  }

  private RunningTask makeTask(TwitterTaskInfo taskInfo, int taskId) {
    return new RunningTask(socketManager, healthChecker, processKiller, pidFetcher,
        new File(executorRoot, String.valueOf(taskId)), TASK_ID_A, taskInfo, COPIER);
  }

  private void assertDirContents(File dir, String... children) {
    assertThat(dir.exists(), is(true));
    assertThat(dir.isDirectory(), is(true));
    assertThat(Sets.newHashSet(dir.list()), is(Sets.newHashSet(children)));
  }

  private static final ExceptionalFunction<FileCopyRequest, File, IOException> COPIER =
      new ExceptionalFunction<FileCopyRequest, File, IOException>() {
        @Override public File apply(FileCopyRequest copy) throws IOException {
          return new File(copy.getDestPath());
        }
      };
}
