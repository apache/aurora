package com.twitter.nexus.executor;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.twitter.common.Pair;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.io.FileUtils;
import com.twitter.nexus.gen.TwitterTaskInfo;
import nexus.TaskState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * Tests for RunningTask.
 *
 * @author wfarner
 */
public class RunningTaskTest {

  private File executorRoot;

  // Simple task to create a file.
  private static final int TASK_ID_A = 1;
  private static final TwitterTaskInfo TASK_A = new TwitterTaskInfo()
      .setOwner("OWNER_A")
      .setJobName("JOB_A")
      .setStartCommand("touch a.txt")
      .setHdfsPath("/fake/path");
  private RunningTask taskA;
  private TwitterTaskInfo taskObj;

  @Before
  public void setUp() throws Exception {
    executorRoot = FileUtils.createTempDir();
    taskObj = new TwitterTaskInfo(TASK_A);
    taskA = new RunningTask(executorRoot, TASK_ID_A, taskObj, FILE_COPIER);
  }

  @After
  public void tearDown() throws Exception {
    if (executorRoot.exists()) org.apache.commons.io.FileUtils.deleteDirectory(executorRoot);
  }

  @Test
  public void testStage() throws Exception {
    taskObj.setStartCommand("touch a.txt");
    taskA.stage();
    assertDirContents(executorRoot, taskObj.getOwner());
    assertDirContents(new File(executorRoot, taskObj.getOwner()), taskObj.getJobName());
    assertThat(taskA.taskRoot.getParentFile().list().length, is(1));
  }

  @Test
  public void testLaunchCreatesOutputFile() throws Exception {
    taskObj.setStartCommand("touch a.txt");
    taskA.stage();
    taskA.launch();
    assertThat(taskA.waitFor(), is(TaskState.TASK_FINISHED));
    assertDirContents(taskA.taskRoot, "a.txt", "stderr", "stdout", "pidfile");
  }

  @Test
  public void testLaunchCapturesStdout() throws Exception {
    taskObj.setStartCommand("echo \"hello world\"");
    taskA.stage();
    taskA.launch();
    assertThat(taskA.waitFor(), is(TaskState.TASK_FINISHED));
    assertDirContents(taskA.taskRoot, "stderr", "stdout", "pidfile");

    assertThat(Files.readLines(new File(taskA.taskRoot, "stdout"), Charsets.UTF_8),
        is(Arrays.asList("hello world")));
  }

  @Test
  public void testLaunchErrorCode() throws Exception {
    taskObj.setStartCommand("exit 2");
    taskA.stage();
    taskA.launch();
    assertThat(taskA.waitFor(), is(TaskState.TASK_FAILED));
    assertThat(taskA.getExitCode(), is(2));
  }

  @Test
  // TODO(wfarner): This test is flaky when running from the command line - figure out
  // a better way to do this.
  public void testKill() throws Exception {
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
    assertDirContents(taskA.taskRoot, "b.txt", "stderr", "stdout", "pidfile");
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
    taskObj.setStartCommand("echo '%port:myport%'");
    taskA.socketManager = new SocketManager(10000, 10001);

    taskA.stage();
    taskA.launch();

    // Lease the remaining socket.
    taskA.socketManager.leaseSocket();

    // Subsequent leases should fail.
    try {
      taskA.socketManager.leaseSocket();
      fail();
    } catch (SocketManager.SocketLeaseException e) {
      // Expected.
    }

    assertThat(taskA.waitFor(), is(TaskState.TASK_FINISHED));

    // Should be able to lease one socket, since one was freed when the task terminated.
    taskA.socketManager.leaseSocket();
  }

  @Test
  public void testReleasesPortsKill() throws Exception {
    taskObj.setStartCommand("echo '%port:myport%'; sleep 10");
    taskA.socketManager = new SocketManager(10000, 10001);
    taskA.stage();
    taskA.launch();

    // Lease the remaining socket.
    taskA.socketManager.leaseSocket();

    // Subsequent leases should fail.
    try {
      taskA.socketManager.leaseSocket();
      fail();
    } catch (SocketManager.SocketLeaseException e) {
      // Expected.
    }

    taskA.kill();

    // Should be able to lease one socket, since one was freed when the task was killed.
    taskA.socketManager.leaseSocket();
  }

  @Test
  public void testLeasePort() throws Exception {
    taskObj.setStartCommand("echo '%port:http%'");
    taskA.socketManager = new SocketManager(10000, 11000);

    Pair<String, Map<String, Integer>> expanded = taskA.expandCommandLine();

    assertThat(expanded.getFirst().matches("echo '\\d+'"), is(true));
    Map<String, Integer> assignedPorts = expanded.getSecond();
    assertMappedValuesRange(assignedPorts, 10000, 11000, "http");
  }

  @Test
  public void testLeasePorts() throws Exception {
    taskObj.setStartCommand("echo '%port:http%'; echo '%port:thrift%'; echo '%port:mail%'");
    taskA.socketManager = new SocketManager(10000, 11000);

    Pair<String, Map<String, Integer>> expanded = taskA.expandCommandLine();

    assertThat(expanded.getFirst().matches("echo '\\d+'; echo '\\d+'; echo '\\d+'"), is(true));
    Map<String, Integer> assignedPorts = expanded.getSecond();
    assertMappedValuesRange(assignedPorts, 10000, 11000, "http", "thrift", "mail");
  }

  @Test
  public void testLeasePortsDuplicateName() throws Exception {
    taskObj.setStartCommand("echo '%port:http%'; echo '%port:http%'");
    taskA.socketManager = new SocketManager(10000, 11000);

    try {
      taskA.expandCommandLine();
      fail();
    } catch (RunningTask.ProcessException e) {
      // Expected.
    }
  }

  @Test
  public void testLeasePortsNoneAvailable() throws Exception {
    taskObj.setStartCommand("echo '%port:http%'; echo '%port:thrift%'; echo '%port:mail%'");
    taskA.socketManager = new SocketManager(10000, 10001);

    try {
      taskA.expandCommandLine();
      fail();
    } catch (SocketManager.SocketLeaseException e) {
      // Expected
    }
  }

  @Test
  public void testTearDown() throws Exception {
    taskA.stage();
    taskA.launch();
    taskA.tearDown();

    assertThat(Lists.newArrayList(executorRoot.list()), is(new ArrayList<String>()));
  }

  private void assertMappedValuesRange(Map<String, Integer> map, int min, int max, String... keys) {
    assertThat(map.size(), is(keys.length));
    for (String key : keys) {
      assertThat(map.containsKey(key), is(true));
      int value = map.get(key);
      assertThat(value >= min, is(true));
      assertThat(value <= max, is(true));
    }
    assertThat(Sets.newHashSet(map.values()).size(), is(keys.length));
  }

  private void assertDirContents(File dir, String... children) {
    assertThat(dir.exists(), is(true));
    assertThat(dir.isDirectory(), is(true));
    assertThat(Sets.newHashSet(dir.list()), is(Sets.newHashSet(children)));
  }

  private static final ExceptionalFunction<FileCopyRequest, File, IOException> FILE_COPIER =
      new ExceptionalFunction<FileCopyRequest, File, IOException>() {
        @Override public File apply(FileCopyRequest copy) throws IOException {
          return new File(copy.getDestPath());
        }
      };
}
