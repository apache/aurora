package com.twitter.nexus.executor;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

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

  @Before
  public void setUp() throws Exception {
    executorRoot = FileUtils.createTempDir();
    taskA = new RunningTask(executorRoot, TASK_ID_A, TASK_A, FILE_COPIER);
  }

  @After
  public void tearDown() throws Exception {
    org.apache.commons.io.FileUtils.deleteDirectory(executorRoot);
  }

  @Test
  public void testStage() throws Exception {
    TASK_A.setStartCommand("touch a.txt");
    taskA.stage();
    assertDirContents(executorRoot, TASK_A.getOwner());
    assertDirContents(new File(executorRoot, TASK_A.getOwner()), TASK_A.getJobName());
    assertThat(taskA.taskRoot.getParentFile().list().length, is(1));
  }

  @Test
  public void testLaunchCreatesOutputFile() throws Exception {
    TASK_A.setStartCommand("touch a.txt");
    taskA.stage();
    taskA.launch();
    assertThat(taskA.waitFor(), is(TaskState.TASK_FINISHED));
    assertDirContents(taskA.taskRoot, "a.txt", "stderr", "stdout", "pidfile");
  }

  @Test
  public void testLaunchCapturesStdout() throws Exception {
    TASK_A.setStartCommand("echo \"hello world\"");
    taskA.stage();
    taskA.launch();
    assertThat(taskA.waitFor(), is(TaskState.TASK_FINISHED));
    assertDirContents(taskA.taskRoot, "stderr", "stdout", "pidfile");

    assertThat(Files.readLines(new File(taskA.taskRoot, "stdout"), Charsets.UTF_8),
        is(Arrays.asList("hello world")));
  }

  @Test
  public void testLaunchErrorCode() throws Exception {
    TASK_A.setStartCommand("exit 2");
    taskA.stage();
    taskA.launch();
    assertThat(taskA.waitFor(), is(TaskState.TASK_FAILED));
    assertThat(taskA.getExitCode(), is(2));
  }

  @Test
  // TODO(wfarner): This test is flaky when running from the command line - figure out
  // a better way to do this.
  public void testKill() throws Exception {
    TASK_A.setStartCommand("touch b.txt; sleep 10");
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
  public void testTearDown() throws Exception {
    taskA.stage();
    taskA.launch();
    taskA.tearDown();

    assertThat(Lists.newArrayList(executorRoot.list()), is(new ArrayList<String>()));
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
