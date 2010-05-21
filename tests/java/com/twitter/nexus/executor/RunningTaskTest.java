package com.twitter.nexus.executor;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Provides;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.io.FileUtils;
import com.twitter.nexus.gen.TwitterTaskInfo;
import nexus.TaskState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
    System.out.println("Executor root: " + executorRoot.getAbsolutePath());

    taskA = new RunningTask(executorRoot, TASK_ID_A, TASK_A);
    Guice.createInjector(new TestModule()).injectMembers(taskA);
  }

  @After
  public void tearDown() throws Exception {
    org.apache.commons.io.FileUtils.deleteDirectory(executorRoot);
  }

  @Test
  public void testStage() throws Exception {
    TASK_A.setStartCommand("touch a.txt");
    taskA.stage();
    assertDirContents(executorRoot, "OWNER_A");
    assertDirContents(new File(executorRoot, "OWNER_A"), "JOB_A");
    assertDirContents(new File(executorRoot, "OWNER_A/JOB_A"), "1");
  }

  @Test
  public void testLaunchCreatesOutputFile() throws Exception {
    TASK_A.setStartCommand("touch a.txt");
    taskA.stage();
    taskA.launch();
    assertThat(taskA.waitFor(), is(TaskState.TASK_FINISHED));
    assertDirContents(new File(executorRoot, "OWNER_A/JOB_A/1"),
        "a.txt", "stderr", "stdout", "pidfile");
  }

  @Test
  public void testLaunchCapturesStdout() throws Exception {
    TASK_A.setStartCommand("echo 'hello world'");
    taskA.stage();
    taskA.launch();
    assertThat(taskA.waitFor(), is(TaskState.TASK_FINISHED));
    File jobDir = new File(executorRoot, "OWNER_A/JOB_A/1");
    assertDirContents(jobDir, "stderr", "stdout", "pidfile");

    assertThat(Files.readLines(new File(jobDir, "stdout"), Charsets.UTF_8),
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
  public void testKill() throws Exception {
    TASK_A.setStartCommand("sleep 10");
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
    assertThat(state.get(), is(TaskState.TASK_FAILED));
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

  private class TestModule extends AbstractModule {
    @Override protected void configure() {
      // Nothing to do here.
    }

    @Provides public FileSystem provideFileSystem() throws IOException {
      return FileSystem.get(new Configuration());
    }

    @Provides public ExceptionalFunction<FileCopyRequest, File, IOException> provideFileCopier(
        final FileSystem fileSystem) {
      return new ExceptionalFunction<FileCopyRequest, File, IOException>() {
        @Override public File apply(FileCopyRequest copy) throws IOException {
          return new File(copy.getDestPath());
        }
      };
    }
  }
}
