package com.twitter.nexus.executor;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.nexus.gen.TwitterTaskInfo;
import nexus.TaskState;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles storing information and performing duties related to a running task.
 *
 * @author wfarner
 */
public class RunningTask {
  private static final Logger LOG = Logger.getLogger(RunningTask.class.getName());

  private final File executorRoot;
  private final int taskId;
  private final TwitterTaskInfo task;
  private final File taskRoot;
  private Process process;

  private boolean running = false;
  private int exitCode = 0;

  @Inject
  private ExceptionalFunction<FileCopyRequest, File, IOException> fileCopier;

  public RunningTask(File executorRoot, int taskId, TwitterTaskInfo task) {
    Preconditions.checkNotNull(executorRoot);
    Preconditions.checkState(executorRoot.exists() && executorRoot.isDirectory());
    this.executorRoot = executorRoot;
    this.taskId = taskId;
    this.task = Preconditions.checkNotNull(task);
    taskRoot = new File(executorRoot,
        String.format("%s/%s/%d", task.getOwner(), task.getJobName(), taskId));
  }

  /**
   * Performs staging operations necessary to launch a task.
   * This will prepare the working directory for the task, and download the binary to run.
   *
   * @throws com.twitter.nexus.executor.RunningTask.ProcessException If there was an error that caused staging to fail.
   */
  public void stage() throws ProcessException {
    LOG.info(String.format("Staging task for job %s/%s", task.getOwner(), task.getJobName()));
    Preconditions.checkState(!taskRoot.exists());

    if (!taskRoot.mkdirs()) {
      throw new ProcessException(
          "Failed to create working directory: " + taskRoot.getAbsolutePath());
    }

    // Pull down the executable.
    File payload;
    try {
      payload = fileCopier.apply(new FileCopyRequest(task.getHdfsPath(), taskRoot.getAbsolutePath()));
    } catch (IOException e) {
      throw new ProcessException("Failed to fetch task binary.", e);
    }

    if (!payload.exists()) {
      throw new ProcessException("Unexpected state - binary does not exist!");
    }
  }

  public void launch() throws ProcessException {
    LOG.info("Executing from working directory: " + executorRoot.getAbsolutePath());
    List<String> commandLine = Arrays.asList(
        "bash",
        "-c",
        String.format("echo $PPID > pidfile && %s >stdout 2>stderr", task.getStartCommand())
    );

    LOG.info("Executing shell command: " + commandLine);

    ProcessBuilder processBuilder = new ProcessBuilder(commandLine);
    processBuilder.directory(taskRoot);

    try {
      process = processBuilder.start();
      running = true;
    } catch (IOException e) {
      throw new ProcessException("Failed to launch process.", e);
    }
  }

  /**
   * Waits for the launched task to terminate.
   *
   * @return The state that the task was in upon termination.
   */
  public TaskState waitFor() {
    Preconditions.checkNotNull(process);
    while (true) {
      try {
        exitCode = process.waitFor();
        if (exitCode == 0) {
          return TaskState.TASK_FINISHED;
        } else {
          LOG.info("Process terminated with exit code: " + exitCode);
          return TaskState.TASK_FAILED;
        }
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING,
            "Warning, Thread interrupted while waiting for process to finish.", e);
      } finally {
        running = false;
      }
    }
  }

  public boolean isRunning() {
    return running;
  }

  public int getExitCode() {
    return exitCode;
  }

  public void tearDown() throws IOException {
    Preconditions.checkNotNull(taskRoot);
    File jobDir = taskRoot.getParentFile();
    File ownerDir = jobDir.getParentFile();
    FileUtils.deleteDirectory(taskRoot);
    if (jobDir.list().length == 0) FileUtils.deleteDirectory(jobDir);
    if (ownerDir.list().length == 0) FileUtils.deleteDirectory(ownerDir);
  }

  public void kill() {
    Preconditions.checkNotNull(process);
    LOG.info("Killing task " + this);
    process.destroy();
    waitFor();
  }

  public String toString() {
    return String.format("%s/%s/%d", task.getOwner(), task.getJobName(), taskId);
  }

  class ProcessException extends Exception {
    public ProcessException(String msg, Throwable t) {
      super(msg, t);
    }

    public ProcessException(String msg) {
      super(msg);
    }
  }
}
