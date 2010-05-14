package com.twitter.nexus.executor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.twitter.nexus.gen.TwitterTaskInfo;
import com.twitter.nexus.util.HdfsUtil;
import nexus.ExecutorDriver;
import nexus.TaskDescription;
import nexus.TaskState;
import nexus.TaskStatus;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ExecutorCore
 *
 * @author Florian Leibert
 */
public class ExecutorCore {
  private static final Logger LOG = Logger.getLogger(ExecutorHub.class.getName());

  private final ConcurrentHashMap<Integer, TaskDescription> tasks = new ConcurrentHashMap<Integer, TaskDescription>();
  private final ConcurrentHashMap<Integer, Process> processes = new ConcurrentHashMap<Integer, Process>();
  private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];
  private final ExecutorMain.TwitterExecutorOptions options;
  private final FileSystem hadoopFileSystem;

  private final ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("NexusExecutor-[%d]").build();

  @Inject
  public ExecutorCore(ExecutorMain.TwitterExecutorOptions options, FileSystem hadoopFileSystem) {
    this.options = Preconditions.checkNotNull(options);
    this.hadoopFileSystem = Preconditions.checkNotNull(hadoopFileSystem);
  }

  // TODO(flo): Handle loss of connection with the ExecutorDriver.
  // TODO(flo): Do input validation on parameters.
  public void executePendingTask(final ExecutorDriver driver, final TwitterTaskInfo taskInfo,
                                 final TaskDescription task) {
    LOG.info("Received task for execution: " + taskInfo);

    // TODO(flo): Do not allow configuration to specify local working directory.
    final String localDirName = String
        .format("%s/%s/", taskInfo.getLocalWorkingDirectory(), task.getName());

    try {
      File localDir = new File(localDirName);
      LOG.info("Executing from working directory: " + localDir.getAbsolutePath());
      localDir.mkdirs();
      File stdOut = new File(localDir,"stdout");
      File stdErr = new File(localDir,"stderr");

      // Pull down the executable.
      File executorBinary = HdfsUtil
          .downloadFileFromHdfs(hadoopFileSystem, taskInfo.getHdfsPath(), localDirName);

      LOG.info("Downloaded executor binary to: " + executorBinary.getAbsolutePath());

      List<String> commandLine = Lists.newArrayList(taskInfo.getCmdLineArgs().split("\\s+"));

      LOG.info("Executing shell command: " + commandLine);

      ProcessBuilder processBuilder = new ProcessBuilder(commandLine);
      processBuilder.directory(new File(localDirName));

      final Process process = processBuilder.start();
      IOUtils.copy(process.getErrorStream(),new FileWriter(stdErr));
      IOUtils.copy(process.getInputStream(),new FileWriter(stdOut));

      processes.put(task.getTaskId(), process);
      tasks.put(task.getTaskId(), task);

      // Launch a thread to reap dead processes.
      // TODO(flo): Since you store the Process object, it would be cleaner to have a timer that
      // periodically scans the processes map for terminated processes.
      threadFactory.newThread(new Runnable() {
        @Override public void run() {
          LOG.info("Waiting for process to complete...");
          waitForProcess(process);
          LOG.info("Process completed.");
          // Make sure this process finished or failed, i.e., wasn't killed.
          // TODO(flo): What if remove() returns false?  It's an unexpected state, and should
          // at least be logged.
          if (processes.remove(task.getTaskId(), process)) {
            tasks.remove(task.getTaskId(), task);
            // TODO(benh): Send process.exitValue() back to scheduler.
            ByteBuffer bb = ByteBuffer.allocate(Long.SIZE);
            bb.putLong(process.exitValue());
            byte[] serializedExitValue = bb.array();

            driver.sendStatusUpdate(
                new TaskStatus(task.getTaskId(), TaskState.TASK_FINISHED, serializedExitValue));
          }
        }
      }).start();

    } catch (IOException e) {
      LOG.log(Level.WARNING,
          "IOException occurred while trying to launch a task with task Descrtiption:" + task.toString(), e);
      driver.sendStatusUpdate(new TaskStatus(task.getTaskId(), TaskState.TASK_FAILED, EMPTY_BYTE_ARRAY));
    } catch (Throwable t) {
      LOG.log(Level.WARNING,
          "Throwable was thrown while trying to launch a task with task Descrtiption:" + task.toString(), t);
      driver.sendStatusUpdate(new TaskStatus(task.getTaskId(), TaskState.TASK_FAILED, EMPTY_BYTE_ARRAY));
    }
  }

  private void waitForProcess(final Process process) {
    boolean waited = false;
    do {
      try {
        process.waitFor();
        waited = true;
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Warning, Thread interrupted while waiting for Process to finish.", e);
      }
    } while (!waited);
  }

  public void stopRunningTask(ExecutorDriver driver, int taskId) {
    TaskDescription task = tasks.get(taskId);
    if (task != null) {
      LOG.info("Killing task " + task.getName() + " with ID " + task.getTaskId());
      Process process = processes.remove(task.getTaskId());

      // Process may have died "naturally" and been cleaned up via reaper thread.
      if (process != null) {
        process.destroy();
        waitForProcess(process);
        tasks.remove(task.getTaskId(), task);
        driver.sendStatusUpdate(new TaskStatus(task.getTaskId(), TaskState.TASK_KILLED, EMPTY_BYTE_ARRAY));
      }
    }
  }

  public void shutdownCore(ExecutorDriver driver) {
    for (Map.Entry<Integer, TaskDescription> entry : tasks.entrySet()) {
      stopRunningTask(driver, entry.getKey());
    }
  }
}
