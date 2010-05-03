package com.twitter.nexus.executor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ExecutorCore
 *
 * @author Florian Leibert
 */
public class ExecutorCore {

  private static Logger LOG = Logger.getLogger(ExecutorHub.class.getName());

  private final ConcurrentHashMap<Integer, TaskDescription> tasks = new ConcurrentHashMap<Integer, TaskDescription>();
  private final ConcurrentHashMap<Integer, Process> processes = new ConcurrentHashMap<Integer, Process>();
  private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];
  private final ExecutorMain.TwitterExecutorOptions options;
  private final FileSystem hadoopFileSystem;

  @Inject
  public ExecutorCore(ExecutorMain.TwitterExecutorOptions options, FileSystem hadoopFileSystem) {
    this.options = Preconditions.checkNotNull(options);
    this.hadoopFileSystem = Preconditions.checkNotNull(hadoopFileSystem);
  }

  public void executePendingTask(final ExecutorDriver driver, final TwitterTaskInfo taskInfo,
                                 final TaskDescription task) {
    final String localDirName = String
        .format("%s/%s/", taskInfo.getLocalWorkingDirectory(), task.getName());

    try {
      File localDir = new File(localDirName);
      localDir.mkdirs();
      File stdOut = new File(localDir,"stdout");
      File stdErr = new File(localDir,"stderr");

      // Pull down the executable.
      File executorBinary = HdfsUtil
          .downloadFileFromHdfs(hadoopFileSystem, taskInfo.getHdfsPath(), localDirName);

      List<String> commandLine = Lists.newArrayList();
      final String[] args = taskInfo.getCmdLineArgs().split("\\s+");
      commandLine.addAll(Arrays.asList(args));

      ProcessBuilder processBuilder = new ProcessBuilder(commandLine);
      processBuilder.directory(new File(localDirName));


      final Process process = processBuilder.start();
      IOUtils.copy(process.getErrorStream(),new FileWriter(stdErr));
      IOUtils.copy(process.getInputStream(),new FileWriter(stdOut));


      processes.put(task.getTaskId(), process);
      tasks.put(task.getTaskId(), task);

      // Launch a thread to reap dead processes.
      new Thread() {
        public void run() {
          setDaemon(true);
          waitForProcess(process);
          // Make sure this process finished or failed, i.e., wasn't killed.
          if (ExecutorCore.this.processes.remove(task.getTaskId(), process)) {
            tasks.remove(task.getTaskId(), task);
            // TODO(benh): Send process.exitValue() back to scheduler.
            ByteBuffer bb = ByteBuffer.allocate(Long.SIZE);
            bb.putLong(process.exitValue());
            byte[] serializedExitValue = bb.array();

            driver.sendStatusUpdate(new TaskStatus(task.getTaskId(), TaskState.TASK_FINISHED, serializedExitValue));
          }
        }
      }.start();
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
