package com.twitter.nexus;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.twitter.nexus.gen.TwitterTaskInfo;
import nexus.ExecutorDriver;
import nexus.TaskDescription;
import nexus.TaskState;
import nexus.TaskStatus;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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

  @Inject
  public ExecutorCore(ExecutorMain.TwitterExecutorOptions options) {
    this.options = Preconditions.checkNotNull(options);
  }

  public void executePendingTask(final ExecutorDriver driver, final TwitterTaskInfo concreteTaskDescription, final TaskDescription task) {
    final String localDirName = String
        .format("%s/%s", concreteTaskDescription.getLocalWorkingDirectory(), task.getName());

    try {
      // Pull down the executable.
      File executorBinary = downloadFileFromHdfs(concreteTaskDescription.getHdfsPath(), localDirName);

      List<String> commandLine = Lists.newArrayList(executorBinary.getAbsolutePath());
      commandLine.addAll(concreteTaskDescription.getCmdLineArgs());

      ProcessBuilder processBuilder = new ProcessBuilder(commandLine);
      processBuilder.directory(new File(concreteTaskDescription.getLocalWorkingDirectory()));

      final Process process = processBuilder.start();
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
    }
  }

  private File downloadFileFromHdfs(final String executorBinaryUrl, final String localDirName) throws IOException {
    Configuration conf = new Configuration(true);
    conf.addResource(new Path(options.hdfsConfig));
    conf.reloadConfiguration();
    FileSystem hdfs = FileSystem.get(conf);
    Path executorBinaryPath = new Path(executorBinaryUrl);
    FSDataInputStream remoteStream = hdfs.open(executorBinaryPath);
    File localFile = new File(localDirName + executorBinaryPath.getName());
    FileOutputStream localStream = new FileOutputStream(localFile);
    try {
      IOUtils.copy(remoteStream, localStream);
    } finally {
      IOUtils.closeQuietly(remoteStream);
      IOUtils.closeQuietly(localStream);
    }
    return localFile;
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
