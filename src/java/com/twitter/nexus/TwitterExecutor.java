package com.twitter.nexus;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.twitter.nexus.gen.ConcreteTaskDescription;
import nexus.Executor;
import nexus.ExecutorDriver;
import nexus.TaskDescription;
import nexus.TaskState;
import nexus.TaskStatus;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TwitterExecutor extends Executor {

  private static Logger LOG = Logger.getLogger(TwitterExecutor.class.getName());

  static {
    System.loadLibrary("nexus");
  }

  private final TDeserializer deserializer = new TDeserializer();
  private final ConcurrentHashMap<Integer, TaskDescription> tasks = new ConcurrentHashMap<Integer, TaskDescription>();
  private final ConcurrentHashMap<Integer, Process> processes = new ConcurrentHashMap<Integer, Process>();

  private final ExecutorMain.TwitterExecutorOptions options;

  @Inject
  public TwitterExecutor(ExecutorMain.TwitterExecutorOptions options) {
    this.options = Preconditions.checkNotNull(options);
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

  @Override
  public void launchTask(final ExecutorDriver driver, final TaskDescription task) {

    LOG.info("Running task " + task.getName() + " with ID " + task.getTaskId());

    ConcreteTaskDescription concreteTaskDescription = new ConcreteTaskDescription();

    try {
      deserializer.deserialize(concreteTaskDescription, task.getArg());
    } catch (TException e) {
      LOG.log(Level.SEVERE, "Error deserializing Thrift ConcreteTaskDescription", e);
      throw new RuntimeException(e);
    }

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
          if (TwitterExecutor.this.processes.remove(task.getTaskId(), process)) {
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

  @Override
  public void killTask(ExecutorDriver driver, int taskId) {
    TaskDescription task = tasks.get(taskId);
    if (task != null) {
      LOG.info("Killing task " + task.getName() + " with ID " + task.getTaskId());
      Process process = processes.remove(task.getTaskId());

      // Process may have died "naturally" and been cleaned up via reaper thread.
      if (process != null) {
        process.destroy();
        waitForProcess(process);
        tasks.remove(task.getTaskId(), task);
        driver.sendStatusUpdate(new TaskStatus(task.getTaskId(), TaskState.TASK_KILLED, new byte[0]));
      }
    }
  }

  @Override
  public void shutdown(ExecutorDriver driver) {
    for (Map.Entry<Integer, TaskDescription> entry : tasks.entrySet()) {
      killTask(driver, entry.getKey());
    }
  }

  @Override
  public void error(ExecutorDriver driver, int code, String message) {
    LOG.info("Error received with code: " + code + " and message: " + message);
    shutdown(driver);
  }
}
