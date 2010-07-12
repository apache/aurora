package com.twitter.nexus.executor;

import com.google.inject.Inject;
import com.twitter.nexus.gen.TwitterTaskInfo;
import nexus.Executor;
import nexus.ExecutorDriver;
import nexus.FrameworkMessage;
import nexus.TaskDescription;
import nexus.TaskState;
import nexus.TaskStatus;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ExecutorHub extends Executor {

  static {
    System.loadLibrary("nexus");
  }

  private static final Logger LOG = Logger.getLogger(ExecutorHub.class.getName());
  private final TDeserializer deserializer = new TDeserializer();
  private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];

  @Inject
  private ExecutorCore executorCore;

  @Override
  public void launchTask(final ExecutorDriver driver, final TaskDescription task) {
    LOG.info(String.format("Running task %s with ID %d.", task.getName(), task.getTaskId()));

    TwitterTaskInfo taskInfo = new TwitterTaskInfo();
    try {
      deserializer.deserialize(taskInfo, task.getArg());
    } catch (TException e) {
      LOG.log(Level.SEVERE, "Error deserializing Thrift TwitterTaskInfo", e);
      driver.sendStatusUpdate(new TaskStatus(task.getTaskId(), TaskState.TASK_FAILED,
          EMPTY_BYTE_ARRAY));
      return;
    }

    executorCore.executePendingTask(driver, taskInfo, task);
  }

  @Override
  public void killTask(ExecutorDriver driver, int taskId) {
    executorCore.stopRunningTask(driver,taskId);
  }

  @Override
  public void shutdown(ExecutorDriver driver) {
    LOG.info("Received shutdown command, terminating...");
    executorCore.shutdownCore(driver);
  }

  @Override
  public void error(ExecutorDriver driver, int code, String message) {
    LOG.info("Error received with code: " + code + " and message: " + message);
    shutdown(driver);
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, FrameworkMessage message) {
    LOG.info("Received framework message.");
  }
}
