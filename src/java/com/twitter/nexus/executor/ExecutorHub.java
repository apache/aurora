package com.twitter.nexus.executor;

import com.google.inject.Inject;
import com.twitter.nexus.gen.ExecutorQuery;
import com.twitter.nexus.gen.ExecutorQueryResponse;
import com.twitter.nexus.gen.TwitterTaskInfo;
import com.twitter.nexus.scheduler.persistence.Codec;
import com.twitter.nexus.scheduler.persistence.ThriftBinaryCodec;
import nexus.Executor;
import nexus.ExecutorDriver;
import nexus.FrameworkMessage;
import nexus.TaskDescription;
import nexus.TaskState;
import nexus.TaskStatus;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ExecutorHub extends Executor {

  static {
    System.loadLibrary("nexus");
  }

  private static final Logger LOG = Logger.getLogger(ExecutorHub.class.getName());
  private final Codec<TwitterTaskInfo, byte[]> taskCodec = new ThriftBinaryCodec<TwitterTaskInfo>(
      TwitterTaskInfo.class);
  private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private final Codec<ExecutorQuery, byte[]> queryCodec = new ThriftBinaryCodec<ExecutorQuery>(
      ExecutorQuery.class);
  private final Codec<ExecutorQueryResponse, byte[]> queryResponseCodec =
      new ThriftBinaryCodec<ExecutorQueryResponse>(ExecutorQueryResponse.class);

  @Inject private ExecutorCore executorCore;

  @Override
  public void launchTask(final ExecutorDriver driver, final TaskDescription task) {
    LOG.info(String.format("Running task %s with ID %d.", task.getName(), task.getTaskId()));

    TwitterTaskInfo taskInfo;
    try {
      taskInfo = taskCodec.decode(task.getArg());
    } catch (Codec.CodingException e) {
      LOG.log(Level.SEVERE, "Error deserializing task object.", e);
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
    FrameworkMessage response;
    try {
      response = new FrameworkMessage();
      response.setData(queryResponseCodec.encode(
          executorCore.query(queryCodec.decode(message.getData()))));
    } catch (Codec.CodingException e) {
      LOG.log(Level.SEVERE, "Failed to decode executor query.", e);
      return;
    }

    driver.sendFrameworkMessage(response);
  }
}
