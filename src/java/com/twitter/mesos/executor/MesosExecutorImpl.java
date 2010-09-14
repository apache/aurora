package com.twitter.mesos.executor;

import com.google.inject.Inject;
import com.twitter.mesos.FrameworkMessageCodec;
import com.twitter.mesos.codec.Codec;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.ExecutorMessage;
import com.twitter.mesos.gen.TwitterTaskInfo;
import mesos.Executor;
import mesos.ExecutorArgs;
import mesos.ExecutorDriver;
import mesos.FrameworkMessage;
import mesos.TaskDescription;
import mesos.TaskState;
import mesos.TaskStatus;

import java.util.logging.Level;
import java.util.logging.Logger;

public class MesosExecutorImpl extends Executor {

  static {
    System.loadLibrary("mesos");
  }

  private static final Logger LOG = Logger.getLogger(MesosExecutorImpl.class.getName());
  private static final Codec<TwitterTaskInfo, byte[]> TASK_CODEC =
      new ThriftBinaryCodec<TwitterTaskInfo>(TwitterTaskInfo.class);
  private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];

  @Inject private ExecutorCore executorCore;

  @Override
  public void init(ExecutorDriver executorDriver, ExecutorArgs executorArgs) {
    executorCore.setSlaveId(executorArgs.getSlaveId());
  }

  @Override
  public void launchTask(final ExecutorDriver driver, final TaskDescription task) {
    LOG.info(String.format("Running task %s with ID %d.", task.getName(), task.getTaskId()));

    TwitterTaskInfo taskInfo;
    try {
      taskInfo = TASK_CODEC.decode(task.getArg());
    } catch (Codec.CodingException e) {
      LOG.log(Level.SEVERE, "Error deserializing task object.", e);
      driver.sendStatusUpdate(new TaskStatus(task.getTaskId(), TaskState.TASK_FAILED,
          EMPTY_BYTE_ARRAY));
      return;
    }

    executorCore.executePendingTask(driver, taskInfo, task.getTaskId());
  }

  @Override
  public void killTask(ExecutorDriver driver, int taskId) {
    executorCore.stopRunningTask(taskId);
  }

  @Override
  public void shutdown(ExecutorDriver driver) {
    LOG.info("Received shutdown command, terminating...");
    executorCore.shutdownCore();
    driver.stop();
  }

  @Override
  public void error(ExecutorDriver driver, int code, String message) {
    LOG.info("Error received with code: " + code + " and message: " + message);
    shutdown(driver);
  }

  private static final Codec<ExecutorMessage, FrameworkMessage> FRAMEWORK_MESSAGE_CODEC =
      new FrameworkMessageCodec<ExecutorMessage>(ExecutorMessage.class);

  @Override
  public void frameworkMessage(ExecutorDriver driver, FrameworkMessage message) {
    if (message.getData() == null) {
      LOG.info("Received empty framework message.");
      return;
    }

    try {
      ExecutorMessage executorMsg = FRAMEWORK_MESSAGE_CODEC.decode(message);
      if (!executorMsg.isSet()) {
        LOG.warning("Received empty executor message.");
        return;
      }

      switch (executorMsg.getSetField()) {
        case MACHINE_DRAIN:
          LOG.info("Received machine drain request.");
          break;
        case RESTART_EXECUTOR:
          LOG.info("Received executor restart request.");
          shutdown(null);
          break;
        default:
          LOG.warning("Received unhandled executor message type: " + executorMsg.getSetField());
      }
    } catch (Codec.CodingException e) {
      LOG.log(Level.SEVERE, "Failed to decode framework message.", e);
    }
  }
}
