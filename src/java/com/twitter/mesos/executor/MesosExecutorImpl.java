package com.twitter.mesos.executor;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.mesos.ExecutorMessageMux;
import com.twitter.mesos.FrameworkMessageCodec;
import com.twitter.mesos.codec.Codec;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.ExecutorMessage;
import com.twitter.mesos.gen.ExecutorMessageType;
import com.twitter.mesos.gen.TwitterTaskInfo;
import mesos.Executor;
import mesos.ExecutorArgs;
import mesos.ExecutorDriver;
import mesos.FrameworkMessage;
import mesos.TaskDescription;
import mesos.TaskState;
import mesos.TaskStatus;
import org.apache.thrift.TBase;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MesosExecutorImpl extends Executor {

  static {
    System.loadLibrary("nexus");
  }

  private static final Logger LOG = Logger.getLogger(MesosExecutorImpl.class.getName());
  private final Codec<TwitterTaskInfo, byte[]> taskCodec = new ThriftBinaryCodec<TwitterTaskInfo>(
      TwitterTaskInfo.class);
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

  private final Map<ExecutorMessageType, Closure<TBase>> messageCallbacks = ImmutableMap.of(
      ExecutorMessageType.MACHINE_DRAIN, new Closure<TBase>() {
        @Override public void execute(TBase item) throws RuntimeException {
          LOG.info("Received request to drain machine of all tasks.");
          // TODO(wfarner): Implement.
        }
      },
      ExecutorMessageType.RESTART_EXECUTOR, new Closure<TBase>() {
        @Override public void execute(TBase item) throws RuntimeException {
          LOG.info("Received request to restart the executor.");
          System.exit(0);
        }
      }
  );

  @Override
  public void frameworkMessage(ExecutorDriver driver, FrameworkMessage message) {
    if (message.getData() == null) {
      LOG.info("Received empty framework message.");
      return;
    }

    try {
      ExecutorMessageMux.demux(
          new FrameworkMessageCodec<ExecutorMessage>(ExecutorMessage.class).decode(message),
          messageCallbacks);
    } catch (Codec.CodingException e) {
      LOG.log(Level.SEVERE, "Failed to decode framework message.", e);
    }
  }
}
