package com.twitter.mesos.executor;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.executor.Task.TaskRunException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ExecutorMessage;
import com.twitter.mesos.gen.ScheduleStatus;
import mesos.Executor;
import mesos.ExecutorDriver;
import mesos.Protos.ExecutorArgs;
import mesos.Protos.FrameworkMessage;
import mesos.Protos.TaskDescription;
import mesos.Protos.TaskID;
import mesos.Protos.TaskState;
import mesos.Protos.TaskStatus;

import java.util.logging.Level;
import java.util.logging.Logger;

import static com.twitter.mesos.gen.ScheduleStatus.FAILED;

public class MesosExecutorImpl implements Executor {

  private static final Logger LOG = Logger.getLogger(MesosExecutorImpl.class.getName());

  private final ExecutorCore executorCore;
  private final Driver driver;

  @Inject
  public MesosExecutorImpl(ExecutorCore executorCore, Driver driver) {
    this.executorCore = Preconditions.checkNotNull(executorCore);
    this.driver = Preconditions.checkNotNull(driver);
  }

  @Override
  public void init(ExecutorDriver executorDriver, ExecutorArgs executorArgs) {
    LOG.info("Initialized with driver " + executorDriver + " and args " + executorArgs);
    executorCore.setSlaveId(executorArgs.getSlaveId().getValue());
    driver.setDriver(executorDriver, executorArgs.getFrameworkId());
  }

  @Override
  public void launchTask(final ExecutorDriver driverDoNotUse, final TaskDescription task) {
    LOG.info(String.format("Running task %s with ID %s.", task.getName(), task.getTaskId()));

    final AssignedTask assignedTask;
    try {
      assignedTask = ThriftBinaryCodec.decode(AssignedTask.class, task.getData().toByteArray());
    } catch (ThriftBinaryCodec.CodingException e) {
      LOG.log(Level.SEVERE, "Error deserializing task object.", e);
      driver.sendStatusUpdate(task.getTaskId().getValue(), FAILED);
      return;
    }

    Preconditions.checkArgument(task.getTaskId().getValue().equals(assignedTask.getTaskId()),
        "Fatal - task IDs do not match: " + task.getTaskId() + ", " + assignedTask.getTaskId());

    try {
      executorCore.executeTask(assignedTask, new Closure<ScheduleStatus>() {
        @Override public void execute(ScheduleStatus state) {
          driver.sendStatusUpdate(assignedTask.getTaskId(), state);
        }
      });
    } catch (TaskRunException e) {
      driver.sendStatusUpdate(assignedTask.getTaskId(), FAILED);
    }
  }

  @Override
  public void killTask(ExecutorDriver executorDriver, TaskID taskID) {
    executorCore.stopLiveTask(taskID.getValue());
  }

  @Override
  public void shutdown(ExecutorDriver driver) {
    LOG.info("Received shutdown command, terminating...");
    for (Task killedTask : executorCore.shutdownCore()) {
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(TaskID.newBuilder().setValue(killedTask.getId()))
          .setState(TaskState.TASK_KILLED)
          .build());
    }
    driver.stop();
  }

  @Override
  public void error(ExecutorDriver driver, int code, String message) {
    LOG.info("Error received with code: " + code + " and message: " + message);
    shutdown(driver);
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, FrameworkMessage message) {
    if (message.getData() == null) {
      LOG.info("Received empty framework message.");
      return;
    }

    try {
      ExecutorMessage executorMsg = ThriftBinaryCodec.decode(ExecutorMessage.class,
          message.getData().toByteArray());
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
          shutdown(driver);
          break;
        default:
          LOG.warning("Received unhandled executor message type: " + executorMsg.getSetField());
      }
    } catch (ThriftBinaryCodec.CodingException e) {
      LOG.log(Level.SEVERE, "Failed to decode framework message.", e);
    }
  }
}
