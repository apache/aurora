package com.twitter.mesos.executor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos.ExecutorArgs;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskDescription;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;

import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.executor.Task.TaskRunException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ExecutorMessage;
import com.twitter.mesos.gen.ScheduleStatus;

import static com.twitter.mesos.gen.ScheduleStatus.FAILED;

public class MesosExecutorImpl implements Executor {

  private static final Logger LOG = Logger.getLogger(MesosExecutorImpl.class.getName());

  private final CountDownLatch initialized = new CountDownLatch(1);
  private final ExecutorCore executorCore;
  private final Driver driver;
  private SlaveID slaveId;

  @Inject
  public MesosExecutorImpl(ExecutorCore executorCore, Driver driver) {
    this.executorCore = Preconditions.checkNotNull(executorCore);
    this.driver = Preconditions.checkNotNull(driver);
  }

  @Override
  public void init(ExecutorDriver executorDriver, ExecutorArgs executorArgs) {
    LOG.info("Initialized with driver " + executorDriver + " and args " + executorArgs);
    executorCore.setSlaveId(executorArgs.getSlaveId().getValue());
    driver.init(executorDriver, executorArgs);
    slaveId = executorArgs.getSlaveId();
    initialized.countDown();
  }

  public boolean awaitInit(Amount<Long, Time> timeout) throws InterruptedException {
    return initialized.await(timeout.as(Time.MILLISECONDS), TimeUnit.MILLISECONDS);
  }

  @Override
  public void launchTask(ExecutorDriver driverDoNotUse, TaskDescription task) {
    LOG.info(String.format("Running task %s with ID %s.", task.getName(), task.getTaskId()));

    final AssignedTask assignedTask;
    try {
      assignedTask = ThriftBinaryCodec.decode(AssignedTask.class, task.getData().toByteArray());
    } catch (ThriftBinaryCodec.CodingException e) {
      LOG.log(Level.SEVERE, "Error deserializing task object.", e);
      driver.sendStatusUpdate(task.getTaskId().getValue(), FAILED,
          "Failed to decode task description.");
      return;
    }

    Preconditions.checkArgument(task.getTaskId().getValue().equals(assignedTask.getTaskId()),
        "Fatal - task IDs do not match: " + task.getTaskId() + ", " + assignedTask.getTaskId());

    try {
      executorCore.executeTask(assignedTask, new Closure<ScheduleStatus>() {
        @Override public void execute(ScheduleStatus state) {
          driver.sendStatusUpdate(assignedTask.getTaskId(), state, null);
        }
      });
    } catch (TaskRunException e) {
      driver.sendStatusUpdate(assignedTask.getTaskId(), FAILED, e.getMessage());
    }
  }

  @Override
  public void killTask(ExecutorDriver executorDriver, TaskID taskID) {
    LOG.info("Received killTask request for " + taskID);
    executorCore.stopLiveTask(taskID.getValue());
  }

  @Override
  public void shutdown(ExecutorDriver driver) {
    LOG.info("Received shutdown command, terminating...");
    for (Task killedTask : executorCore.shutdownCore()) {
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(TaskID.newBuilder().setValue(killedTask.getId())).setSlaveId(slaveId)
          .setState(TaskState.TASK_KILLED).build());
    }
    driver.stop();
  }

  @Override
  public void error(ExecutorDriver driver, int code, String message) {
    LOG.info("Error received with code: " + code + " and message: " + message);
    shutdown(driver);
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] data) {
    if (data == null) {
      LOG.info("Received empty framework message.");
      return;
    }

    try {
      ExecutorMessage executorMsg = ThriftBinaryCodec.decode(ExecutorMessage.class, data);
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
