package com.twitter.mesos.executor;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;

import com.twitter.common.application.Lifecycle;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.JNICallback;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.comm.AdjustRetainedTasks;
import com.twitter.mesos.gen.comm.ExecutorMessage;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;

public class MesosExecutorImpl implements Executor {

  private static final Logger LOG = Logger.getLogger(MesosExecutorImpl.class.getName());

  private final CountDownLatch initialized = new CountDownLatch(1);
  private final ExecutorCore executorCore;
  private final Driver driver;
  private final Lifecycle lifecycle;

  private volatile boolean shuttingDown = false;
  private final ShutdownRegistry shutdownRegistry;

  @Inject
  public MesosExecutorImpl(ExecutorCore executorCore, Driver driver,
      Lifecycle lifecycle, ShutdownRegistry shutdownRegistry) {
    this.executorCore = checkNotNull(executorCore);
    this.driver = checkNotNull(driver);
    this.lifecycle = checkNotNull(lifecycle);
    this.shutdownRegistry = checkNotNull(shutdownRegistry);
  }

  @JNICallback
  @Override
  public void registered(ExecutorDriver executorDriver,
      ExecutorInfo executorInfo,
      FrameworkInfo frameworkInfo,
      SlaveInfo slaveInfo) {

    LOG.info("Initialized with driver " + executorDriver + " and args " + executorInfo);
    executorCore.setSlaveId(slaveInfo.getId().getValue());
    driver.init(executorDriver, executorInfo);
    shutdownRegistry.addAction(new ExceptionalCommand<RuntimeException>() {
      @Override public void execute() {
        shuttingDown = true;
      }
    });
    initialized.countDown();
  }

  @Override
  public void reregistered(ExecutorDriver executorDriver, SlaveInfo slaveInfo) {
    LOG.info("Re-registered with " + executorDriver + " and slave info " + slaveInfo);
  }

  @Override
  public void disconnected(ExecutorDriver executorDriver) {
    LOG.info("Disconnected from slave.");
  }

  public boolean awaitInit(Amount<Long, Time> timeout) throws InterruptedException {
    return initialized.await(timeout.as(Time.MILLISECONDS), TimeUnit.MILLISECONDS);
  }

  @JNICallback
  @Override
  public void launchTask(ExecutorDriver driverDoNotUse, TaskInfo task) {
    if (shuttingDown) {
      LOG.warning(String.format("Rejecting task %s with ID %s since the executor is shutting down.",
          task.getName(), task.getTaskId()));
      driver.sendStatusUpdate(task.getTaskId().getValue(), LOST,
          Optional.of("Executor shutting down."));
      return;
    }

    LOG.info(String.format("Running task %s.", task.getTaskId()));

    final AssignedTask assignedTask;
    try {
      assignedTask = ThriftBinaryCodec.decode(AssignedTask.class, task.getData().toByteArray());
    } catch (ThriftBinaryCodec.CodingException e) {
      LOG.log(Level.SEVERE, "Error deserializing task object.", e);
      driver.sendStatusUpdate(task.getTaskId().getValue(), FAILED,
          Optional.of("Failed to decode task description."));
      return;
    }

    Preconditions.checkNotNull(assignedTask, "No task found in task description " + task);
    Preconditions.checkArgument(task.getTaskId().getValue().equals(assignedTask.getTaskId()),
        "Fatal - task IDs do not match: " + task.getTaskId() + ", " + assignedTask.getTaskId());

    executorCore.executeTask(assignedTask);
  }

  @JNICallback
  @Override
  public void killTask(ExecutorDriver driverDoNotUse, TaskID taskID) {
    LOG.info("Received killTask request for " + taskID);
    executorCore.stopLiveTask(taskID.getValue(), "Killed by framework.");
  }

  @JNICallback
  @Override
  public void shutdown(ExecutorDriver driverDoNotUse) {
    shuttingDown = true;
    LOG.info("Received shutdown command, terminating...");
    try {
      for (Task killedTask : executorCore.shutdownCore()) {
        driver.sendStatusUpdate(
            killedTask.getId(), ScheduleStatus.KILLED, Optional.<String>absent());
      }
      driver.stop();
    } finally {
      lifecycle.shutdown();
    }
  }

  @JNICallback
  @Override
  public void error(ExecutorDriver driver, String message) {
    LOG.info("Error received with message: " + message);
    shutdown(driver);
  }

  @JNICallback
  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] data) {
    if (data == null) {
      LOG.info("Received empty framework message.");
      return;
    }

    try {
      ExecutorMessage executorMsg = ThriftBinaryCodec.decode(ExecutorMessage.class, data);
      if (executorMsg == null || !executorMsg.isSet()) {
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

        case ADJUST_RETAINED_TASKS:
          AdjustRetainedTasks adjustment = executorMsg.getAdjustRetainedTasks();
          if (adjustment == null) {
            LOG.severe("Ignoring empty task adjustment.");
            return;
          }

          Map<String, ScheduleStatus> tasks = adjustment.getRetainedTasks();
          if (tasks == null) {
            LOG.severe("Ignoring empty tasks set in task adjustment");
            return;
          }

          LOG.info("Received request to adjust retained tasks to " + tasks);
          executorCore.adjustRetainedTasks(tasks);
          break;

        default:
          LOG.warning("Received unhandled executor message type: " + executorMsg.getSetField());
          break;
      }
    } catch (ThriftBinaryCodec.CodingException e) {
      LOG.log(Level.SEVERE, "Failed to decode framework message.", e);
    }
  }
}
