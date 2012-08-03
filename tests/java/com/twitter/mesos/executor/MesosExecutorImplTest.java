package com.twitter.mesos.executor;

import java.lang.Thread.UncaughtExceptionHandler;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.application.Lifecycle;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.application.ShutdownRegistry.ShutdownRegistryImpl;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.gen.ScheduleStatus;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;

public class MesosExecutorImplTest extends EasyMockTest {
  private static final String TASK_ID = "TASK_ID";
  private static final String SLAVE_ID = "SLAVE_ID";

  private static final FrameworkInfo FRAMEWORK_INFO = FrameworkInfo.getDefaultInstance();
  private static final ExecutorInfo EXECUTOR_INFO = ExecutorInfo.getDefaultInstance();
  private static final SlaveInfo SLAVE_INFO = SlaveInfo.newBuilder()
      .setHostname("HOST")
      .setWebuiHostname("WEB_HOST")
      .setId(SlaveID.newBuilder().setValue(SLAVE_ID)).build();

  private ExecutorDriver executorDriver;
  private ExecutorCore executorCore;
  private Driver driver;
  private Lifecycle lifecycle;
  private ShutdownRegistry shutdownRegistry;
  private MesosExecutorImpl mesosExecutor;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    executorDriver = createMock(ExecutorDriver.class);
    driver = createMock(Driver.class);
    lifecycle = new Lifecycle(
        new ShutdownRegistryImpl(),
        new UncaughtExceptionHandler() {
          @Override public void uncaughtException(Thread t, Throwable e) {
            // No-op.
          }
        });
    executorCore = createMock(ExecutorCore.class);
    shutdownRegistry = createMock(ShutdownRegistry.class);

    mesosExecutor = new MesosExecutorImpl(
        executorCore,
        driver,
        lifecycle,
        shutdownRegistry);
  }

  @Test
  public void testLifecycleShutdown() {
    executorCore.setSlaveId(EasyMock.<String>anyObject());
    driver.init(executorDriver, EXECUTOR_INFO);

    Capture<ExceptionalCommand<RuntimeException>> shutdownCommand = createCapture();
    shutdownRegistry.addAction(capture(shutdownCommand));

    // Rejected task assignment.
    TaskInfo task = TaskInfo.newBuilder()
        .setTaskId(TaskID.newBuilder().setValue(TASK_ID))
        .setName("NAME")
        .setSlaveId(SlaveID.newBuilder().setValue(SLAVE_ID))
        .build();
    expect(driver.sendStatusUpdate(
        eq(TASK_ID), eq(ScheduleStatus.LOST), eq(Optional.of("Executor shutting down."))))
        .andReturn(0);

    control.replay();

    mesosExecutor.registered(executorDriver, EXECUTOR_INFO, FRAMEWORK_INFO, SLAVE_INFO);
    shutdownCommand.getValue().execute();
    mesosExecutor.launchTask(executorDriver, task);
  }

  @Test
  public void testShutdown() {
    executorCore.setSlaveId(EasyMock.<String>anyObject());
    driver.init(executorDriver, EXECUTOR_INFO);
    shutdownRegistry.addAction(EasyMock.<ExceptionalCommand<RuntimeException>>anyObject());

    Task task = createMock(Task.class);
    expect(executorCore.shutdownCore()).andReturn(ImmutableSet.of(task));
    expect(task.getId()).andReturn(TASK_ID);
    expect(driver.sendStatusUpdate(
        eq(TASK_ID), eq(ScheduleStatus.KILLED), eq(Optional.<String>absent())))
        .andReturn(0);
    driver.stop();
    lifecycle.shutdown();

    // Rejected task assignment.
    TaskInfo taskDescription = TaskInfo.newBuilder()
        .setTaskId(TaskID.newBuilder().setValue(TASK_ID))
        .setName("NAME")
        .setSlaveId(SlaveID.newBuilder().setValue(SLAVE_ID))
        .build();
    expect(driver.sendStatusUpdate(eq(TASK_ID), eq(ScheduleStatus.LOST),
        eq(Optional.of("Executor shutting down.")))).andReturn(0);

    control.replay();

    mesosExecutor.registered(executorDriver, EXECUTOR_INFO, FRAMEWORK_INFO, SLAVE_INFO);
    mesosExecutor.shutdown(executorDriver);
    mesosExecutor.launchTask(executorDriver, taskDescription);
  }
}
