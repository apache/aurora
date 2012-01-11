package com.twitter.mesos.executor;

import java.lang.Thread.UncaughtExceptionHandler;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos.ExecutorArgs;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskDescription;
import org.apache.mesos.Protos.TaskID;
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

/**
 * @author Benjamin Mahlerr
 */
public class MesosExecutorImplTest extends EasyMockTest {
  public static final String TASK_ID = "TASK_ID";

  private ExecutorCore executorCore;
  private Driver driver;
  private Lifecycle lifecycle;
  private ShutdownRegistry shutdownRegistry;
  private MesosExecutorImpl mesosExecutor;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    driver = createMock(Driver.class);
    lifecycle = new Lifecycle(
        new ShutdownRegistryImpl(),
        new UncaughtExceptionHandler() {
          @Override public void uncaughtException(Thread t, Throwable e) { }
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
    ExecutorDriver executorDriver = createMock(ExecutorDriver.class);
    ExecutorArgs executorArgs = ExecutorArgs.getDefaultInstance();
    executorCore.setSlaveId(EasyMock.<String>anyObject());
    driver.init(executorDriver, executorArgs);

    Capture<ExceptionalCommand<RuntimeException>> shutdownCommand = createCapture();
    shutdownRegistry.addAction(capture(shutdownCommand));

    // Rejected task assignment.
    TaskDescription task = TaskDescription.newBuilder()
        .setTaskId(TaskID.newBuilder().setValue("TASK_ID"))
        .setName("NAME")
        .setSlaveId(SlaveID.newBuilder().setValue("SLAVE_ID"))
        .build();
    expect(driver.sendStatusUpdate(
        eq(TASK_ID), eq(ScheduleStatus.LOST), eq(Optional.of("Executor shutting down."))))
        .andReturn(0);

    control.replay();

    mesosExecutor.init(executorDriver, executorArgs);
    shutdownCommand.getValue().execute();
    mesosExecutor.launchTask(executorDriver, task);
  }

  @Test
  public void testShutdown() {
    ExecutorDriver executorDriver = createMock(ExecutorDriver.class);
    ExecutorArgs executorArgs = ExecutorArgs.getDefaultInstance();
    executorCore.setSlaveId(EasyMock.<String>anyObject());
    driver.init(executorDriver, executorArgs);
    shutdownRegistry.addAction(EasyMock.<ExceptionalCommand>anyObject());

    Task task = createMock(Task.class);
    expect(executorCore.shutdownCore()).andReturn(ImmutableSet.of(task));
    expect(task.getId()).andReturn(TASK_ID);
    expect(driver.sendStatusUpdate(
        eq(TASK_ID), eq(ScheduleStatus.KILLED), eq(Optional.<String>absent())))
        .andReturn(0);
    driver.stop();
    lifecycle.shutdown();

    // Rejected task assignment.
    TaskDescription taskDescription = TaskDescription.newBuilder()
        .setTaskId(TaskID.newBuilder().setValue(TASK_ID))
        .setName("NAME")
        .setSlaveId(SlaveID.newBuilder().setValue("SLAVE_ID"))
        .build();
    expect(driver.sendStatusUpdate(eq(TASK_ID), eq(ScheduleStatus.LOST),
        eq(Optional.of("Executor shutting down.")))).andReturn(0);

    control.replay();

    mesosExecutor.init(executorDriver, executorArgs);
    mesosExecutor.shutdown(executorDriver);
    mesosExecutor.launchTask(executorDriver, taskDescription);
  }
}
