package com.twitter.mesos.scheduler;

import com.google.common.base.Supplier;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

/**
 * @author John Sirois
 */
public class DriverTest extends EasyMockTest {

  private static final Protos.TaskID TASK_1 = createTaskId("1");
  private static final Protos.TaskID TASK_2 = createTaskId("2");

  private static Protos.TaskID createTaskId(String taskId) {
    return Protos.TaskID.newBuilder().setValue(taskId).build();
  }

  private SchedulerDriver schedulerDriver;
  private Supplier<SchedulerDriver> driverSupplier;
  private Driver driver;

  @Before
  public void setUp() {
    schedulerDriver = createMock(SchedulerDriver.class);
    driverSupplier = createMock(new Clazz<Supplier<SchedulerDriver>>() { });
    driver = new Driver(driverSupplier);
  }

  @Test
  public void testNoopStop() {
    control.replay();

    driver.stop();
  }

  @Test
  public void testMultipleStops() {
    expect(driverSupplier.get()).andReturn(schedulerDriver);
    expect(schedulerDriver.run()).andReturn(Protos.Status.OK);
    expect(schedulerDriver.stop(true)).andReturn(Protos.Status.DRIVER_ABORTED);
    control.replay();

    assertEquals(Protos.Status.OK, driver.run());
    driver.stop();
    driver.stop();
  }

  @Test
  public void testStop() {
    expect(driverSupplier.get()).andReturn(schedulerDriver);
    expect(schedulerDriver.run()).andReturn(Protos.Status.OK);
    expect(schedulerDriver.stop(true)).andReturn(Protos.Status.DRIVER_ABORTED);
    control.replay();

    assertEquals(Protos.Status.OK, driver.run());
    driver.stop();
  }

  @Test
  public void testNormalLifecycle() {
    expect(driverSupplier.get()).andReturn(schedulerDriver);
    expect(schedulerDriver.run()).andReturn(Protos.Status.OK);
    expect(schedulerDriver.killTask(TASK_1)).andReturn(Protos.Status.OK);
    expect(schedulerDriver.killTask(TASK_2)).andReturn(Protos.Status.OK);
    expect(schedulerDriver.stop(true)).andReturn(Protos.Status.DRIVER_ABORTED);
    control.replay();

    assertEquals(Protos.Status.OK, driver.run());
    assertEquals(Protos.Status.OK, driver.killTask(TASK_1));
    assertEquals(Protos.Status.OK, driver.killTask(TASK_2));
    driver.stop();
  }

  @Test(expected = IllegalStateException.class)
  public void testMustRunBeforeKill() {
    control.replay();

    driver.killTask(TASK_1);
  }

  @Test(expected = IllegalStateException.class)
  public void testOnlyOneRunAllowed() {
    expect(driverSupplier.get()).andReturn(schedulerDriver);
    expect(schedulerDriver.run()).andReturn(Protos.Status.OK);
    control.replay();

    assertEquals(Protos.Status.OK, driver.run());
    driver.run();
  }
}
