package com.twitter.mesos.scheduler;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.scheduler.Driver.DriverImpl;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

/**
 * @author John Sirois
 */
public class DriverTest extends EasyMockTest {

  private static final String TASK_1 = "1";
  private static final String TASK_2 = "2";

  private SchedulerDriver schedulerDriver;

  private Supplier<Optional<SchedulerDriver>>  driverSupplier;
  private DriverImpl driver;

  private static Protos.TaskID createTaskId(String taskId) {
    return Protos.TaskID.newBuilder().setValue(taskId).build();
  }

  @Before
  public void setUp() {
    schedulerDriver = createMock(SchedulerDriver.class);
    driverSupplier = createMock(new Clazz<Supplier<Optional<SchedulerDriver>>>() { });
    driver = new DriverImpl(driverSupplier);
  }

  @Test
  public void testNoopStop() {
    control.replay();

    driver.stop();
  }

  @Test
  public void testMultipleStops() {
    expect(driverSupplier.get()).andReturn(Optional.of(schedulerDriver)).times(2);
    expect(schedulerDriver.run()).andReturn(Protos.Status.OK);
    expect(schedulerDriver.stop(true)).andReturn(Protos.Status.DRIVER_ABORTED);
    control.replay();

    assertEquals(Protos.Status.OK, driver.run());
    driver.stop();
    driver.stop();
  }

  @Test
  public void testStop() {
    expect(driverSupplier.get()).andReturn(Optional.of(schedulerDriver)).times(2);
    expect(schedulerDriver.run()).andReturn(Protos.Status.OK);
    expect(schedulerDriver.stop(true)).andReturn(Protos.Status.DRIVER_ABORTED);
    control.replay();

    assertEquals(Protos.Status.OK, driver.run());
    driver.stop();
  }

  @Test
  public void testNormalLifecycle() {
    expect(driverSupplier.get()).andReturn(Optional.of(schedulerDriver)).times(4);
    expect(schedulerDriver.run()).andReturn(Protos.Status.OK);
    expect(schedulerDriver.killTask(createTaskId(TASK_1))).andReturn(Protos.Status.OK);
    expect(schedulerDriver.killTask(createTaskId(TASK_2))).andReturn(Protos.Status.OK);
    expect(schedulerDriver.stop(true)).andReturn(Protos.Status.DRIVER_ABORTED);
    control.replay();

    assertEquals(Protos.Status.OK, driver.run());
    driver.killTask(TASK_1);
    driver.killTask(TASK_2);
    driver.stop();
  }

  @Test(expected = IllegalStateException.class)
  public void testMustRunBeforeKill() {
    control.replay();

    driver.killTask(TASK_1);
  }

  @Test(expected = IllegalStateException.class)
  public void testOnlyOneRunAllowed() {
    expect(driverSupplier.get()).andReturn(Optional.of(schedulerDriver));
    expect(schedulerDriver.run()).andReturn(Protos.Status.OK);
    control.replay();

    assertEquals(Protos.Status.OK, driver.run());
    driver.run();
  }
}
