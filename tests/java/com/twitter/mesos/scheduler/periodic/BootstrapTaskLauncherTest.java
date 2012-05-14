package com.twitter.mesos.scheduler.periodic;

import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.scheduler.MesosTaskFactory;
import com.twitter.mesos.scheduler.PulseMonitor;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BootstrapTaskLauncherTest extends EasyMockTest {

  private static final String HOST = "slave-host";

  private static final Offer OFFER = Offer.newBuilder()
        .setSlaveId(SlaveID.newBuilder().setValue("slave-id"))
        .setHostname(HOST)
        .setFrameworkId(FrameworkID.newBuilder().setValue("framework-id").build())
        .setId(OfferID.newBuilder().setValue("offer-id"))
        .build();

  private static final TaskStatus BOOTSTRAP_STATUS = TaskStatus.newBuilder()
      .setTaskId(TaskID.newBuilder().setValue(BootstrapTaskLauncher.TASK_ID_PREFIX + "foo"))
      .setState(TaskState.TASK_RUNNING)
      .build();

  private static final TaskStatus OTHER_STATUS = TaskStatus.newBuilder()
      .setTaskId(TaskID.newBuilder().setValue("foo"))
      .setState(TaskState.TASK_RUNNING)
      .build();

  private PulseMonitor<String> hostMonitor;
  private MesosTaskFactory taskFactory;
  private BootstrapTaskLauncher bootstrap;

  @Before
  public void setUp() {
    hostMonitor = createMock(new Clazz<PulseMonitor<String>>() { });
    taskFactory = createMock(MesosTaskFactory.class);
    bootstrap = new BootstrapTaskLauncher(hostMonitor, taskFactory);
  }

  @Test
  public void testLaunchBootstrap() {
    expect(hostMonitor.isAlive(HOST)).andReturn(false);
    expect(taskFactory.createFrom(EasyMock.<AssignedTask>anyObject(), eq(OFFER.getSlaveId())))
        .andReturn(TaskInfo.getDefaultInstance());
    hostMonitor.pulse(HOST);

    control.replay();

    assertTrue(bootstrap.createTask(OFFER).isPresent());
  }

  @Test
  public void testHostAlive() {
    expect(hostMonitor.isAlive(HOST)).andReturn(true);

    control.replay();

    assertFalse(bootstrap.createTask(OFFER).isPresent());
  }

  @Test
  public void testCapturesBootstrapStatusUpdates() {
    control.replay();
    assertTrue(bootstrap.statusUpdate(BOOTSTRAP_STATUS));
  }

  @Test
  public void testIgnoresOtherStatusUpdates() {
    control.replay();
    assertFalse(bootstrap.statusUpdate(OTHER_STATUS));
  }
}
