package com.twitter.mesos.scheduler;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskDescription;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.SchedulerDriver;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.application.Lifecycle;
import com.twitter.common.base.Command;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.scheduler.MesosSchedulerImpl.SlaveMapper;

import static org.easymock.EasyMock.expect;

/**
 * @author William Farner
 */
public class MesosSchedulerImplTest extends EasyMockTest {

  private static final String FRAMEWORK_ID = "framework-id";
  private static final FrameworkID FRAMEWORK =
      FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build();

  private static final String SLAVE_HOST = "slave-hostname";
  private static final SlaveID SLAVE_ID = SlaveID.newBuilder().setValue("slave-id").build();
  private static final String SLAVE_HOST_2 = "slave-hostname-2";
  private static final SlaveID SLAVE_ID_2 = SlaveID.newBuilder().setValue("slave-id-2").build();

  private static final OfferID OFFER_ID = OfferID.newBuilder().setValue("offer-id").build();
  private static final Offer OFFER = Offer.newBuilder()
      .setFrameworkId(FRAMEWORK)
      .setSlaveId(SLAVE_ID)
      .setHostname(SLAVE_HOST)
      .setId(OFFER_ID)
      .build();
  private static final OfferID OFFER_ID_2 = OfferID.newBuilder().setValue("offer-id-2").build();
  private static final Offer OFFER_2 = Offer.newBuilder(OFFER)
      .setSlaveId(SLAVE_ID_2)
      .setHostname(SLAVE_HOST_2)
      .setId(OFFER_ID_2)
      .build();

  private static final TaskID TASK_ID = TaskID.newBuilder().setValue("task-id").build();
  private static final TaskDescription TASK = TaskDescription.newBuilder()
      .setName("task-name")
      .setSlaveId(SLAVE_ID)
      .setTaskId(TASK_ID)
      .build();

  private static final TaskDescription BIGGER_TASK = TaskDescription.newBuilder()
      .setName("task-name")
      .setSlaveId(SLAVE_ID)
      .addResources(Resources.makeMesosResource(Resources.CPUS, 5))
      .setTaskId(TaskID.newBuilder().setValue("task-id"))
      .build();

  private static final TaskStatus STATUS = TaskStatus.newBuilder()
      .setState(TaskState.TASK_RUNNING)
      .setTaskId(TASK_ID)
      .build();

  private SchedulerCore schedulerCore;
  private TaskLauncher launcher;
  private SlaveMapper slaveMapper;
  private SchedulerDriver driver;

  private MesosSchedulerImpl scheduler;

  @Before
  public void setUp() {
    schedulerCore = createMock(SchedulerCore.class);
    Lifecycle lifecycle =
        new Lifecycle(createMock(Command.class), createMock(UncaughtExceptionHandler.class));
    launcher = createMock(TaskLauncher.class);
    slaveMapper = createMock(SlaveMapper.class);
    scheduler = new MesosSchedulerImpl(
        schedulerCore, lifecycle, Arrays.asList(launcher, schedulerCore), slaveMapper);
    driver = createMock(SchedulerDriver.class);
  }

  @Test(expected = IllegalStateException.class)
  public void testBadOrdering() {
    control.replay();

    // Should fail since the scheduler is not yet registered.
    scheduler.resourceOffers(driver, ImmutableList.<Offer>of());
  }

  @Test
  public void testNoOffers() {
    schedulerCore.registered(FRAMEWORK_ID);

    control.replay();

    scheduler.registered(driver, FRAMEWORK);
    scheduler.resourceOffers(driver, ImmutableList.<Offer>of());
  }

  @Test
  public void testNoAccepts() throws Exception {
    new OfferFixture() {
      @Override List<TaskDescription> respondToOffer() throws Exception {
        expect(launcher.createTask(OFFER)).andReturn(Optional.<TaskDescription>absent());
        expect(schedulerCore.createTask(OFFER)).andReturn(Optional.<TaskDescription>absent());
        return ImmutableList.of();
      }
    };
  }

  @Test
  public void testOfferFirstAccepts() throws Exception {
    new OfferFixture() {
      @Override List<TaskDescription> respondToOffer() throws Exception {
        expect(launcher.createTask(OFFER)).andReturn(Optional.of(TASK));
        return ImmutableList.of(TASK);
      }
    };
  }

  @Test
  public void testOfferSchedulerAccepts() throws Exception {
    new OfferFixture() {
      @Override List<TaskDescription> respondToOffer() throws Exception {
        expect(launcher.createTask(OFFER)).andReturn(Optional.<TaskDescription>absent());
        expect(schedulerCore.createTask(OFFER)).andReturn(Optional.of(TASK));
        return ImmutableList.of(TASK);
      }
    };
  }

  @Test
  public void testAcceptedExceedsOffer() throws Exception {
    new OfferFixture() {
      @Override List<TaskDescription> respondToOffer() throws Exception {
        expect(launcher.createTask(OFFER)).andReturn(Optional.of(BIGGER_TASK));
        expect(schedulerCore.createTask(OFFER)).andReturn(Optional.<TaskDescription>absent());
        return ImmutableList.of();
      }
    };
  }

  @Test
  public void testStatusUpdateNoAccepts() throws Exception {
    new StatusFixture() {
      @Override void expectations() throws Exception {
        expect(launcher.statusUpdate(STATUS)).andReturn(false);
        expect(schedulerCore.statusUpdate(STATUS)).andReturn(false);
      }
    };
  }

  @Test
  public void testStatusUpdateFirstAccepts() throws Exception {
    new StatusFixture() {
      @Override void expectations() throws Exception {
        expect(launcher.statusUpdate(STATUS)).andReturn(true);
      }
    };
  }

  @Test
  public void testStatusUpdateSecondAccepts() throws Exception {
    new StatusFixture() {
      @Override void expectations() throws Exception {
        expect(launcher.statusUpdate(STATUS)).andReturn(false);
        expect(schedulerCore.statusUpdate(STATUS)).andReturn(true);
      }
    };
  }

  @Test
  public void testMultipleOffers() throws Exception {
    new RegisteredFixture() {
      @Override void expectations() throws Exception {
        slaveMapper.addSlave(SLAVE_HOST, SLAVE_ID);
        slaveMapper.addSlave(SLAVE_HOST_2, SLAVE_ID_2);
        expect(launcher.createTask(OFFER)).andReturn(Optional.<TaskDescription>absent());
        expect(schedulerCore.createTask(OFFER)).andReturn(Optional.of(TASK));
        expect(driver.launchTasks(OFFER_ID, ImmutableList.of(TASK))).andReturn(Status.OK);
        expect(launcher.createTask(OFFER_2)).andReturn(Optional.<TaskDescription>absent());
        expect(schedulerCore.createTask(OFFER_2)).andReturn(Optional.<TaskDescription>absent());
        expect(driver.launchTasks(OFFER_ID_2, ImmutableList.<TaskDescription>of()))
            .andReturn(Status.OK);
      }

      @Override void test() {
        scheduler.resourceOffers(driver, ImmutableList.of(OFFER, OFFER_2));
      }
    };
  }

  private abstract class RegisteredFixture {
    RegisteredFixture() throws Exception {
      schedulerCore.registered(FRAMEWORK_ID);
      expectations();

      control.replay();

      scheduler.registered(driver, FRAMEWORK);
      test();
    }

    abstract void expectations() throws Exception;

    abstract void test();
  }

  private abstract class OfferFixture extends RegisteredFixture {
    OfferFixture() throws Exception {
      super();
    }

    abstract List<TaskDescription> respondToOffer() throws Exception;

    @Override void expectations() throws Exception {
      slaveMapper.addSlave(SLAVE_HOST, SLAVE_ID);
      expect(driver.launchTasks(OFFER_ID, respondToOffer())).andReturn(Status.OK);
    }

    @Override void test() {
      scheduler.resourceOffers(driver, ImmutableList.of(OFFER));
    }
  }

  private abstract class StatusFixture extends RegisteredFixture {
    StatusFixture() throws Exception {
      super();
    }

    @Override void test() {
      scheduler.statusUpdate(driver, STATUS);
    }
  }
}
