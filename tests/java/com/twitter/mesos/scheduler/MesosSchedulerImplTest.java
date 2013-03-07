package com.twitter.mesos.scheduler;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Arrays;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.SchedulerDriver;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.application.Lifecycle;
import com.twitter.common.base.Command;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.scheduler.storage.Storage.StorageException;
import com.twitter.mesos.scheduler.storage.testing.StorageTestUtil;

import static org.apache.mesos.Protos.Status.DRIVER_RUNNING;
import static org.easymock.EasyMock.expect;

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
  private static final TaskInfo TASK = TaskInfo.newBuilder()
      .setName("task-name")
      .setSlaveId(SLAVE_ID)
      .setTaskId(TASK_ID)
      .build();

  private static final TaskInfo BIGGER_TASK = TaskInfo.newBuilder()
      .setName("task-name")
      .setSlaveId(SLAVE_ID)
      .addResources(Resources.makeMesosResource(Resources.CPUS, 5))
      .setTaskId(TaskID.newBuilder().setValue("task-id"))
      .build();

  private static final TaskStatus STATUS = TaskStatus.newBuilder()
      .setState(TaskState.TASK_RUNNING)
      .setTaskId(TASK_ID)
      .build();

  private StorageTestUtil storageUtil;
  private TaskLauncher systemLauncher;
  private TaskLauncher userLauncher;
  private RegisteredListener registeredListener;
  private SchedulerDriver driver;

  private MesosSchedulerImpl scheduler;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    Lifecycle lifecycle =
        new Lifecycle(createMock(Command.class), createMock(UncaughtExceptionHandler.class));
    systemLauncher = createMock(TaskLauncher.class);
    userLauncher = createMock(TaskLauncher.class);
    registeredListener = createMock(RegisteredListener.class);
    scheduler = new MesosSchedulerImpl(
        storageUtil.storage,
        createMock(SchedulerCore.class),
        lifecycle,
        Arrays.asList(systemLauncher, userLauncher),
        registeredListener);
    driver = createMock(SchedulerDriver.class);
  }

  @Test(expected = IllegalStateException.class)
  public void testBadOrdering() {
    control.replay();

    // Should fail since the scheduler is not yet registered.
    scheduler.resourceOffers(driver, ImmutableList.<Offer>of());
  }

  @Test
  public void testNoOffers() throws Exception {
    registeredListener.registered(FRAMEWORK_ID);

    control.replay();

    scheduler.registered(driver, FRAMEWORK, MasterInfo.getDefaultInstance());
    scheduler.resourceOffers(driver, ImmutableList.<Offer>of());
  }

  @Test
  public void testNoAccepts() throws Exception {
    new OfferFixture() {
      @Override void respondToOffer() throws Exception {
        expectOfferAttributesSaved(OFFER);
        expect(systemLauncher.createTask(OFFER)).andReturn(Optional.<TaskInfo>absent());
        expect(userLauncher.createTask(OFFER)).andReturn(Optional.<TaskInfo>absent());
      }
    };
  }

  @Test
  public void testOfferFirstAccepts() throws Exception {
    new OfferFixture() {
      @Override void respondToOffer() throws Exception {
        expectOfferAttributesSaved(OFFER);
        expect(systemLauncher.createTask(OFFER)).andReturn(Optional.of(TASK));
        expectLaunch(TASK);
      }
    };
  }

  @Test
  public void testOfferSchedulerAccepts() throws Exception {
    new OfferFixture() {
      @Override void respondToOffer() throws Exception {
        expectOfferAttributesSaved(OFFER);
        expect(systemLauncher.createTask(OFFER)).andReturn(Optional.<TaskInfo>absent());
        expect(userLauncher.createTask(OFFER)).andReturn(Optional.of(TASK));
        expectLaunch(TASK);
      }
    };
  }

  @Test
  public void testAcceptedExceedsOffer() throws Exception {
    new OfferFixture() {
      @Override void respondToOffer() throws Exception {
        expectOfferAttributesSaved(OFFER);
        expect(systemLauncher.createTask(OFFER)).andReturn(Optional.of(BIGGER_TASK));
        expect(userLauncher.createTask(OFFER)).andReturn(Optional.<TaskInfo>absent());
      }
    };
  }

  @Test
  public void testStatusUpdateNoAccepts() throws Exception {
    new StatusFixture() {
      @Override void expectations() throws Exception {
        expect(systemLauncher.statusUpdate(STATUS)).andReturn(false);
        expect(userLauncher.statusUpdate(STATUS)).andReturn(false);
      }
    };
  }

  @Test
  public void testStatusUpdateFirstAccepts() throws Exception {
    new StatusFixture() {
      @Override void expectations() throws Exception {
        expect(systemLauncher.statusUpdate(STATUS)).andReturn(true);
      }
    };
  }

  @Test
  public void testStatusUpdateSecondAccepts() throws Exception {
    new StatusFixture() {
      @Override void expectations() throws Exception {
        expect(systemLauncher.statusUpdate(STATUS)).andReturn(false);
        expect(userLauncher.statusUpdate(STATUS)).andReturn(true);
      }
    };
  }

  @Test(expected = SchedulerException.class)
  public void testStatusUpdateFails() throws Exception {
    new StatusFixture() {
      @Override void expectations() throws Exception {
        expect(systemLauncher.statusUpdate(STATUS)).andReturn(false);
        expect(userLauncher.statusUpdate(STATUS)).andThrow(new StorageException("Injected."));
      }
    };
  }

  @Test
  public void testMultipleOffers() throws Exception {
    new RegisteredFixture() {
      @Override void expectations() throws Exception {
        storageUtil.expectTransactions();
        expectOfferAttributesSaved(OFFER);
        expectOfferAttributesSaved(OFFER_2);
        expect(systemLauncher.createTask(OFFER)).andReturn(Optional.<TaskInfo>absent());
        expect(userLauncher.createTask(OFFER)).andReturn(Optional.of(TASK));
        expectLaunch(TASK);
        expect(systemLauncher.createTask(OFFER_2)).andReturn(Optional.<TaskInfo>absent());
        expect(userLauncher.createTask(OFFER_2)).andReturn(Optional.<TaskInfo>absent());
      }

      @Override void test() {
        scheduler.resourceOffers(driver, ImmutableList.of(OFFER, OFFER_2));
      }
    };
  }

  private void expectOfferAttributesSaved(Offer offer) {
    storageUtil.attributeStore.saveHostAttributes(Conversions.getAttributes(offer));
  }

  private abstract class RegisteredFixture {
    RegisteredFixture() throws Exception {
      registeredListener.registered(FRAMEWORK_ID);
      expectations();

      control.replay();

      scheduler.registered(driver, FRAMEWORK, MasterInfo.getDefaultInstance());
      test();
    }

    protected void expectLaunch(TaskInfo task) {
      expect(driver.launchTasks(OFFER_ID, ImmutableList.of(task))).andReturn(DRIVER_RUNNING);
    }

    abstract void expectations() throws Exception;

    abstract void test();
  }

  private abstract class OfferFixture extends RegisteredFixture {
    OfferFixture() throws Exception {
      super();
    }

    abstract void respondToOffer() throws Exception;

    @Override void expectations() throws Exception {
      storageUtil.expectTransactions();
      respondToOffer();
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
