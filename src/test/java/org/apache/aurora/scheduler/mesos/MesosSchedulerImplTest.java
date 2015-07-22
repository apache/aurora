/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.mesos;

import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.testing.TearDown;
import com.google.common.util.concurrent.MoreExecutors;
import com.twitter.common.application.Lifecycle;
import com.twitter.common.base.Command;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.TaskStatusHandler;
import org.apache.aurora.scheduler.base.Conversions;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverDisconnected;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverRegistered;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStatusReceived;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.TaskStatus.Reason;
import org.apache.mesos.Protos.TaskStatus.Source;
import org.apache.mesos.SchedulerDriver;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.mesos.Protos.Offer;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertTrue;

public class MesosSchedulerImplTest extends EasyMockTest {

  private static final String FRAMEWORK_ID = "framework-id";
  private static final FrameworkID FRAMEWORK =
      FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build();

  private static final String SLAVE_HOST = "slave-hostname";
  private static final SlaveID SLAVE_ID = SlaveID.newBuilder().setValue("slave-id").build();
  private static final String SLAVE_HOST_2 = "slave-hostname-2";
  private static final SlaveID SLAVE_ID_2 = SlaveID.newBuilder().setValue("slave-id-2").build();
  private static final ExecutorID EXECUTOR_ID =
      ExecutorID.newBuilder().setValue("executor-id").build();

  private static final OfferID OFFER_ID = OfferID.newBuilder().setValue("offer-id").build();
  private static final HostOffer OFFER = new HostOffer(
      Offer.newBuilder()
          .setFrameworkId(FRAMEWORK)
          .setSlaveId(SLAVE_ID)
          .setHostname(SLAVE_HOST)
          .setId(OFFER_ID)
          .build(),
      IHostAttributes.build(
          new HostAttributes()
              .setHost(SLAVE_HOST)
              .setSlaveId(SLAVE_ID.getValue())
              .setMode(NONE)
              .setAttributes(ImmutableSet.of())));
  private static final OfferID OFFER_ID_2 = OfferID.newBuilder().setValue("offer-id-2").build();
  private static final HostOffer OFFER_2 = new HostOffer(
      Offer.newBuilder(OFFER.getOffer())
          .setSlaveId(SLAVE_ID_2)
          .setHostname(SLAVE_HOST_2)
          .setId(OFFER_ID_2)
          .build(),
      IHostAttributes.build(
          new HostAttributes()
              .setHost(SLAVE_HOST_2)
              .setSlaveId(SLAVE_ID_2.getValue())
              .setMode(NONE)
              .setAttributes(ImmutableSet.of())));

  private static final TaskStatus STATUS_NO_REASON = TaskStatus.newBuilder()
      .setState(TaskState.TASK_RUNNING)
      .setSource(Source.SOURCE_SLAVE)
      .setMessage("message")
      .setTimestamp(1D)
      .setTaskId(TaskID.newBuilder().setValue("task-id").build())
      .build();

  private static final TaskStatus STATUS = STATUS_NO_REASON
      .toBuilder()
      // Only testing data plumbing, this field with TASK_RUNNING would not normally happen,
      .setReason(Reason.REASON_COMMAND_EXECUTOR_FAILED)
      .build();

  private static final TaskStatus STATUS_RECONCILIATION = STATUS_NO_REASON
      .toBuilder()
      .setReason(Reason.REASON_RECONCILIATION)
      .build();

  private static final TaskStatusReceived PUBSUB_RECONCILIATION_EVENT = new TaskStatusReceived(
      STATUS_RECONCILIATION.getState(),
      Optional.of(STATUS_RECONCILIATION.getSource()),
      Optional.of(STATUS_RECONCILIATION.getReason()),
      Optional.of(1000000L)
  );

  private Logger log;
  private StorageTestUtil storageUtil;
  private Command shutdownCommand;
  private TaskStatusHandler statusHandler;
  private OfferManager offerManager;
  private SchedulerDriver driver;
  private EventSink eventSink;

  private MesosSchedulerImpl scheduler;

  @Before
  public void setUp() {
    log = Logger.getAnonymousLogger();
    log.setLevel(Level.INFO);
    initializeScheduler(log);
  }

  private void initializeScheduler(Logger logger) {
    storageUtil = new StorageTestUtil(this);
    shutdownCommand = createMock(Command.class);
    final Lifecycle lifecycle =
        new Lifecycle(shutdownCommand, createMock(UncaughtExceptionHandler.class));
    statusHandler = createMock(TaskStatusHandler.class);
    offerManager = createMock(OfferManager.class);
    eventSink = createMock(EventSink.class);

    scheduler = new MesosSchedulerImpl(
        storageUtil.storage,
        lifecycle,
        statusHandler,
        offerManager,
        eventSink,
        MoreExecutors.sameThreadExecutor(),
        logger,
        new CachedCounters(new FakeStatsProvider()));
    driver = createMock(SchedulerDriver.class);
  }

  @Test(expected = IllegalStateException.class)
  public void testBadOrdering() {
    control.replay();

    // Should fail since the scheduler is not yet registered.
    scheduler.resourceOffers(driver, ImmutableList.of());
  }

  @Test
  public void testNoOffers() {
    new RegisteredFixture() {
      @Override
      void test() {
        scheduler.resourceOffers(driver, ImmutableList.of());
      }
    }.run();
  }

  @Test
  public void testAcceptOffer() {
    new OfferFixture() {
      @Override
      void respondToOffer() {
        expectOfferAttributesSaved(OFFER);
        offerManager.addOffer(OFFER);
      }
    }.run();
  }

  @Test
  public void testAcceptOfferFineLogging() {
    Logger mockLogger = createMock(Logger.class);
    mockLogger.info(anyString());
    expect(mockLogger.isLoggable(Level.FINE)).andReturn(true);
    mockLogger.log(eq(Level.FINE), anyString());
    initializeScheduler(mockLogger);

    new OfferFixture() {
      @Override
      void respondToOffer() {
        expectOfferAttributesSaved(OFFER);
        offerManager.addOffer(OFFER);
      }
    }.run();
  }

  @Test
  public void testAttributesModePreserved() {
    new OfferFixture() {
      @Override
      void respondToOffer() {
        IHostAttributes draining =
            IHostAttributes.build(OFFER.getAttributes().newBuilder().setMode(DRAINING));
        expect(storageUtil.attributeStore.getHostAttributes(OFFER.getOffer().getHostname()))
            .andReturn(Optional.of(draining));
        IHostAttributes saved = IHostAttributes.build(
            Conversions.getAttributes(OFFER.getOffer()).newBuilder().setMode(DRAINING));
        expect(storageUtil.attributeStore.saveHostAttributes(saved)).andReturn(true);

        HostOffer offer = new HostOffer(OFFER.getOffer(), draining);
        offerManager.addOffer(offer);
      }
    }.run();
  }

  @Test
  public void testStatusUpdate() {
    // Test multiple variations of fields in TaskStatus to cover all branches.
    new StatusUpdater(STATUS).run();
    control.verify();
    control.reset();
    new StatusUpdater(STATUS.toBuilder().clearSource().build()).run();
    control.verify();
    control.reset();
    new StatusUpdater(STATUS.toBuilder().clearReason().build()).run();
    control.verify();
    control.reset();
    new StatusUpdater(STATUS.toBuilder().clearMessage().build()).run();
  }

  @Test(expected = SchedulerException.class)
  public void testStatusUpdateFails() {
    new StatusFixture() {
      @Override
      void expectations() {
        eventSink.post(new TaskStatusReceived(
            STATUS.getState(),
            Optional.of(STATUS.getSource()),
            Optional.of(STATUS.getReason()),
            Optional.of(1000000L)
        ));
        statusHandler.statusUpdate(status);
        expectLastCall().andThrow(new StorageException("Injected."));
      }
    }.run();
  }

  @Test
  public void testMultipleOffers() {
    new RegisteredFixture() {
      @Override
      void expectations() {
        expectOfferAttributesSaved(OFFER);
        expectOfferAttributesSaved(OFFER_2);
        offerManager.addOffer(OFFER);
        offerManager.addOffer(OFFER_2);
      }

      @Override
      void test() {
        scheduler.resourceOffers(driver, ImmutableList.of(OFFER.getOffer(), OFFER_2.getOffer()));
      }
    }.run();
  }

  @Test
  public void testDisconnected() {
    new RegisteredFixture() {
      @Override
      void expectations() {
        eventSink.post(new DriverDisconnected());
      }

      @Override
      void test() {
        scheduler.disconnected(driver);
      }
    }.run();
  }

  @Test
  public void testFrameworkMessageIgnored() {
    control.replay();

    scheduler.frameworkMessage(
        driver,
        EXECUTOR_ID,
        SLAVE_ID,
        "hello".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testSlaveLost() {
    control.replay();

    scheduler.slaveLost(driver, SLAVE_ID);
  }

  @Test
  public void testReregistered() {
    control.replay();

    scheduler.reregistered(driver, MasterInfo.getDefaultInstance());
  }

  @Test
  public void testOfferRescinded() {
    offerManager.cancelOffer(OFFER_ID);

    control.replay();

    scheduler.offerRescinded(driver, OFFER_ID);
  }

  @Test
  public void testError() {
    shutdownCommand.execute();

    control.replay();

    scheduler.error(driver, "error");
  }

  @Test
  public void testExecutorLost() {
    control.replay();

    scheduler.executorLost(driver, EXECUTOR_ID, SLAVE_ID, 1);
  }

  @Test
  public void testStatusReconciliationAcceptsFineLogging() {
    Logger mockLogger = createMock(Logger.class);
    mockLogger.info(anyString());
    mockLogger.log(eq(Level.FINE), anyString());
    initializeScheduler(mockLogger);

    new StatusReconciliationFixture() {
      @Override
      void expectations() {
        eventSink.post(PUBSUB_RECONCILIATION_EVENT);
        statusHandler.statusUpdate(status);
      }
    }.run();
  }

  private class StatusUpdater extends StatusFixture {
    StatusUpdater(TaskStatus status) {
      super(status);
    }

    @Override
    void expectations() {
      eventSink.post(new TaskStatusReceived(
          status.getState(),
          Optional.fromNullable(status.getSource()),
          status.hasReason() ? Optional.of(status.getReason()) : Optional.absent(),
          Optional.of(1000000L)
      ));
      statusHandler.statusUpdate(status);
    }
  }

  private void expectOfferAttributesSaved(HostOffer offer) {
    expect(storageUtil.attributeStore.getHostAttributes(offer.getOffer().getHostname()))
        .andReturn(Optional.absent());
    IHostAttributes defaultMode = IHostAttributes.build(
        Conversions.getAttributes(offer.getOffer()).newBuilder().setMode(MaintenanceMode.NONE));
    expect(storageUtil.attributeStore.saveHostAttributes(defaultMode)).andReturn(true);
  }

  private abstract class RegisteredFixture {
    private final AtomicBoolean runCalled = new AtomicBoolean(false);

    RegisteredFixture() {
      // Prevent otherwise silent noop tests that forget to call run().
      addTearDown(new TearDown() {
        @Override
        public void tearDown() {
          assertTrue(runCalled.get());
        }
      });
    }

    void run() {
      runCalled.set(true);
      eventSink.post(new DriverRegistered());
      storageUtil.expectOperations();
      storageUtil.schedulerStore.saveFrameworkId(FRAMEWORK_ID);
      expectations();

      control.replay();

      scheduler.registered(driver, FRAMEWORK, MasterInfo.getDefaultInstance());
      test();
    }

    void expectations() {
      // Default no-op, subclasses may override.
    }

    abstract void test();
  }

  private abstract class OfferFixture extends RegisteredFixture {
    OfferFixture() {
      super();
    }

    abstract void respondToOffer();

    @Override
    void expectations() {
      respondToOffer();
    }

    @Override
    void test() {
      scheduler.resourceOffers(driver, ImmutableList.of(OFFER.getOffer()));
    }
  }

  private abstract class StatusFixture extends RegisteredFixture {
    protected final TaskStatus status;

    StatusFixture() {
      this(STATUS);
    }

    StatusFixture(TaskStatus status) {
      super();
      this.status = status;
    }

    @Override
    void test() {
      scheduler.statusUpdate(driver, status);
    }
  }

  private abstract class StatusReconciliationFixture extends StatusFixture {
    StatusReconciliationFixture() {
      super(STATUS_RECONCILIATION);
    }
  }
}
