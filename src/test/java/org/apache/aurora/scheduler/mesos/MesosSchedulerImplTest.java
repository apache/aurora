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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.HostAttributes;
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
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.OfferID;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.Protos.TaskState;
import org.apache.mesos.v1.Protos.TaskStatus;
import org.apache.mesos.v1.Protos.TaskStatus.Reason;
import org.apache.mesos.v1.Protos.TaskStatus.Source;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MesosSchedulerImplTest extends EasyMockTest {

  private static final String FRAMEWORK_ID = "framework-id";
  private static final FrameworkID FRAMEWORK =
      FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build();

  private static final String SLAVE_HOST = "slave-hostname";
  private static final Protos.SlaveID SLAVE_ID =
      Protos.SlaveID.newBuilder().setValue("slave-id").build();
  private static final String SLAVE_HOST_2 = "slave-hostname-2";
  private static final Protos.SlaveID SLAVE_ID_2 =
      Protos.SlaveID.newBuilder().setValue("slave-id-2").build();
  private static final Protos.ExecutorID EXECUTOR_ID =
      Protos.ExecutorID.newBuilder().setValue("executor-id").build();

  private static final OfferID OFFER_ID = OfferID.newBuilder().setValue("offer-id").build();
  private static final Protos.Offer OFFER = Protos.Offer.newBuilder()
          .setFrameworkId(ProtosConversion.convert(FRAMEWORK))
          .setSlaveId(SLAVE_ID)
          .setHostname(SLAVE_HOST)
          .setId(ProtosConversion.convert(OFFER_ID))
          .build();
  private static final HostOffer HOST_OFFER = new HostOffer(
      ProtosConversion.convert(OFFER),
      IHostAttributes.build(
          new HostAttributes()
              .setHost(SLAVE_HOST)
              .setSlaveId(SLAVE_ID.getValue())
              .setMode(NONE)
              .setAttributes(ImmutableSet.of())));
  private static final OfferID OFFER_ID_2 = OfferID.newBuilder().setValue("offer-id-2").build();
  private static final Protos.Offer OFFER_2 = Protos.Offer.newBuilder(OFFER)
          .setSlaveId(SLAVE_ID_2)
          .setHostname(SLAVE_HOST_2)
          .setId(ProtosConversion.convert(OFFER_ID_2))
          .build();
  private static final HostOffer HOST_OFFER_2 = new HostOffer(
      ProtosConversion.convert(OFFER_2),
      IHostAttributes.build(
          new HostAttributes()
              .setHost(SLAVE_HOST_2)
              .setSlaveId(SLAVE_ID_2.getValue())
              .setMode(NONE)
              .setAttributes(ImmutableSet.of())));

  private static final TaskStatus STATUS_NO_REASON = TaskStatus.newBuilder()
      .setState(TaskState.TASK_RUNNING)
      .setSource(Source.SOURCE_AGENT)
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

  private StorageTestUtil storageUtil;
  private Command shutdownCommand;
  private TaskStatusHandler statusHandler;
  private OfferManager offerManager;
  private SchedulerDriver driver;
  private EventSink eventSink;
  private FakeStatsProvider statsProvider;

  private MesosSchedulerImpl scheduler;

  @Before
  public void setUp() {
    Logger log = LoggerFactory.getLogger("");
    initializeScheduler(log);
  }

  private void initializeScheduler(Logger logger) {
    storageUtil = new StorageTestUtil(this);
    shutdownCommand = createMock(Command.class);
    statusHandler = createMock(TaskStatusHandler.class);
    offerManager = createMock(OfferManager.class);
    eventSink = createMock(EventSink.class);
    statsProvider = new FakeStatsProvider();

    scheduler = new MesosSchedulerImpl(
        storageUtil.storage,
        new Lifecycle(shutdownCommand),
        statusHandler,
        offerManager,
        eventSink,
        MoreExecutors.sameThreadExecutor(),
        new CachedCounters(new FakeStatsProvider()),
        logger,
        statsProvider);
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
    new AbstractRegisteredTest() {
      @Override
      void test() {
        scheduler.resourceOffers(driver, ImmutableList.of());
      }
    }.run();
  }

  @Test
  public void testAcceptOffer() {
    new AbstractOfferTest() {
      @Override
      void respondToOffer() {
        expectOfferAttributesSaved(HOST_OFFER);
        offerManager.addOffer(HOST_OFFER);
      }
    }.run();
  }

  @Test
  public void testAcceptOfferDebugLogging() {
    Logger mockLogger = createMock(Logger.class);
    mockLogger.info(anyString());
    mockLogger.debug(anyString(), EasyMock.<Object>anyObject());
    initializeScheduler(mockLogger);

    new AbstractOfferTest() {
      @Override
      void respondToOffer() {
        expectOfferAttributesSaved(HOST_OFFER);
        offerManager.addOffer(HOST_OFFER);
      }
    }.run();
  }

  @Test
  public void testAttributesModePreserved() {
    new AbstractOfferTest() {
      @Override
      void respondToOffer() {
        IHostAttributes draining =
            IHostAttributes.build(HOST_OFFER.getAttributes().newBuilder().setMode(DRAINING));
        expect(storageUtil.attributeStore.getHostAttributes(HOST_OFFER.getOffer().getHostname()))
            .andReturn(Optional.of(draining));
        IHostAttributes saved = IHostAttributes.build(
            Conversions.getAttributes(HOST_OFFER.getOffer()).newBuilder().setMode(DRAINING));
        expect(storageUtil.attributeStore.saveHostAttributes(saved)).andReturn(true);

        HostOffer offer = new HostOffer(HOST_OFFER.getOffer(), draining);
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
    new AbstractStatusTest() {
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
    new AbstractRegisteredTest() {
      @Override
      void expectations() {
        expectOfferAttributesSaved(HOST_OFFER);
        expectOfferAttributesSaved(HOST_OFFER_2);
        offerManager.addOffer(HOST_OFFER);
        offerManager.addOffer(HOST_OFFER_2);
      }

      @Override
      void test() {
        scheduler.resourceOffers(driver,
            ImmutableList.of(
                ProtosConversion.convert(HOST_OFFER.getOffer()),
                ProtosConversion.convert(HOST_OFFER_2.getOffer())));
      }
    }.run();
  }

  @Test
  public void testDisconnected() {
    new AbstractRegisteredTest() {
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
    assertEquals(1L, statsProvider.getLongValue("slaves_lost"));
  }

  @Test
  public void testReregistered() {
    control.replay();

    scheduler.reregistered(driver, Protos.MasterInfo.getDefaultInstance());
  }

  @Test
  public void testOfferRescinded() {
    offerManager.cancelOffer(OFFER_ID);

    control.replay();

    scheduler.offerRescinded(driver, ProtosConversion.convert(OFFER_ID));
    assertEquals(1L, statsProvider.getLongValue("offers_rescinded"));
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
  public void testStatusReconciliationAcceptsDebugLogging() {
    Logger mockLogger = createMock(Logger.class);
    mockLogger.info(anyString());
    mockLogger.debug(anyString());
    initializeScheduler(mockLogger);

    new AbstractStatusReconciliationTest() {
      @Override
      void expectations() {
        eventSink.post(PUBSUB_RECONCILIATION_EVENT);
        statusHandler.statusUpdate(status);
      }
    }.run();
  }

  private class StatusUpdater extends AbstractStatusTest {
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
        Conversions.getAttributes(offer.getOffer()).newBuilder().setMode(NONE));
    expect(storageUtil.attributeStore.saveHostAttributes(defaultMode)).andReturn(true);
  }

  private abstract class AbstractRegisteredTest {
    private final AtomicBoolean runCalled = new AtomicBoolean(false);

    AbstractRegisteredTest() {
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

      scheduler.registered(
          driver,
          ProtosConversion.convert(FRAMEWORK),
          Protos.MasterInfo.getDefaultInstance());
      test();
    }

    void expectations() {
      // Default no-op, subclasses may override.
    }

    abstract void test();
  }

  private abstract class AbstractOfferTest extends AbstractRegisteredTest {
    AbstractOfferTest() {
      super();
    }

    abstract void respondToOffer();

    @Override
    void expectations() {
      respondToOffer();
    }

    @Override
    void test() {
      scheduler.resourceOffers(
          driver,
          ImmutableList.of(ProtosConversion.convert(HOST_OFFER.getOffer())));
    }
  }

  private abstract class AbstractStatusTest extends AbstractRegisteredTest {
    protected final TaskStatus status;

    AbstractStatusTest() {
      this(STATUS);
    }

    AbstractStatusTest(TaskStatus status) {
      super();
      this.status = status;
    }

    @Override
    void test() {
      scheduler.statusUpdate(driver, ProtosConversion.convert(status));
    }
  }

  private abstract class AbstractStatusReconciliationTest extends AbstractStatusTest {
    AbstractStatusReconciliationTest() {
      super(STATUS_RECONCILIATION);
    }
  }
}
