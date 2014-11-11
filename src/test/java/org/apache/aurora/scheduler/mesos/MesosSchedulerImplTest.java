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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.testing.TearDown;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.twitter.common.application.Lifecycle;
import com.twitter.common.base.Command;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.TaskLauncher;
import org.apache.aurora.scheduler.base.Conversions;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverDisconnected;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverRegistered;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.SchedulerDriver;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.easymock.EasyMock.expect;
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

  private static final TaskStatus STATUS = TaskStatus.newBuilder()
      .setState(TaskState.TASK_RUNNING)
      .setTaskId(TaskID.newBuilder().setValue("task-id").build())
      .build();

  private StorageTestUtil storageUtil;
  private TaskLauncher systemLauncher;
  private TaskLauncher userLauncher;
  private SchedulerDriver driver;
  private EventSink eventSink;

  private MesosSchedulerImpl scheduler;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    final Lifecycle lifecycle =
        new Lifecycle(createMock(Command.class), createMock(UncaughtExceptionHandler.class));
    systemLauncher = createMock(TaskLauncher.class);
    userLauncher = createMock(TaskLauncher.class);
    eventSink = createMock(EventSink.class);

    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(Storage.class).toInstance(storageUtil.storage);
        bind(Lifecycle.class).toInstance(lifecycle);
        bind(new TypeLiteral<List<TaskLauncher>>() { })
            .toInstance(Arrays.asList(systemLauncher, userLauncher));
        bind(EventSink.class).toInstance(eventSink);
        bind(Executor.class).annotatedWith(MesosSchedulerImpl.SchedulerExecutor.class)
            .toInstance(MoreExecutors.sameThreadExecutor());
      }
    });
    scheduler = injector.getInstance(MesosSchedulerImpl.class);
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
    new RegisteredFixture() {
      @Override
      void test() {
        scheduler.resourceOffers(driver, ImmutableList.<Offer>of());
      }
    }.run();
  }

  @Test
  public void testNoAccepts() throws Exception {
    new OfferFixture() {
      @Override
      void respondToOffer() throws Exception {
        expectOfferAttributesSaved(OFFER);
        expect(systemLauncher.willUse(OFFER)).andReturn(false);
        expect(userLauncher.willUse(OFFER)).andReturn(false);
      }
    }.run();
  }

  @Test
  public void testOfferFirstAccepts() throws Exception {
    new OfferFixture() {
      @Override
      void respondToOffer() throws Exception {
        expectOfferAttributesSaved(OFFER);
        expect(systemLauncher.willUse(OFFER)).andReturn(true);
      }
    }.run();
  }

  @Test
  public void testOfferSchedulerAccepts() throws Exception {
    new OfferFixture() {
      @Override
      void respondToOffer() throws Exception {
        expectOfferAttributesSaved(OFFER);
        expect(systemLauncher.willUse(OFFER)).andReturn(false);
        expect(userLauncher.willUse(OFFER)).andReturn(true);
      }
    }.run();
  }

  @Test
  public void testAttributesModePreserved() throws Exception {
    new OfferFixture() {
      @Override
      void respondToOffer() throws Exception {
        IHostAttributes draining = IHostAttributes.build(new HostAttributes().setMode(DRAINING));
        expect(storageUtil.attributeStore.getHostAttributes(OFFER.getHostname()))
            .andReturn(Optional.of(draining));
        IHostAttributes saved = IHostAttributes.build(
            Conversions.getAttributes(OFFER).newBuilder().setMode(DRAINING));
        expect(storageUtil.attributeStore.saveHostAttributes(saved)).andReturn(true);

        expect(systemLauncher.willUse(OFFER)).andReturn(false);
        expect(userLauncher.willUse(OFFER)).andReturn(true);
      }
    }.run();
  }

  @Test
  public void testStatusUpdateNoAccepts() throws Exception {
    new StatusFixture() {
      @Override
      void expectations() throws Exception {
        expect(systemLauncher.statusUpdate(STATUS)).andReturn(false);
        expect(userLauncher.statusUpdate(STATUS)).andReturn(false);
      }
    }.run();
  }

  @Test
  public void testStatusUpdateFirstAccepts() throws Exception {
    new StatusFixture() {
      @Override
      void expectations() throws Exception {
        expect(systemLauncher.statusUpdate(STATUS)).andReturn(true);
      }
    }.run();
  }

  @Test
  public void testStatusUpdateSecondAccepts() throws Exception {
    new StatusFixture() {
      @Override
      void expectations() throws Exception {
        expect(systemLauncher.statusUpdate(STATUS)).andReturn(false);
        expect(userLauncher.statusUpdate(STATUS)).andReturn(true);
      }
    }.run();
  }

  @Test(expected = SchedulerException.class)
  public void testStatusUpdateFails() throws Exception {
    new StatusFixture() {
      @Override
      void expectations() throws Exception {
        expect(systemLauncher.statusUpdate(STATUS)).andReturn(false);
        expect(userLauncher.statusUpdate(STATUS)).andThrow(new StorageException("Injected."));
      }
    }.run();
  }

  @Test
  public void testMultipleOffers() throws Exception {
    new RegisteredFixture() {
      @Override
      void expectations() throws Exception {
        expectOfferAttributesSaved(OFFER);
        expectOfferAttributesSaved(OFFER_2);
        expect(systemLauncher.willUse(OFFER)).andReturn(false);
        expect(userLauncher.willUse(OFFER)).andReturn(true);
        expect(systemLauncher.willUse(OFFER_2)).andReturn(false);
        expect(userLauncher.willUse(OFFER_2)).andReturn(false);
      }

      @Override
      void test() {
        scheduler.resourceOffers(driver, ImmutableList.of(OFFER, OFFER_2));
      }
    }.run();
  }

  @Test
  public void testDisconnected() throws Exception {
    new RegisteredFixture() {
      @Override
      void expectations() throws Exception {
        eventSink.post(new DriverDisconnected());
      }

      @Override
      void test() {
        scheduler.disconnected(driver);
      }
    }.run();
  }

  @Test
  public void testFrameworkMessageIgnored() throws Exception {
    control.replay();

    scheduler.frameworkMessage(
        driver,
        EXECUTOR_ID,
        SLAVE_ID,
        "hello".getBytes(StandardCharsets.UTF_8));
  }

  private void expectOfferAttributesSaved(Offer offer) {
    expect(storageUtil.attributeStore.getHostAttributes(offer.getHostname()))
        .andReturn(Optional.<IHostAttributes>absent());
    IHostAttributes defaultMode = IHostAttributes.build(
        Conversions.getAttributes(offer).newBuilder().setMode(MaintenanceMode.NONE));
    expect(storageUtil.attributeStore.saveHostAttributes(defaultMode)).andReturn(true);
  }

  private abstract class RegisteredFixture {
    private final AtomicBoolean runCalled = new AtomicBoolean(false);

    RegisteredFixture() throws Exception {
      // Prevent otherwise silent noop tests that forget to call run().
      addTearDown(new TearDown() {
        @Override
        public void tearDown() {
          assertTrue(runCalled.get());
        }
      });
    }

    void run() throws Exception {
      runCalled.set(true);
      eventSink.post(new DriverRegistered());
      storageUtil.expectOperations();
      storageUtil.schedulerStore.saveFrameworkId(FRAMEWORK_ID);
      expectations();

      control.replay();

      scheduler.registered(driver, FRAMEWORK, MasterInfo.getDefaultInstance());
      test();
    }

    void expectations() throws Exception {
      // Default no-op, subclasses may override.
    }

    abstract void test();
  }

  private abstract class OfferFixture extends RegisteredFixture {
    OfferFixture() throws Exception {
      super();
    }

    abstract void respondToOffer() throws Exception;

    @Override
    void expectations() throws Exception {
      respondToOffer();
    }

    @Override
    void test() {
      scheduler.resourceOffers(driver, ImmutableList.of(OFFER));
    }
  }

  private abstract class StatusFixture extends RegisteredFixture {
    StatusFixture() throws Exception {
      super();
    }

    @Override
    void test() {
      scheduler.statusUpdate(driver, STATUS);
    }
  }
}
