/**
 * Copyright 2013 Apache Software Foundation
 *
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
package org.apache.aurora.scheduler.async;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.testing.TearDown;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;

import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.Driver;
import org.apache.aurora.scheduler.async.OfferQueue.LaunchException;
import org.apache.aurora.scheduler.async.OfferQueue.OfferQueueImpl;
import org.apache.aurora.scheduler.async.OfferQueue.OfferReturnDelay;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverDisconnected;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskInfo;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertFalse;

public class OfferQueueImplTest extends EasyMockTest {

  private static final Amount<Integer, Time> RETURN_DELAY = Amount.of(1, Time.DAYS);
  private static final String HOST_A = "HOST_A";
  private static final Offer OFFER_A = Offers.makeOffer("OFFER_A", HOST_A);
  private static final String HOST_B = "HOST_B";
  private static final Offer OFFER_B = Offers.makeOffer("OFFER_B", HOST_B);
  private static final String HOST_C = "HOST_C";
  private static final Offer OFFER_C = Offers.makeOffer("OFFER_C", HOST_C);

  private Driver driver;
  private ScheduledExecutorService executor;
  private ExecutorService testExecutor;
  private MaintenanceController maintenanceController;
  private Function<Offer, Optional<TaskInfo>> offerAcceptor;
  private OfferQueueImpl offerQueue;

  @Before
  public void setUp() {
    driver = createMock(Driver.class);
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).build();
    executor = Executors.newSingleThreadScheduledExecutor(threadFactory);
    testExecutor = Executors.newCachedThreadPool(threadFactory);
    addTearDown(new TearDown() {
      @Override
      public void tearDown() throws Exception {
        new ExecutorServiceShutdown(executor, Amount.of(1L, Time.SECONDS)).execute();
        new ExecutorServiceShutdown(testExecutor, Amount.of(1L, Time.SECONDS)).execute();
      }
    });
    maintenanceController = createMock(MaintenanceController.class);
    offerAcceptor = createMock(new Clazz<Function<Offer, Optional<TaskInfo>>>() { });
    OfferReturnDelay returnDelay = new OfferReturnDelay() {
      @Override
      public Amount<Integer, Time> get() {
        return RETURN_DELAY;
      }
    };
    offerQueue = new OfferQueueImpl(driver, returnDelay, executor, maintenanceController);
  }

  @Test
  public void testNoDeadlock() throws Exception {
    // Test that a blocked call to maintenanceController does not result in a deadlock between
    // the intrinsic lock and the storage lock.
    final CountDownLatch launchAttempted = new CountDownLatch(1);
    expect(maintenanceController.getMode(HOST_A)).andAnswer(new IAnswer<MaintenanceMode>() {
      @Override
      public MaintenanceMode answer() throws InterruptedException {
        launchAttempted.await();
        return MaintenanceMode.NONE;
      }
    });

    control.replay();

    final CountDownLatch offerAdded = new CountDownLatch(1);
    testExecutor.submit(new Runnable() {
      @Override
      public void run() {
        offerQueue.addOffer(OFFER_A);
        offerAdded.countDown();
      }
    });
    testExecutor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          offerQueue.launchFirst(offerAcceptor);
          launchAttempted.countDown();
        } catch (LaunchException e) {
          throw Throwables.propagate(e);
        }
      }
    });

    launchAttempted.await();
    offerAdded.await();
  }

  @Test
  public void testOffersSorted() throws Exception {
    expect(maintenanceController.getMode(HOST_A)).andReturn(MaintenanceMode.NONE);
    expect(maintenanceController.getMode(HOST_B)).andReturn(MaintenanceMode.DRAINING);
    expect(maintenanceController.getMode(HOST_C)).andReturn(MaintenanceMode.NONE);
    expect(offerAcceptor.apply(OFFER_A)).andReturn(Optional.<TaskInfo>absent());
    expect(offerAcceptor.apply(OFFER_C)).andReturn(Optional.<TaskInfo>absent());
    expect(offerAcceptor.apply(OFFER_B)).andReturn(Optional.<TaskInfo>absent());

    control.replay();

    offerQueue.addOffer(OFFER_A);
    offerQueue.addOffer(OFFER_B);
    offerQueue.addOffer(OFFER_C);
    assertFalse(offerQueue.launchFirst(offerAcceptor));
  }

  @Test
  public void testFlushOffers() throws Exception {
    expect(maintenanceController.getMode(HOST_A)).andReturn(MaintenanceMode.NONE);
    expect(maintenanceController.getMode(HOST_B)).andReturn(MaintenanceMode.NONE);

    control.replay();

    offerQueue.addOffer(OFFER_A);
    offerQueue.addOffer(OFFER_B);
    offerQueue.driverDisconnected(new DriverDisconnected());
    assertFalse(offerQueue.launchFirst(offerAcceptor));
  }
}
