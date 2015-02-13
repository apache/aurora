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
package org.apache.aurora.scheduler.async;

import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Function;
import com.google.common.testing.TearDown;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.async.OfferQueue.OfferQueueImpl;
import org.apache.aurora.scheduler.async.OfferQueue.OfferReturnDelay;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverDisconnected;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.state.TaskAssigner.Assignment;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.testing.FakeScheduledExecutor;
import org.apache.mesos.Protos.TaskInfo;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OfferQueueImplTest extends EasyMockTest {

  private static final Amount<Long, Time> RETURN_DELAY = Amount.of(1L, Time.DAYS);
  private static final String HOST_A = "HOST_A";
  private static final HostOffer OFFER_A = new HostOffer(
      Offers.makeOffer("OFFER_A", HOST_A),
      IHostAttributes.build(new HostAttributes().setMode(NONE)));
  private static final String HOST_B = "HOST_B";
  private static final HostOffer OFFER_B = new HostOffer(
      Offers.makeOffer("OFFER_B", HOST_B),
      IHostAttributes.build(new HostAttributes().setMode(NONE)));
  private static final String HOST_C = "HOST_C";
  private static final HostOffer OFFER_C = new HostOffer(
      Offers.makeOffer("OFFER_C", HOST_C),
      IHostAttributes.build(new HostAttributes().setMode(NONE)));

  private Driver driver;
  private FakeScheduledExecutor clock;
  private Function<HostOffer, Assignment> offerAcceptor;
  private OfferQueueImpl offerQueue;

  @Before
  public void setUp() {
    driver = createMock(Driver.class);
    ScheduledExecutorService executorMock = createMock(ScheduledExecutorService.class);
    clock = FakeScheduledExecutor.scheduleExecutor(executorMock);

    addTearDown(new TearDown() {
      @Override
      public void tearDown() throws Exception {
        clock.assertEmpty();
      }
    });
    offerAcceptor = createMock(new Clazz<Function<HostOffer, Assignment>>() { });
    OfferReturnDelay returnDelay = new OfferReturnDelay() {
      @Override
      public Amount<Long, Time> get() {
        return RETURN_DELAY;
      }
    };
    offerQueue = new OfferQueueImpl(driver, returnDelay, executorMock);
  }

  @Test
  public void testOffersSorted() throws Exception {
    // Ensures that non-DRAINING offers are preferred - the DRAINING offer would be tried last.

    HostOffer offerA = setMode(OFFER_A, DRAINING);
    HostOffer offerC = setMode(OFFER_C, DRAINING);

    TaskInfo task = TaskInfo.getDefaultInstance();
    expect(offerAcceptor.apply(OFFER_B)).andReturn(Assignment.success(task));
    driver.launchTask(OFFER_B.getOffer().getId(), task);

    driver.declineOffer(offerA.getOffer().getId());
    driver.declineOffer(offerC.getOffer().getId());

    control.replay();

    offerQueue.addOffer(offerA);
    offerQueue.addOffer(OFFER_B);
    offerQueue.addOffer(offerC);
    assertTrue(offerQueue.launchFirst(offerAcceptor));
    clock.advance(RETURN_DELAY);
  }

  @Test
  public void testFlushOffers() throws Exception {
    control.replay();

    offerQueue.addOffer(OFFER_A);
    offerQueue.addOffer(OFFER_B);
    offerQueue.driverDisconnected(new DriverDisconnected());
    assertFalse(offerQueue.launchFirst(offerAcceptor));
    clock.advance(RETURN_DELAY);
  }

  @Test
  public void testDeclineOffer() throws Exception {
    driver.declineOffer(OFFER_A.getOffer().getId());

    control.replay();

    offerQueue.addOffer(OFFER_A);
    clock.advance(RETURN_DELAY);
  }

  private static HostOffer setMode(HostOffer offer, MaintenanceMode mode) {
    return new HostOffer(
        offer.getOffer(),
        IHostAttributes.build(offer.getAttributes().newBuilder().setMode(mode)));
  }
}
