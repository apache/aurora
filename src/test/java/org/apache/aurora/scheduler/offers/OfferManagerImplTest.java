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
package org.apache.aurora.scheduler.offers;

import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.testing.TearDown;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverDisconnected;
import org.apache.aurora.scheduler.events.PubsubEvent.HostAttributesChanged;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.offers.OfferManager.OfferManagerImpl;
import org.apache.aurora.scheduler.offers.OfferManager.OfferReturnDelay;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.testing.FakeScheduledExecutor;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskInfo;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OfferManagerImplTest extends EasyMockTest {

  private static final Amount<Long, Time> RETURN_DELAY = Amount.of(1L, Time.DAYS);
  private static final String HOST_A = "HOST_A";
  private static final IHostAttributes HOST_ATTRIBUTES_A =
      IHostAttributes.build(new HostAttributes().setMode(NONE).setHost(HOST_A));
  private static final HostOffer OFFER_A = new HostOffer(
      Offers.makeOffer("OFFER_A", HOST_A),
      HOST_ATTRIBUTES_A);
  private static final Protos.OfferID OFFER_A_ID = OFFER_A.getOffer().getId();
  private static final String HOST_B = "HOST_B";
  private static final HostOffer OFFER_B = new HostOffer(
      Offers.makeOffer("OFFER_B", HOST_B),
      IHostAttributes.build(new HostAttributes().setMode(NONE)));
  private static final String HOST_C = "HOST_C";
  private static final HostOffer OFFER_C = new HostOffer(
      Offers.makeOffer("OFFER_C", HOST_C),
      IHostAttributes.build(new HostAttributes().setMode(NONE)));
  private static final TaskGroupKey GROUP_KEY = TaskGroupKey.from(
      ITaskConfig.build(new TaskConfig().setJob(new JobKey("role", "env", "name"))));
  private static final TaskInfo TASK_INFO = TaskInfo.getDefaultInstance();

  private Driver driver;
  private FakeScheduledExecutor clock;
  private OfferManagerImpl offerManager;

  @Before
  public void setUp() {
    offerManager.LOG.setLevel(Level.FINE);
    addTearDown(new TearDown() {
      @Override
      public void tearDown() throws Exception {
        offerManager.LOG.setLevel(Level.INFO);
      }
    });
    driver = createMock(Driver.class);
    ScheduledExecutorService executorMock = createMock(ScheduledExecutorService.class);
    clock = FakeScheduledExecutor.scheduleExecutor(executorMock);

    addTearDown(new TearDown() {
      @Override
      public void tearDown() throws Exception {
        clock.assertEmpty();
      }
    });
    OfferReturnDelay returnDelay = new OfferReturnDelay() {
      @Override
      public Amount<Long, Time> get() {
        return RETURN_DELAY;
      }
    };
    offerManager = new OfferManagerImpl(driver, returnDelay, executorMock);
  }

  @Test
  public void testOffersSorted() throws Exception {
    // Ensures that non-DRAINING offers are preferred - the DRAINING offer would be tried last.

    HostOffer offerA = setMode(OFFER_A, DRAINING);
    HostOffer offerC = setMode(OFFER_C, DRAINING);

    driver.launchTask(OFFER_B.getOffer().getId(), TASK_INFO);

    driver.declineOffer(OFFER_A_ID);
    driver.declineOffer(offerC.getOffer().getId());

    control.replay();

    offerManager.addOffer(offerA);
    offerManager.addOffer(OFFER_B);
    offerManager.addOffer(offerC);
    assertEquals(
        ImmutableSet.of(OFFER_B, offerA, offerC),
        ImmutableSet.copyOf(offerManager.getOffers()));
    offerManager.launchTask(OFFER_B.getOffer().getId(), TASK_INFO);
    clock.advance(RETURN_DELAY);
  }

  @Test
  public void hostAttributeChangeUpdatesOfferSorting() throws Exception {
    driver.declineOffer(OFFER_A_ID);
    driver.declineOffer(OFFER_B.getOffer().getId());

    control.replay();

    offerManager.hostAttributesChanged(new HostAttributesChanged(HOST_ATTRIBUTES_A));

    offerManager.addOffer(OFFER_A);
    offerManager.addOffer(OFFER_B);
    assertEquals(ImmutableSet.of(OFFER_A, OFFER_B), ImmutableSet.copyOf(offerManager.getOffers()));

    HostOffer offerA = setMode(OFFER_A, DRAINING);
    offerManager.hostAttributesChanged(new HostAttributesChanged(offerA.getAttributes()));
    assertEquals(ImmutableSet.of(OFFER_B, offerA), ImmutableSet.copyOf(offerManager.getOffers()));

    offerA = setMode(OFFER_A, NONE);
    HostOffer offerB = setMode(OFFER_B, DRAINING);
    offerManager.hostAttributesChanged(new HostAttributesChanged(offerA.getAttributes()));
    offerManager.hostAttributesChanged(new HostAttributesChanged(offerB.getAttributes()));
    assertEquals(ImmutableSet.of(OFFER_A, OFFER_B), ImmutableSet.copyOf(offerManager.getOffers()));

    clock.advance(RETURN_DELAY);
  }

  @Test
  public void testAddSameSlaveOffer() {
    driver.declineOffer(OFFER_A_ID);
    expectLastCall().times(2);

    control.replay();

    offerManager.addOffer(OFFER_A);
    offerManager.addOffer(OFFER_A);

    clock.advance(RETURN_DELAY);
  }

  @Test
  public void testGetOffersReturnsAllOffers() throws Exception {
    control.replay();

    offerManager.addOffer(OFFER_A);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers()));

    offerManager.cancelOffer(OFFER_A_ID);
    assertTrue(Iterables.isEmpty(offerManager.getOffers()));

    clock.advance(RETURN_DELAY);
  }

  @Test
  public void testOfferFilteringDueToStaticBan() throws Exception {
    driver.declineOffer(OFFER_A_ID);

    control.replay();

    // Static ban ignored when now offers.
    offerManager.banOffer(OFFER_A_ID, GROUP_KEY);
    offerManager.addOffer(OFFER_A);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers(GROUP_KEY)));

    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers()));

    // Add static ban.
    offerManager.banOffer(OFFER_A_ID, GROUP_KEY);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers()));
    assertTrue(Iterables.isEmpty(offerManager.getOffers(GROUP_KEY)));

    clock.advance(RETURN_DELAY);
  }

  @Test
  public void testStaticBanIsClearedOnOfferReturn() throws Exception {
    driver.declineOffer(OFFER_A_ID);
    expectLastCall().times(2);

    control.replay();

    offerManager.addOffer(OFFER_A);
    offerManager.banOffer(OFFER_A_ID, GROUP_KEY);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers()));
    assertTrue(Iterables.isEmpty(offerManager.getOffers(GROUP_KEY)));

    // Make sure the static ban is cleared when the offers are returned.
    clock.advance(RETURN_DELAY);
    offerManager.addOffer(OFFER_A);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers(GROUP_KEY)));

    clock.advance(RETURN_DELAY);
  }

  @Test
  public void testStaticBanIsClearedOnDriverDisconnect() throws Exception {
    driver.declineOffer(OFFER_A_ID);

    control.replay();

    offerManager.addOffer(OFFER_A);
    offerManager.banOffer(OFFER_A_ID, GROUP_KEY);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers()));
    assertTrue(Iterables.isEmpty(offerManager.getOffers(GROUP_KEY)));

    // Make sure the static ban is cleared when driver is disconnected.
    offerManager.driverDisconnected(new DriverDisconnected());
    offerManager.addOffer(OFFER_A);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers(GROUP_KEY)));

    clock.advance(RETURN_DELAY);
  }

  @Test
  public void getOffer() {
    driver.declineOffer(OFFER_A_ID);

    control.replay();

    offerManager.addOffer(OFFER_A);
    assertEquals(Optional.of(OFFER_A), offerManager.getOffer(OFFER_A.getOffer().getSlaveId()));
    clock.advance(RETURN_DELAY);
  }

  @Test(expected = OfferManager.LaunchException.class)
  public void testLaunchTaskDriverThrows() throws OfferManager.LaunchException {
    driver.launchTask(OFFER_A_ID, TASK_INFO);
    expectLastCall().andThrow(new IllegalStateException());

    control.replay();

    offerManager.addOffer(OFFER_A);

    try {
      offerManager.launchTask(OFFER_A_ID, TASK_INFO);
    } finally {
      clock.advance(RETURN_DELAY);
    }
  }

  @Test(expected = OfferManager.LaunchException.class)
  public void testLaunchTaskOfferRaceThrows() throws OfferManager.LaunchException {
    control.replay();
    offerManager.launchTask(OFFER_A_ID, TASK_INFO);
  }

  @Test
  public void testFlushOffers() throws Exception {
    control.replay();

    offerManager.addOffer(OFFER_A);
    offerManager.addOffer(OFFER_B);
    offerManager.driverDisconnected(new DriverDisconnected());
    clock.advance(RETURN_DELAY);
  }

  @Test
  public void testDeclineOffer() throws Exception {
    driver.declineOffer(OFFER_A.getOffer().getId());

    control.replay();

    offerManager.addOffer(OFFER_A);
    clock.advance(RETURN_DELAY);
  }

  private static HostOffer setMode(HostOffer offer, MaintenanceMode mode) {
    return new HostOffer(
        offer.getOffer(),
        IHostAttributes.build(offer.getAttributes().newBuilder().setMode(mode)));
  }
}
