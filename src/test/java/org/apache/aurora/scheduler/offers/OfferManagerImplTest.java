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

import java.util.List;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.async.DelayExecutor;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverDisconnected;
import org.apache.aurora.scheduler.events.PubsubEvent.HostAttributesChanged;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.offers.OfferManager.OfferManagerImpl;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.testing.FakeScheduledExecutor;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.Filters;
import org.apache.mesos.v1.Protos.Offer.Operation;
import org.apache.mesos.v1.Protos.TaskInfo;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.scheduler.base.TaskTestUtil.JOB;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.apache.aurora.scheduler.offers.OfferManager.OfferManagerImpl.OFFER_ACCEPT_RACES;
import static org.apache.aurora.scheduler.offers.OfferManager.OfferManagerImpl.OUTSTANDING_OFFERS;
import static org.apache.aurora.scheduler.offers.OfferManager.OfferManagerImpl.STATICALLY_BANNED_OFFERS;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosRange;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.offer;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
  private static final int PORT = 1000;
  private static final Protos.Offer MESOS_OFFER = offer(mesosRange(PORTS, PORT));
  private static final IScheduledTask TASK = makeTask("id", JOB);
  private static final TaskGroupKey GROUP_KEY = TaskGroupKey.from(TASK.getAssignedTask().getTask());
  private static final TaskInfo TASK_INFO = TaskInfo.newBuilder()
      .setName("taskName")
      .setTaskId(Protos.TaskID.newBuilder().setValue(Tasks.id(TASK)))
      .setAgentId(MESOS_OFFER.getAgentId())
      .build();
  private static Operation launch = Operation.newBuilder()
      .setType(Operation.Type.LAUNCH)
      .setLaunch(Operation.Launch.newBuilder().addTaskInfos(TASK_INFO))
      .build();
  private static final List<Operation> OPERATIONS = ImmutableList.of(launch);
  private static final long OFFER_FILTER_SECONDS = 0L;
  private static final Filters OFFER_FILTER = Filters.newBuilder()
      .setRefuseSeconds(OFFER_FILTER_SECONDS)
      .build();

  private Driver driver;
  private FakeScheduledExecutor clock;
  private OfferManagerImpl offerManager;
  private FakeStatsProvider statsProvider;

  @Before
  public void setUp() {
    driver = createMock(Driver.class);
    DelayExecutor executorMock = createMock(DelayExecutor.class);
    clock = FakeScheduledExecutor.fromDelayExecutor(executorMock);
    addTearDown(clock::assertEmpty);
    OfferSettings offerSettings = new OfferSettings(
        Amount.of(OFFER_FILTER_SECONDS, Time.SECONDS),
        () -> RETURN_DELAY);
    statsProvider = new FakeStatsProvider();
    offerManager = new OfferManagerImpl(driver, offerSettings, statsProvider, executorMock);
  }

  @Test
  public void testOffersSorted() throws Exception {
    // Ensures that non-DRAINING offers are preferred - the DRAINING offer would be tried last.

    HostOffer offerA = setMode(OFFER_A, DRAINING);
    HostOffer offerC = setMode(OFFER_C, DRAINING);

    driver.acceptOffers(OFFER_B.getOffer().getId(), OPERATIONS, OFFER_FILTER);
    expectLastCall();

    driver.declineOffer(OFFER_A_ID, OFFER_FILTER);
    expectLastCall();
    driver.declineOffer(offerC.getOffer().getId(), OFFER_FILTER);
    expectLastCall();

    control.replay();

    offerManager.addOffer(offerA);
    assertEquals(1L, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    offerManager.addOffer(OFFER_B);
    assertEquals(2L, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    offerManager.addOffer(offerC);
    assertEquals(3L, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    assertEquals(
        ImmutableSet.of(OFFER_B, offerA, offerC),
        ImmutableSet.copyOf(offerManager.getOffers()));
    offerManager.launchTask(OFFER_B.getOffer().getId(), TASK_INFO);
    assertEquals(2L, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    clock.advance(RETURN_DELAY);
    assertEquals(0L, statsProvider.getLongValue(OUTSTANDING_OFFERS));
  }

  @Test
  public void hostAttributeChangeUpdatesOfferSorting() throws Exception {
    driver.declineOffer(OFFER_A_ID, OFFER_FILTER);
    expectLastCall();
    driver.declineOffer(OFFER_B.getOffer().getId(), OFFER_FILTER);
    expectLastCall();

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
    driver.declineOffer(OFFER_A_ID, OFFER_FILTER);
    expectLastCall().times(2);

    control.replay();

    offerManager.addOffer(OFFER_A);
    assertEquals(1L, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    offerManager.addOffer(OFFER_A);
    assertEquals(0L, statsProvider.getLongValue(OUTSTANDING_OFFERS));

    clock.advance(RETURN_DELAY);
  }

  @Test
  public void testGetOffersReturnsAllOffers() throws Exception {
    control.replay();

    offerManager.addOffer(OFFER_A);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers()));
    assertEquals(1L, statsProvider.getLongValue(OUTSTANDING_OFFERS));

    offerManager.cancelOffer(OFFER_A_ID);
    assertTrue(Iterables.isEmpty(offerManager.getOffers()));
    assertEquals(0L, statsProvider.getLongValue(OUTSTANDING_OFFERS));

    clock.advance(RETURN_DELAY);
  }

  @Test
  public void testOfferFilteringDueToStaticBan() throws Exception {
    driver.declineOffer(OFFER_A_ID, OFFER_FILTER);
    expectLastCall();

    control.replay();

    // Static ban ignored when now offers.
    offerManager.banOffer(OFFER_A_ID, GROUP_KEY);
    assertEquals(0L, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
    offerManager.addOffer(OFFER_A);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers(GROUP_KEY)));
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers()));

    // Add static ban.
    offerManager.banOffer(OFFER_A_ID, GROUP_KEY);
    assertEquals(1L, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers()));
    assertTrue(Iterables.isEmpty(offerManager.getOffers(GROUP_KEY)));

    clock.advance(RETURN_DELAY);
    assertEquals(0L, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
  }

  @Test
  public void testStaticBanIsClearedOnOfferReturn() throws Exception {
    driver.declineOffer(OFFER_A_ID, OFFER_FILTER);
    expectLastCall().times(2);

    control.replay();

    offerManager.addOffer(OFFER_A);
    offerManager.banOffer(OFFER_A_ID, GROUP_KEY);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers()));
    assertTrue(Iterables.isEmpty(offerManager.getOffers(GROUP_KEY)));
    assertEquals(1L, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));

    // Make sure the static ban is cleared when the offers are returned.
    clock.advance(RETURN_DELAY);
    offerManager.addOffer(OFFER_A);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers(GROUP_KEY)));
    assertEquals(0L, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));

    clock.advance(RETURN_DELAY);
  }

  @Test
  public void testStaticBanIsClearedOnDriverDisconnect() throws Exception {
    driver.declineOffer(OFFER_A_ID, OFFER_FILTER);
    expectLastCall();

    control.replay();

    offerManager.addOffer(OFFER_A);
    offerManager.banOffer(OFFER_A_ID, GROUP_KEY);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers()));
    assertTrue(Iterables.isEmpty(offerManager.getOffers(GROUP_KEY)));
    assertEquals(1L, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));

    // Make sure the static ban is cleared when driver is disconnected.
    offerManager.driverDisconnected(new DriverDisconnected());
    assertEquals(0L, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
    offerManager.addOffer(OFFER_A);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers(GROUP_KEY)));

    clock.advance(RETURN_DELAY);
  }

  @Test
  public void getOffer() {
    driver.declineOffer(OFFER_A_ID, OFFER_FILTER);
    expectLastCall();

    control.replay();

    offerManager.addOffer(OFFER_A);
    assertEquals(Optional.of(OFFER_A), offerManager.getOffer(OFFER_A.getOffer().getAgentId()));
    assertEquals(1L, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    clock.advance(RETURN_DELAY);
  }

  @Test(expected = OfferManager.LaunchException.class)
  public void testAcceptOffersDriverThrows() throws OfferManager.LaunchException {
    driver.acceptOffers(OFFER_A_ID, OPERATIONS, OFFER_FILTER);
    expectLastCall().andThrow(new IllegalStateException());

    control.replay();

    offerManager.addOffer(OFFER_A);

    try {
      offerManager.launchTask(OFFER_A_ID, TASK_INFO);
    } finally {
      clock.advance(RETURN_DELAY);
    }
  }

  @Test
  public void testLaunchTaskOfferRaceThrows() {
    control.replay();
    try {
      offerManager.launchTask(OFFER_A_ID, TASK_INFO);
      fail("Method invocation is expected to throw exception.");
    } catch (OfferManager.LaunchException e) {
      assertEquals(1L, statsProvider.getLongValue(OFFER_ACCEPT_RACES));
    }
  }

  @Test
  public void testFlushOffers() throws Exception {
    control.replay();

    offerManager.addOffer(OFFER_A);
    offerManager.addOffer(OFFER_B);
    assertEquals(2L, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    offerManager.driverDisconnected(new DriverDisconnected());
    assertEquals(0L, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    clock.advance(RETURN_DELAY);
  }

  @Test
  public void testDeclineOffer() throws Exception {
    driver.declineOffer(OFFER_A.getOffer().getId(), OFFER_FILTER);
    expectLastCall();

    control.replay();

    offerManager.addOffer(OFFER_A);
    assertEquals(1L, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    clock.advance(RETURN_DELAY);
    assertEquals(0L, statsProvider.getLongValue(OUTSTANDING_OFFERS));
  }

  private static HostOffer setMode(HostOffer offer, MaintenanceMode mode) {
    return new HostOffer(
        offer.getOffer(),
        IHostAttributes.build(offer.getAttributes().newBuilder().setMode(mode)));
  }
}
