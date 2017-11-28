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
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.common.collections.Pair;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeTicker;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverDisconnected;
import org.apache.aurora.scheduler.events.PubsubEvent.HostAttributesChanged;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.offers.Deferment.Noop;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.testing.FakeScheduledExecutor;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.Filters;
import org.apache.mesos.v1.Protos.Offer.Operation;
import org.apache.mesos.v1.Protos.OfferID;
import org.apache.mesos.v1.Protos.TaskInfo;
import org.apache.mesos.v1.Protos.TimeInfo;
import org.apache.mesos.v1.Protos.Unavailability;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.scheduler.base.TaskTestUtil.JOB;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.apache.aurora.scheduler.filter.AttributeAggregate.empty;
import static org.apache.aurora.scheduler.offers.OfferManagerImpl.GLOBALLY_BANNED_OFFERS;
import static org.apache.aurora.scheduler.offers.OfferManagerImpl.OFFER_ACCEPT_RACES;
import static org.apache.aurora.scheduler.offers.OfferManagerImpl.OFFER_CANCEL_FAILURES;
import static org.apache.aurora.scheduler.offers.OfferManagerImpl.OUTSTANDING_OFFERS;
import static org.apache.aurora.scheduler.offers.OfferManagerImpl.STATICALLY_BANNED_OFFERS;
import static org.apache.aurora.scheduler.offers.OfferManagerImpl.VETO_EVALUATED_OFFERS;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosRange;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.offer;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OfferManagerImplTest extends EasyMockTest {

  private static final Amount<Long, Time> RETURN_DELAY = Amount.of(1L, Time.DAYS);
  private static final Amount<Long, Time> ONE_HOUR = Amount.of(1L, Time.HOURS);
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
      IHostAttributes.build(new HostAttributes().setMode(NONE).setHost(HOST_C)));
  private static final int PORT = 1000;
  private static final Protos.Offer MESOS_OFFER = offer(mesosRange(PORTS, PORT));
  private static final IScheduledTask TASK = makeTask("id", JOB);
  private static final ResourceRequest EMPTY_REQUEST = new ResourceRequest(
      TASK.getAssignedTask().getTask(),
      ResourceBag.EMPTY,
      empty());
  private static final TaskGroupKey GROUP_KEY = TaskGroupKey.from(TASK.getAssignedTask().getTask());
  private static final TaskInfo TASK_INFO = TaskInfo.newBuilder()
      .setName("taskName")
      .setTaskId(Protos.TaskID.newBuilder().setValue(Tasks.id(TASK)))
      .setAgentId(MESOS_OFFER.getAgentId())
      .build();
  private static final Operation LAUNCH = Operation.newBuilder()
      .setType(Operation.Type.LAUNCH)
      .setLaunch(Operation.Launch.newBuilder().addTaskInfos(TASK_INFO))
      .build();
  private static final List<Operation> OPERATIONS = ImmutableList.of(LAUNCH);
  private static final long OFFER_FILTER_SECONDS = 0;
  private static final Filters OFFER_FILTER = Filters.newBuilder()
      .setRefuseSeconds(OFFER_FILTER_SECONDS)
      .build();
  private static final FakeTicker FAKE_TICKER = new FakeTicker();

  private Driver driver;
  private OfferManagerImpl offerManager;
  private FakeStatsProvider statsProvider;
  private SchedulingFilter schedulingFilter;

  @Before
  public void setUp() {
    driver = createMock(Driver.class);
    OfferSettings offerSettings = new OfferSettings(
        Amount.of(OFFER_FILTER_SECONDS, Time.SECONDS),
        ImmutableList.of(OfferOrder.RANDOM),
        RETURN_DELAY,
        Long.MAX_VALUE,
        FAKE_TICKER
    );
    statsProvider = new FakeStatsProvider();
    schedulingFilter = createMock(SchedulingFilter.class);

    offerManager = new OfferManagerImpl(driver,
        offerSettings,
        statsProvider,
        new Noop(),
        schedulingFilter);
  }

  @Test
  public void testOffersSortedByUnavailability() {
    HostOffer hostOfferB = setUnavailability(OFFER_B, 1);
    long offerCStartTime = ONE_HOUR.as(Time.MILLISECONDS);
    HostOffer hostOfferC = setUnavailability(OFFER_C, offerCStartTime);

    control.replay();

    offerManager.add(hostOfferB);
    offerManager.add(OFFER_A);
    offerManager.add(hostOfferC);

    List<HostOffer> actual = ImmutableList.copyOf(offerManager.getAll());

    assertEquals(
        // hostOfferC has a further away start time, so it should be preferred.
        ImmutableList.of(OFFER_A, hostOfferC, hostOfferB),
        actual);
  }

  @Test
  public void testOffersSortedByMaintenance() throws Exception {
    // Ensures that non-DRAINING offers are preferred - the DRAINING offer would be tried last.

    HostOffer offerA = setMode(OFFER_A, DRAINING);
    HostOffer offerC = setMode(OFFER_C, DRAINING);

    driver.acceptOffers(OFFER_B.getOffer().getId(), OPERATIONS, OFFER_FILTER);
    expectLastCall();

    control.replay();

    offerManager.add(offerA);
    assertEquals(1, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    offerManager.add(OFFER_B);
    assertEquals(2, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    offerManager.add(offerC);
    assertEquals(3, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    assertEquals(
        ImmutableSet.of(OFFER_B, offerA, offerC),
        ImmutableSet.copyOf(offerManager.getAll()));
    offerManager.launchTask(OFFER_B.getOffer().getId(), TASK_INFO);
    assertEquals(2, statsProvider.getLongValue(OUTSTANDING_OFFERS));
  }

  @Test
  public void hostAttributeChangeUpdatesOfferSorting() {
    control.replay();

    offerManager.hostAttributesChanged(new HostAttributesChanged(HOST_ATTRIBUTES_A));

    offerManager.add(OFFER_A);
    offerManager.add(OFFER_B);
    assertEquals(ImmutableSet.of(OFFER_A, OFFER_B), ImmutableSet.copyOf(offerManager.getAll()));

    HostOffer offerA = setMode(OFFER_A, DRAINING);
    offerManager.hostAttributesChanged(new HostAttributesChanged(offerA.getAttributes()));
    assertEquals(ImmutableSet.of(OFFER_B, offerA), ImmutableSet.copyOf(offerManager.getAll()));

    offerA = setMode(OFFER_A, NONE);
    HostOffer offerB = setMode(OFFER_B, DRAINING);
    offerManager.hostAttributesChanged(new HostAttributesChanged(offerA.getAttributes()));
    offerManager.hostAttributesChanged(new HostAttributesChanged(offerB.getAttributes()));
    assertEquals(ImmutableSet.of(OFFER_A, OFFER_B), ImmutableSet.copyOf(offerManager.getAll()));
  }

  @Test
  public void testAddSameSlaveOffer() {
    driver.declineOffer(OFFER_A_ID, OFFER_FILTER);
    expectLastCall().times(2);

    control.replay();

    offerManager.add(OFFER_A);
    assertEquals(1, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    offerManager.add(OFFER_A);
    assertEquals(0, statsProvider.getLongValue(OUTSTANDING_OFFERS));
  }

  @Test
  public void testGetOffersReturnsAllOffers() {
    control.replay();

    offerManager.add(OFFER_A);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getAll()));
    assertEquals(1, statsProvider.getLongValue(OUTSTANDING_OFFERS));

    offerManager.cancel(OFFER_A_ID);
    assertEquals(0, statsProvider.getLongValue(OFFER_CANCEL_FAILURES));
    assertTrue(Iterables.isEmpty(offerManager.getAll()));
    assertEquals(0, statsProvider.getLongValue(OUTSTANDING_OFFERS));
  }

  @Test
  public void testOfferFilteringDueToStaticBan() {
    expectFilterNone();

    control.replay();

    // Static ban ignored when now offers.
    offerManager.banForTaskGroup(OFFER_A_ID, GROUP_KEY);
    assertEquals(0, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
    offerManager.add(OFFER_A);
    assertEquals(OFFER_A,
        Iterables.getOnlyElement(offerManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getAll()));

    // Add static ban.
    offerManager.banForTaskGroup(OFFER_A_ID, GROUP_KEY);
    assertEquals(1, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getAll()));
    assertTrue(Iterables.isEmpty(offerManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));
  }

  @Test
  public void testStaticBanExpiresAfterMaxHoldTime() throws InterruptedException {
    expectFilterNone();

    control.replay();

    offerManager.add(OFFER_A);
    offerManager.banForTaskGroup(OFFER_A_ID, GROUP_KEY);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getAll()));
    assertTrue(Iterables.isEmpty(offerManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));
    assertEquals(1, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));

    // Make sure the static ban expires after maximum amount of time an offer is held.
    FAKE_TICKER.advance(RETURN_DELAY);
    offerManager.cleanupStaticBans();
    assertEquals(OFFER_A,
        Iterables.getOnlyElement(offerManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));
    assertEquals(0, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
  }

  @Test
  public void testStaticBanIsClearedOnDriverDisconnect() {
    expectFilterNone();

    control.replay();

    offerManager.add(OFFER_A);
    offerManager.banForTaskGroup(OFFER_A_ID, GROUP_KEY);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getAll()));
    assertTrue(Iterables.isEmpty(offerManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));
    assertEquals(1, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));

    // Make sure the static ban is cleared when driver is disconnected.
    offerManager.driverDisconnected(new DriverDisconnected());
    assertEquals(0, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
    offerManager.add(OFFER_A);
    assertEquals(OFFER_A,
        Iterables.getOnlyElement(offerManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));
  }

  @Test
  public void testGetOffer() {
    control.replay();

    offerManager.add(OFFER_A);
    assertEquals(Optional.of(OFFER_A), offerManager.get(OFFER_A.getOffer().getAgentId()));
    assertEquals(1, statsProvider.getLongValue(OUTSTANDING_OFFERS));
  }

  @Test(expected = OfferManager.LaunchException.class)
  public void testAcceptOffersDriverThrows() throws OfferManager.LaunchException {
    driver.acceptOffers(OFFER_A_ID, OPERATIONS, OFFER_FILTER);
    expectLastCall().andThrow(new IllegalStateException());

    control.replay();

    offerManager.add(OFFER_A);
    offerManager.launchTask(OFFER_A_ID, TASK_INFO);
  }

  @Test
  public void testLaunchTaskOfferRaceThrows() {
    control.replay();
    try {
      offerManager.launchTask(OFFER_A_ID, TASK_INFO);
      fail("Method invocation is expected to throw exception.");
    } catch (OfferManager.LaunchException e) {
      assertEquals(1, statsProvider.getLongValue(OFFER_ACCEPT_RACES));
    }
  }

  @Test
  public void testFlushOffers() {
    control.replay();

    offerManager.add(OFFER_A);
    offerManager.add(OFFER_B);
    assertEquals(2, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    offerManager.driverDisconnected(new DriverDisconnected());
    assertEquals(0, statsProvider.getLongValue(OUTSTANDING_OFFERS));
  }

  @Test
  public void testCancelFailure() {
    control.replay();

    offerManager.cancel(OFFER_A.getOffer().getId());
    assertEquals(1, statsProvider.getLongValue(OFFER_CANCEL_FAILURES));
  }

  @Test
  public void testBanAndUnbanOffer() {
    expectFilterNone();

    control.replay();

    // After adding a banned offer, user can see it is in OUTSTANDING_OFFERS but cannot retrieve it.
    offerManager.ban(OFFER_A_ID);
    offerManager.add(OFFER_A);
    assertEquals(1, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    assertEquals(1, statsProvider.getLongValue(GLOBALLY_BANNED_OFFERS));
    assertTrue(Iterables.isEmpty(offerManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));

    offerManager.cancel(OFFER_A_ID);
    offerManager.add(OFFER_A);
    assertEquals(1, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    assertEquals(0, statsProvider.getLongValue(GLOBALLY_BANNED_OFFERS));
    assertEquals(OFFER_A,
        Iterables.getOnlyElement(offerManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));
  }

  private static HostOffer setUnavailability(HostOffer offer, long startMs) {
    Unavailability unavailability = Unavailability.newBuilder()
        .setStart(TimeInfo.newBuilder().setNanoseconds(startMs * 1000L)).build();
    return new HostOffer(
        offer.getOffer().toBuilder().setUnavailability(unavailability).build(),
        offer.getAttributes());
  }

  private static HostOffer setMode(HostOffer offer, MaintenanceMode mode) {
    return new HostOffer(
        offer.getOffer(),
        IHostAttributes.build(offer.getAttributes().newBuilder().setMode(mode)));
  }

  private OfferManager createOrderedManager(List<OfferOrder> order) {
    OfferSettings settings =
        new OfferSettings(
            Amount.of(OFFER_FILTER_SECONDS, Time.SECONDS),
            order,
            RETURN_DELAY,
            Long.MAX_VALUE,
            FAKE_TICKER);
    return new OfferManagerImpl(driver, settings, statsProvider, new Noop(), schedulingFilter);
  }

  @Test
  public void testCPUOrdering() {
    OfferManager cpuManager = createOrderedManager(ImmutableList.of(OfferOrder.CPU));

    HostOffer small = setMode(new HostOffer(
        offer(
            "host1",
            mesosScalar(CPUS, 1.0),
            mesosScalar(CPUS, 24.0, true),
            mesosScalar(RAM_MB, 1024)),
        HOST_ATTRIBUTES_A), DRAINING);
    HostOffer medium = setMode(new HostOffer(
        offer("host2", mesosScalar(CPUS, 5.0), mesosScalar(RAM_MB, 1024)),
        HOST_ATTRIBUTES_A), DRAINING);
    HostOffer large = setMode(new HostOffer(
        offer("host3", mesosScalar(CPUS, 10.0), mesosScalar(RAM_MB, 1024)),
        HOST_ATTRIBUTES_A), DRAINING);

    expectFilterNone();

    control.replay();

    cpuManager.add(medium);
    cpuManager.add(large);
    cpuManager.add(small);

    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));
    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getAll()));
  }

  @Test
  public void testRevocableCPUOrdering() {
    ResourceType.initializeEmptyCliArgsForTest();
    OfferManager cpuManager = createOrderedManager(ImmutableList.of(OfferOrder.REVOCABLE_CPU));

    HostOffer small = setMode(new HostOffer(
        offer(
            "host2",
            mesosScalar(CPUS, 5.0),
            mesosScalar(CPUS, 23.0, true),
            mesosScalar(RAM_MB, 1024)),
        HOST_ATTRIBUTES_A), DRAINING);
    HostOffer medium = setMode(new HostOffer(
        offer(
            "host1",
            mesosScalar(CPUS, 3.0),
            mesosScalar(CPUS, 24.0, true),
            mesosScalar(RAM_MB, 1024)),
        HOST_ATTRIBUTES_A), DRAINING);
    HostOffer large = setMode(new HostOffer(
        offer("host3", mesosScalar(CPUS, 1.0), mesosScalar(RAM_MB, 1024)),
        HOST_ATTRIBUTES_A), DRAINING);

    expectFilterNone();

    control.replay();

    cpuManager.add(medium);
    cpuManager.add(large);
    cpuManager.add(small);

    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, true)));
    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getAll()));
  }

  @Test
  public void testDiskOrdering() {
    OfferManager cpuManager = createOrderedManager(ImmutableList.of(OfferOrder.DISK));

    HostOffer small = setMode(new HostOffer(
        offer("host1", mesosScalar(CPUS, 1), mesosScalar(RAM_MB, 1), mesosScalar(DISK_MB, 1.0)),
        HOST_ATTRIBUTES_A), DRAINING);
    HostOffer medium = setMode(new HostOffer(
        offer("host2", mesosScalar(CPUS, 1), mesosScalar(RAM_MB, 1), mesosScalar(DISK_MB, 5.0)),
        HOST_ATTRIBUTES_A), DRAINING);
    HostOffer large = setMode(new HostOffer(
        offer("host3", mesosScalar(CPUS, 1), mesosScalar(RAM_MB, 1), mesosScalar(DISK_MB, 10.0)),
        HOST_ATTRIBUTES_A), DRAINING);

    expectFilterNone();

    control.replay();

    cpuManager.add(medium);
    cpuManager.add(large);
    cpuManager.add(small);

    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));
    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getAll()));
  }

  @Test
  public void testMemoryOrdering() {
    OfferManager cpuManager = createOrderedManager(ImmutableList.of(OfferOrder.MEMORY));

    HostOffer small = setMode(new HostOffer(
        offer("host1", mesosScalar(CPUS, 10), mesosScalar(RAM_MB, 1.0)),
        HOST_ATTRIBUTES_A), DRAINING);
    HostOffer medium = setMode(new HostOffer(
        offer("host2", mesosScalar(CPUS, 10), mesosScalar(RAM_MB, 5.0)),
        HOST_ATTRIBUTES_A), DRAINING);
    HostOffer large = setMode(new HostOffer(
        offer("host3", mesosScalar(CPUS, 10), mesosScalar(RAM_MB, 10.0)),
        HOST_ATTRIBUTES_A), DRAINING);

    expectFilterNone();

    control.replay();

    cpuManager.add(medium);
    cpuManager.add(large);
    cpuManager.add(small);

    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));
    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getAll()));
  }

  @Test
  public void testCPUMemoryOrdering() {
    OfferManager cpuManager = createOrderedManager(
        ImmutableList.of(OfferOrder.CPU, OfferOrder.MEMORY));

    HostOffer small = setMode(new HostOffer(
        offer("host1",
            mesosScalar(CPUS, 1.0),
            mesosScalar(RAM_MB, 2.0),
            mesosScalar(DISK_MB, 3.0)),
        HOST_ATTRIBUTES_A), DRAINING);
    HostOffer medium = setMode(new HostOffer(
        offer("host2",
            mesosScalar(CPUS, 1.0),
            mesosScalar(RAM_MB, 3.0),
            mesosScalar(DISK_MB, 2.0)),
        HOST_ATTRIBUTES_A), DRAINING);
    HostOffer large = setMode(new HostOffer(
        offer("host3",
            mesosScalar(CPUS, 10.0),
            mesosScalar(CPUS, 1.0),
            mesosScalar(RAM_MB, 1024),
            mesosScalar(DISK_MB, 1.0)),
        HOST_ATTRIBUTES_A), DRAINING);

    expectFilterNone();

    control.replay();

    cpuManager.add(large);
    cpuManager.add(medium);
    cpuManager.add(small);

    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));
    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getAll()));
  }

  @Test
  public void testDelayedOfferReturn() {
    OfferSettings settings = new OfferSettings(
        Amount.of(OFFER_FILTER_SECONDS, Time.SECONDS),
        ImmutableList.of(OfferOrder.RANDOM),
        RETURN_DELAY,
        Long.MAX_VALUE,
        FAKE_TICKER);
    ScheduledExecutorService executorMock = createMock(ScheduledExecutorService.class);
    FakeScheduledExecutor clock = FakeScheduledExecutor.fromScheduledExecutorService(executorMock);
    addTearDown(clock::assertEmpty);
    offerManager = new OfferManagerImpl(
        driver,
        settings,
        statsProvider,
        new Deferment.DelayedDeferment(() -> RETURN_DELAY, executorMock),
        schedulingFilter);

    driver.declineOffer(OFFER_A_ID, OFFER_FILTER);

    control.replay();

    offerManager.add(OFFER_A);
    assertEquals(1, statsProvider.getLongValue(OUTSTANDING_OFFERS));

    clock.advance(RETURN_DELAY);
    assertEquals(0, statsProvider.getLongValue(OUTSTANDING_OFFERS));
  }

  @Test
  public void testTwoOffersPerHost() {
    // Test for regression of AURORA-1952, where a specific call order could cause OfferManager
    // to violate its one-offer-per-host invariant.

    HostOffer sameAgent = new HostOffer(
        OFFER_A.getOffer().toBuilder().setId(OfferID.newBuilder().setValue("sameAgent")).build(),
        HOST_ATTRIBUTES_A);
    HostOffer sameAgent2 = new HostOffer(
        OFFER_A.getOffer().toBuilder().setId(OfferID.newBuilder().setValue("sameAgent2")).build(),
        HOST_ATTRIBUTES_A);

    driver.declineOffer(OFFER_A_ID, OFFER_FILTER);
    driver.declineOffer(sameAgent.getOffer().getId(), OFFER_FILTER);

    control.replay();

    offerManager.ban(OFFER_A_ID);
    offerManager.add(OFFER_A);
    offerManager.add(sameAgent);
    offerManager.cancel(OFFER_A_ID);
    offerManager.add(sameAgent2);
    assertEquals(ImmutableSet.of(sameAgent2), offerManager.getAll());
  }

  private void expectFilterNone() {
    // Most tests will use a permissive scheduling filter
    expect(schedulingFilter.filter(anyObject(), anyObject()))
        .andReturn(ImmutableSet.of())
        .anyTimes();
  }

  @Test
  public void testGetMatchingSingleAgent() {
    expectFilterNone();

    control.replay();
    offerManager.add(OFFER_A);
    assertEquals(Optional.of(OFFER_A),
        offerManager.getMatching(OFFER_A.getOffer().getAgentId(), EMPTY_REQUEST, false));
  }

  @Test
  public void testGetMatchingNoGloballyBanned() {
    expectFilterNone();

    control.replay();
    offerManager.add(OFFER_A);
    assertEquals(0, statsProvider.getLongValue(GLOBALLY_BANNED_OFFERS));
    offerManager.ban(OFFER_A_ID);
    assertEquals(Optional.absent(),
        offerManager.getMatching(OFFER_A.getOffer().getAgentId(), EMPTY_REQUEST, false));
    assertEquals(1, statsProvider.getLongValue(GLOBALLY_BANNED_OFFERS));
  }

  @Test
  public void testGetMatchingNoVetoed() {
    // Calling getMatching when a veto is present should return an empty option. Additionally,
    // it should not statically ban the offer if it is vetoed.
    expect(schedulingFilter.filter(new UnusedResource(OFFER_A, false), EMPTY_REQUEST))
        .andReturn(ImmutableSet.of(SchedulingFilter.Veto.dedicatedHostConstraintMismatch()));

    control.replay();
    offerManager.add(OFFER_A);
    assertEquals(0, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
    assertEquals(Optional.absent(),
        offerManager.getMatching(OFFER_A.getOffer().getAgentId(), EMPTY_REQUEST, false));
    assertEquals(0, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
  }

  @Test
  public void testGetAllMatching() {
    expectFilterNone();

    control.replay();
    offerManager.add(OFFER_A);
    offerManager.add(OFFER_B);
    offerManager.add(OFFER_C);
    assertEquals(0, statsProvider.getLongValue(VETO_EVALUATED_OFFERS));
    assertEquals(ImmutableSet.of(OFFER_A, OFFER_B, OFFER_C),
        ImmutableSet.copyOf(offerManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));
    assertEquals(3, statsProvider.getLongValue(VETO_EVALUATED_OFFERS));
  }

  @Test
  public void testGetAllMatchingNoGloballyBanned() {
    expectFilterNone();

    control.replay();
    offerManager.add(OFFER_A);
    offerManager.add(OFFER_B);
    offerManager.add(OFFER_C);
    assertEquals(0, statsProvider.getLongValue(VETO_EVALUATED_OFFERS));
    assertEquals(0, statsProvider.getLongValue(GLOBALLY_BANNED_OFFERS));
    offerManager.ban(OFFER_B.getOffer().getId());
    assertEquals(ImmutableSet.of(OFFER_A, OFFER_C),
        ImmutableSet.copyOf(offerManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));
    assertEquals(2, statsProvider.getLongValue(VETO_EVALUATED_OFFERS));
    assertEquals(1, statsProvider.getLongValue(GLOBALLY_BANNED_OFFERS));
  }

  @Test
  public void testGetAllMatchingNoStaticallyBanned() {
    expectFilterNone();

    control.replay();
    offerManager.add(OFFER_A);
    offerManager.add(OFFER_B);
    offerManager.add(OFFER_C);
    assertEquals(0, statsProvider.getLongValue(VETO_EVALUATED_OFFERS));
    assertEquals(0, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
    offerManager.banForTaskGroup(OFFER_B.getOffer().getId(), GROUP_KEY);
    assertEquals(ImmutableSet.of(OFFER_A, OFFER_C),
        ImmutableSet.copyOf(offerManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));
    assertEquals(2, statsProvider.getLongValue(VETO_EVALUATED_OFFERS));
    assertEquals(1, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
    assertEquals(ImmutableSet.of(Pair.of(OFFER_B.getOffer().getId(), GROUP_KEY)),
        offerManager.getStaticBans());
  }

  @Test
  public void testGetAllMatchingIgnoreNoCpusAndMem() {
    expectFilterNone();

    HostOffer empty = setMode(new HostOffer(
        offer("host1",
            mesosScalar(CPUS, 0),
            mesosScalar(RAM_MB, 0),
            mesosScalar(DISK_MB, 3.0)),
        HOST_ATTRIBUTES_A), NONE);

    control.replay();
    offerManager.add(empty);
    offerManager.add(OFFER_A);
    assertEquals(0, statsProvider.getLongValue(GLOBALLY_BANNED_OFFERS));
    assertEquals(0, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
    assertEquals(0, statsProvider.getLongValue(VETO_EVALUATED_OFFERS));
    assertEquals(ImmutableSet.of(OFFER_A),
        ImmutableSet.copyOf(offerManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));
    assertEquals(1, statsProvider.getLongValue(VETO_EVALUATED_OFFERS));
    assertEquals(ImmutableSet.of(empty, OFFER_A),
        ImmutableSet.copyOf(offerManager.getAll()));
  }

  @Test
  public void testGetAllMatchingNoVetoed() {
    // Calling getAllMatching should statically ban the offer as well if it is statically vetoed
    expect(schedulingFilter.filter(new UnusedResource(OFFER_A, false), EMPTY_REQUEST))
        .andReturn(ImmutableSet.of(SchedulingFilter.Veto.dedicatedHostConstraintMismatch()));
    expect(schedulingFilter.filter(new UnusedResource(OFFER_B, false), EMPTY_REQUEST))
        .andReturn(ImmutableSet.of());
    expect(schedulingFilter.filter(new UnusedResource(OFFER_C, false), EMPTY_REQUEST))
        .andReturn(ImmutableSet.of(SchedulingFilter.Veto.unsatisfiedLimit("test_limit")));

    control.replay();
    offerManager.add(OFFER_A);
    offerManager.add(OFFER_B);
    offerManager.add(OFFER_C);
    assertEquals(0, statsProvider.getLongValue(VETO_EVALUATED_OFFERS));
    assertEquals(0, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
    assertEquals(ImmutableSet.of(OFFER_B),
        ImmutableSet.copyOf(offerManager.getAllMatching(GROUP_KEY, EMPTY_REQUEST, false)));
    assertEquals(3, statsProvider.getLongValue(VETO_EVALUATED_OFFERS));
    assertEquals(1, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
    assertEquals(ImmutableSet.of(Pair.of(OFFER_A.getOffer().getId(), GROUP_KEY)),
        offerManager.getStaticBans());
  }
}
