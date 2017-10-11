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
package org.apache.aurora.scheduler.preemptor;

import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.filter.SchedulingFilterImpl;
import org.apache.aurora.scheduler.mesos.TaskExecutors;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceTestUtil;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.mesos.v1.Protos;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static java.util.stream.Collectors.toSet;

import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.base.TaskTestUtil.DEV_TIER;
import static org.apache.aurora.scheduler.base.TaskTestUtil.PREFERRED_TIER;
import static org.apache.aurora.scheduler.base.TaskTestUtil.REVOCABLE_TIER;
import static org.apache.aurora.scheduler.filter.AttributeAggregate.empty;
import static org.apache.aurora.scheduler.preemptor.PreemptionVictimFilter.PreemptionVictimFilterImpl.ORDER;
import static org.apache.aurora.scheduler.preemptor.PreemptorMetrics.MISSING_ATTRIBUTES_NAME;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.bag;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosRange;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.GPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.apache.mesos.v1.Protos.Offer;
import static org.apache.mesos.v1.Protos.Resource;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class PreemptionVictimFilterTest extends EasyMockTest {
  private static final String USER_A = "user_a";
  private static final String USER_B = "user_b";
  private static final String USER_C = "user_c";
  private static final String JOB_A = "job_a";
  private static final String JOB_B = "job_b";
  private static final String JOB_C = "job_c";
  private static final String TASK_ID_A = "task_a";
  private static final String TASK_ID_B = "task_b";
  private static final String TASK_ID_C = "task_c";
  private static final String TASK_ID_D = "task_d";
  private static final String HOST_A = "hostA";
  private static final String RACK_A = "rackA";
  private static final String SLAVE_ID = HOST_A + "_id";
  private static final String RACK_ATTRIBUTE = "rack";
  private static final String HOST_ATTRIBUTE = "host";
  private static final String OFFER = "offer";
  private static final Optional<HostOffer> NO_OFFER = Optional.absent();
  private static final Amount<Long, Time> UNAVAILABLITY_THRESHOLD = Amount.of(1L, Time.MINUTES);

  private StorageTestUtil storageUtil;
  private SchedulingFilter schedulingFilter;
  private FakeStatsProvider statsProvider;
  private PreemptorMetrics preemptorMetrics;
  private TierManager tierManager;
  private FakeClock clock;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    statsProvider = new FakeStatsProvider();
    preemptorMetrics = new PreemptorMetrics(new CachedCounters(statsProvider));
    tierManager = createMock(TierManager.class);
    clock = new FakeClock();
    ResourceType.initializeEmptyCliArgsForTest();
  }

  private Optional<ImmutableSet<PreemptionVictim>> runFilter(
      ScheduledTask pendingTask,
      Optional<HostOffer> offer,
      ScheduledTask... victims) {

    PreemptionVictimFilter.PreemptionVictimFilterImpl filter =
        new PreemptionVictimFilter.PreemptionVictimFilterImpl(
            schedulingFilter,
            TaskExecutors.NO_OVERHEAD_EXECUTOR,
            preemptorMetrics,
            tierManager);

    return filter.filterPreemptionVictims(
        ITaskConfig.build(pendingTask.getAssignedTask().getTask()),
        preemptionVictims(victims),
        empty(),
        offer,
        storageUtil.mutableStoreProvider);
  }

  @Test
  public void testPreempted() throws Exception {
    setUpHost();

    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A);
    assignToHost(lowPriority);
    expectGetTier(lowPriority, DEV_TIER).times(2);

    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 100);
    expectGetTier(highPriority, DEV_TIER);

    expectFiltering();

    control.replay();
    assertVictims(runFilter(highPriority, NO_OFFER, lowPriority), lowPriority);
  }

  @Test
  public void testLowestPriorityPreempted() throws Exception {
    setUpHost();

    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A, 10);
    assignToHost(lowPriority);

    ScheduledTask lowerPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 1);
    assignToHost(lowerPriority);
    expectGetTier(lowerPriority, DEV_TIER).atLeastOnce();

    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_C, 100);
    expectGetTier(highPriority, DEV_TIER);

    expectFiltering();

    control.replay();
    assertVictims(runFilter(highPriority, NO_OFFER, lowerPriority), lowerPriority);
  }

  @Test
  public void testOnePreemptableTask() throws Exception {
    setUpHost();

    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_A, 100);
    assignToHost(highPriority);
    expectGetTier(highPriority, DEV_TIER);

    ScheduledTask lowerPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 99);
    assignToHost(lowerPriority);
    expectGetTier(lowerPriority, DEV_TIER);

    ScheduledTask lowestPriority = makeTask(USER_A, JOB_A, TASK_ID_C, 1);
    assignToHost(lowestPriority);
    expectGetTier(lowestPriority, DEV_TIER).times(2);

    ScheduledTask pendingPriority = makeTask(USER_A, JOB_A, TASK_ID_D, 98);
    expectGetTier(pendingPriority, DEV_TIER).times(3);

    expectFiltering();

    control.replay();
    assertVictims(
        runFilter(pendingPriority, NO_OFFER, highPriority, lowerPriority, lowestPriority),
        lowestPriority);
  }

  @Test
  public void testHigherPriorityRunning() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 100);
    assignToHost(highPriority);
    expectGetTier(highPriority, DEV_TIER);

    ScheduledTask task = makeTask(USER_A, JOB_A, TASK_ID_A);
    expectGetTier(task, DEV_TIER);

    control.replay();
    assertNoVictims(runFilter(task, NO_OFFER, highPriority));
  }

  @Test
  public void testProductionPreemptingNonproduction() throws Exception {
    setUpHost();

    schedulingFilter = createMock(SchedulingFilter.class);
    // Use a very low priority for the production task to show that priority is irrelevant.
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", -1000);
    expectGetTier(p1, PREFERRED_TIER);
    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_B + "_a1", 100);
    expectGetTier(a1, DEV_TIER).times(2);
    assignToHost(a1);

    expectFiltering();

    control.replay();
    assertVictims(runFilter(p1, NO_OFFER, a1), a1);
  }

  @Test
  public void testProductionPreemptingNonproductionAcrossUsers() throws Exception {
    setUpHost();

    schedulingFilter = createMock(SchedulingFilter.class);
    // Use a very low priority for the production task to show that priority is irrelevant.
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", -1000);
    expectGetTier(p1, PREFERRED_TIER);
    ScheduledTask a1 = makeTask(USER_B, JOB_A, TASK_ID_B + "_a1", 100);
    assignToHost(a1);
    expectGetTier(a1, DEV_TIER).times(2);

    expectFiltering();

    control.replay();
    assertVictims(runFilter(p1, NO_OFFER, a1), a1);
  }

  @Test
  public void testProductionUsersDoNotPreemptEachOther() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", 1000);
    expectGetTier(p1, PREFERRED_TIER);
    ScheduledTask a1 = makeProductionTask(USER_B, JOB_A, TASK_ID_B + "_a1", 0);
    expectGetTier(a1, PREFERRED_TIER);
    assignToHost(a1);

    control.replay();
    assertNoVictims(runFilter(p1, NO_OFFER, a1));
  }

  // Ensures a production task can preempt 2 tasks on the same host.
  @Test
  public void testProductionPreemptingManyNonProduction() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(UNAVAILABLITY_THRESHOLD, clock);
    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    setResource(a1, CPUS, 1.0);
    setResource(a1, RAM_MB, 512.0);
    expectGetTier(a1, DEV_TIER).atLeastOnce();

    ScheduledTask b1 = makeTask(USER_B, JOB_B, TASK_ID_B + "_b1");
    setResource(b1, CPUS, 1.0);
    setResource(b1, RAM_MB, 512.0);
    expectGetTier(b1, DEV_TIER).atLeastOnce();

    setUpHost();

    assignToHost(a1);
    assignToHost(b1);

    ScheduledTask p1 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1");
    setResource(p1, CPUS, 2.0);
    setResource(p1, RAM_MB, 1024.0);
    expectGetTier(p1, PREFERRED_TIER).times(2);

    control.replay();
    assertVictims(runFilter(p1, NO_OFFER, a1, b1), a1, b1);
  }

  // Ensures we select the minimal number of tasks to preempt
  @Test
  public void testMinimalSetPreempted() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(UNAVAILABLITY_THRESHOLD, clock);
    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    setResource(a1, CPUS, 4.0);
    setResource(a1, RAM_MB, 4096.0);
    expectGetTier(a1, DEV_TIER).atLeastOnce();

    ScheduledTask b1 = makeTask(USER_B, JOB_B, TASK_ID_B + "_b1");
    b1.getAssignedTask().getTask()
        .setResources(ImmutableSet.of(
            numCpus(1),
            ramMb(512)));
    setResource(b1, CPUS, 1.0);
    setResource(b1, RAM_MB, 512.0);
    expectGetTier(b1, DEV_TIER).anyTimes();

    ScheduledTask b2 = makeTask(USER_B, JOB_B, TASK_ID_B + "_b2");
    setResource(b2, CPUS, 1.0);
    setResource(b2, RAM_MB, 512.0);
    expectGetTier(b2, DEV_TIER).anyTimes();

    setUpHost();

    assignToHost(a1);
    assignToHost(b1);
    assignToHost(b2);

    ScheduledTask p1 = makeProductionTask(USER_C, JOB_C, TASK_ID_C + "_p1");
    setResource(p1, CPUS, 2.0);
    setResource(p1, RAM_MB, 1024.0);
    expectGetTier(p1, PREFERRED_TIER).times(3);

    control.replay();
    assertVictims(runFilter(p1, NO_OFFER, b1, b2, a1), a1);
  }

  // Ensures a production task *never* preempts a production task from another job.
  @Test
  public void testProductionJobNeverPreemptsProductionJob() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(UNAVAILABLITY_THRESHOLD, clock);
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1");
    p1.getAssignedTask().getTask()
        .setResources(ImmutableSet.of(
            numCpus(2),
            ramMb(1024)));
    expectGetTier(p1, PREFERRED_TIER);

    setUpHost();

    assignToHost(p1);

    ScheduledTask p2 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p2");
    p2.getAssignedTask().getTask()
        .setResources(ImmutableSet.of(
            numCpus(1),
            ramMb(512)));
    expectGetTier(p2, PREFERRED_TIER);

    control.replay();
    assertNoVictims(runFilter(p2, NO_OFFER, p1));
  }

  // Ensures that we can preempt if a task + offer can satisfy a pending task.
  @Test
  public void testPreemptWithOfferAndTask() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(UNAVAILABLITY_THRESHOLD, clock);

    setUpHost();

    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    a1.getAssignedTask().getTask()
        .setResources(ImmutableSet.of(
            numCpus(1),
            ramMb(512)));
    assignToHost(a1);
    expectGetTier(a1, DEV_TIER).times(2);

    ScheduledTask p1 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1");
    p1.getAssignedTask().getTask()
        .setResources(ImmutableSet.of(
            numCpus(2),
            ramMb(1024)));
    expectGetTier(p1, PREFERRED_TIER);

    control.replay();
    assertVictims(
        runFilter(
            p1,
            makeOffer(OFFER, 1, Amount.of(512L, Data.MB), Amount.of(1L, Data.MB), 1, false),
            a1),
        a1);
  }

  // Ensures revocable offer resources are filtered out.
  @Test
  public void testRevocableOfferFiltered() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(UNAVAILABLITY_THRESHOLD, clock);

    setUpHost();

    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    setResource(a1, CPUS, 1.0);
    setResource(a1, RAM_MB, 512.0);
    assignToHost(a1);
    expectGetTier(a1, DEV_TIER).times(2);

    ScheduledTask p1 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1");
    setResource(p1, CPUS, 2.0);
    setResource(p1, RAM_MB, 1024.0);
    expectGetTier(p1, PREFERRED_TIER);

    control.replay();
    assertNoVictims(runFilter(
        p1,
        makeOffer(OFFER, 1, Amount.of(512L, Data.MB), Amount.of(1L, Data.MB), 1, true),
        a1));
  }

  // Ensures revocable task CPU is not considered for preemption.
  @Test
  public void testRevocableVictimsFiltered() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(UNAVAILABLITY_THRESHOLD, clock);

    setUpHost();

    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    setResource(a1, CPUS, 1.0);
    setResource(a1, RAM_MB, 512.0);
    assignToHost(a1);
    expectGetTier(a1, REVOCABLE_TIER).times(2);

    ScheduledTask p1 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1");
    setResource(p1, CPUS, 2.0);
    setResource(p1, RAM_MB, 1024.0);
    expectGetTier(p1, PREFERRED_TIER);

    control.replay();
    assertNoVictims(runFilter(
        p1,
        makeOffer(OFFER, 1, Amount.of(512L, Data.MB), Amount.of(1L, Data.MB), 1, false),
        a1));
  }

  // Ensures revocable victim non-compressible resources are still considered.
  @Test
  public void testRevocableVictimRamUsed() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(UNAVAILABLITY_THRESHOLD, clock);

    setUpHost();

    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    setResource(a1, CPUS, 1.0);
    setResource(a1, RAM_MB, 512.0);
    assignToHost(a1);
    expectGetTier(a1, REVOCABLE_TIER).times(2);

    ScheduledTask p1 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1");
    setResource(p1, CPUS, 2.0);
    setResource(p1, RAM_MB, 1024.0);
    expectGetTier(p1, PREFERRED_TIER);

    control.replay();
    assertVictims(
        runFilter(
            p1,
            makeOffer(OFFER, 2, Amount.of(512L, Data.MB), Amount.of(1L, Data.MB), 1, false),
            a1),
        a1);
  }

  // Ensures we can preempt if two tasks and an offer can satisfy a pending task.
  @Test
  public void testPreemptWithOfferAndMultipleTasks() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(UNAVAILABLITY_THRESHOLD, clock);

    setUpHost();

    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    setResource(a1, CPUS, 1.0);
    setResource(a1, RAM_MB, 512.0);
    assignToHost(a1);
    expectGetTier(a1, DEV_TIER).atLeastOnce();

    ScheduledTask a2 = makeTask(USER_A, JOB_B, TASK_ID_A + "_a2");
    setResource(a2, CPUS, 1.0);
    setResource(a2, RAM_MB, 512.0);
    assignToHost(a2);
    expectGetTier(a2, DEV_TIER).atLeastOnce();

    ScheduledTask p1 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1");
    setResource(p1, CPUS, 4.0);
    setResource(p1, RAM_MB, 2048.0);
    expectGetTier(p1, PREFERRED_TIER).times(2);

    control.replay();
    Optional<HostOffer> offer =
        makeOffer(OFFER, 2, Amount.of(1024L, Data.MB), Amount.of(1L, Data.MB), 1, false);
    assertVictims(runFilter(p1, offer, a1, a2), a1, a2);
  }

  @Test
  public void testNoPreemptionVictims() {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask task = makeProductionTask(USER_A, JOB_A, TASK_ID_A);

    control.replay();

    assertNoVictims(runFilter(task, NO_OFFER));
  }

  @Test
  public void testMissingAttributes() {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask task = makeProductionTask(USER_A, JOB_A, TASK_ID_A);
    assignToHost(task);
    expectGetTier(task, PREFERRED_TIER);

    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    setResource(a1, CPUS, 1.0);
    setResource(a1, RAM_MB, 512.0);
    assignToHost(a1);
    expectGetTier(a1, DEV_TIER);

    expect(storageUtil.attributeStore.getHostAttributes(HOST_A)).andReturn(Optional.absent());

    control.replay();

    assertNoVictims(runFilter(task, NO_OFFER, a1));
    assertEquals(1L, statsProvider.getLongValue(MISSING_ATTRIBUTES_NAME));
  }

  @Test
  public void testAllVictimsVetoed() {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask task = makeProductionTask(USER_A, JOB_A, TASK_ID_A);
    assignToHost(task);
    expectGetTier(task, PREFERRED_TIER);

    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    setResource(a1, CPUS, 1.0);
    setResource(a1, RAM_MB, 512.0);
    assignToHost(a1);
    expectGetTier(a1, DEV_TIER).times(2);

    setUpHost();
    expectFiltering(Optional.of(Veto.constraintMismatch("ban")));

    control.replay();

    assertNoVictims(runFilter(task, NO_OFFER, a1));
  }

  @Test
  public void testOrder() {
    control.replay();

    ResourceBag one = bag(1, 1, 1);
    ResourceBag two = bag(2, 2, 2);
    ResourceBag three = bag(3, 3, 3);
    assertEquals(
        ImmutableList.of(one, two, three, three),
        ORDER.sortedCopy(ImmutableList.of(three, one, two, three)));
  }

  @Test
  public void testOrderDifferentResources() {
    control.replay();

    ResourceBag one = bag(ImmutableMap.of(
        CPUS, 1.0,
        RAM_MB, 1.0
    ));

    ResourceBag two = bag(ImmutableMap.of(
        CPUS, 1.0,
        GPUS, 1.0
    ));

    ResourceBag three = bag(ImmutableMap.of(
        CPUS, 1.0
    ));

    assertEquals(0, ORDER.compare(one, two));
    assertEquals(1, ORDER.compare(one, three));
  }

  private static ImmutableSet<PreemptionVictim> preemptionVictims(ScheduledTask... tasks) {
    return FluentIterable.from(ImmutableSet.copyOf(tasks))
        .transform(
            task -> PreemptionVictim.fromTask(IAssignedTask.build(task.getAssignedTask()))).toSet();
  }

  private static void assertVictims(
      Optional<ImmutableSet<PreemptionVictim>> actual,
      ScheduledTask... expected) {

    assertEquals(Optional.of(preemptionVictims(expected)), actual);
  }

  private static void assertNoVictims(Optional<ImmutableSet<PreemptionVictim>> actual) {
    assertEquals(Optional.<ImmutableSet<PreemptionVictim>>absent(), actual);
  }

  private Optional<HostOffer> makeOffer(
      String offerId,
      double cpu,
      Amount<Long, Data> ram,
      Amount<Long, Data> disk,
      int numPorts,
      boolean revocable) {

    List<Resource> resources = ImmutableList.of(
        mesosScalar(CPUS, cpu),
        mesosScalar(RAM_MB, ram.getValue()),
        mesosScalar(DISK_MB, disk.getValue()),
        mesosRange(
            PORTS,
            Optional.absent(),
            IntStream.range(1, numPorts).boxed().collect(toSet())));
    if (revocable) {
      resources = ImmutableList.<Resource>builder()
          .addAll(FluentIterable.from(resources)
              .filter(e -> !e.getName().equals(CPUS.getMesosName()))
              .toList())
          .add(Protos.Resource.newBuilder()
              .setName(CPUS.getMesosName())
              .setType(Protos.Value.Type.SCALAR)
              .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpu))
              .setRevocable(Resource.RevocableInfo.newBuilder())
              .build())
          .build();
    }
    Offer.Builder builder = Offer.newBuilder();
    builder.getIdBuilder().setValue(offerId);
    builder.getFrameworkIdBuilder().setValue("framework-id");
    builder.getAgentIdBuilder().setValue(SLAVE_ID);
    builder.setHostname(HOST_A);
    builder.addAllResources(resources);

    return Optional.of(new HostOffer(
        builder.build(),
        IHostAttributes.build(new HostAttributes().setMode(NONE))));
  }

  private IExpectationSetters<Set<SchedulingFilter.Veto>> expectFiltering() {
    return expectFiltering(Optional.absent());
  }

  private IExpectationSetters<Set<SchedulingFilter.Veto>> expectFiltering(
      final Optional<Veto> veto) {

    return expect(schedulingFilter.filter(
        EasyMock.anyObject(),
        EasyMock.anyObject()))
        .andAnswer(
            veto::asSet);
  }

  private IExpectationSetters<TierInfo> expectGetTier(ScheduledTask task, TierInfo tier) {
    return expect(tierManager.getTier(ITaskConfig.build(task.getAssignedTask().getTask())))
        .andReturn(tier);
  }

  private static void setResource(ScheduledTask task, ResourceType type, Double value) {
    task.getAssignedTask().setTask(ResourceTestUtil.resetResource(
        ITaskConfig.build(task.getAssignedTask().getTask()),
        type,
        value).newBuilder());
  }

  private static ScheduledTask makeTask(
      String role,
      String job,
      String taskId,
      int priority,
      String env,
      boolean production) {

    AssignedTask assignedTask = new AssignedTask()
        .setTaskId(taskId)
        .setTask(new TaskConfig()
            .setJob(new JobKey(role, env, job))
            .setPriority(priority)
            .setProduction(production)
            .setConstraints(Sets.newHashSet())
            .setExecutorConfig(new ExecutorConfig(apiConstants.AURORA_EXECUTOR_NAME, "config")));
    return new ScheduledTask().setAssignedTask(assignedTask);
  }

  static ScheduledTask makeTask(String role, String job, String taskId) {
    return makeTask(role, job, taskId, 0, "dev", false);
  }

  static void addEvent(ScheduledTask task, ScheduleStatus status) {
    task.addToTaskEvents(new TaskEvent(0, status));
  }

  private ScheduledTask makeProductionTask(String role, String job, String taskId) {
    return makeTask(role, job, taskId, 0, "prod", true);
  }

  private ScheduledTask makeProductionTask(String role, String job, String taskId, int priority) {
    return makeTask(role, job, taskId, priority, "prod", true);
  }

  private ScheduledTask makeTask(String role, String job, String taskId, int priority) {
    return makeTask(role, job, taskId, priority, "dev", false);
  }

  private void assignToHost(ScheduledTask task) {
    task.setStatus(RUNNING);
    addEvent(task, RUNNING);
    task.getAssignedTask().setSlaveHost(HOST_A);
    task.getAssignedTask().setSlaveId(SLAVE_ID);
  }

  private Attribute host(String host) {
    return new Attribute(HOST_ATTRIBUTE, ImmutableSet.of(host));
  }

  private Attribute rack(String rack) {
    return new Attribute(RACK_ATTRIBUTE, ImmutableSet.of(rack));
  }

  // Sets up a normal host, no dedicated hosts and no maintenance.
  private void setUpHost() {
    IHostAttributes hostAttrs = IHostAttributes.build(
        new HostAttributes().setHost(HOST_A).setSlaveId(HOST_A + "_id")
            .setMode(NONE).setAttributes(ImmutableSet.of(rack(RACK_A), host(RACK_A))));

    expect(storageUtil.attributeStore.getHostAttributes(HOST_A))
        .andReturn(Optional.of(hostAttrs)).anyTimes();
  }
}
