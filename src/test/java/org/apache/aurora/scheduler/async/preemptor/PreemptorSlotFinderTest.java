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
package org.apache.aurora.scheduler.async.preemptor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.async.OfferManager;
import org.apache.aurora.scheduler.async.preemptor.PreemptionSlotFinder.PreemptionSlot;
import org.apache.aurora.scheduler.async.preemptor.PreemptionSlotFinder.PreemptionSlotFinderImpl;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.filter.SchedulingFilterImpl;
import org.apache.aurora.scheduler.mesos.TaskExecutors;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.mesos.Protos;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.async.preemptor.PreemptorMetrics.MISSING_ATTRIBUTES_NAME;
import static org.apache.aurora.scheduler.filter.AttributeAggregate.EMPTY;
import static org.apache.mesos.Protos.Offer;
import static org.apache.mesos.Protos.Resource;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class PreemptorSlotFinderTest extends EasyMockTest {
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
  private static final String HOST = "host";
  private static final String RACK = "rack";
  private static final String SLAVE_ID = HOST + "_id";
  private static final String RACK_ATTRIBUTE = "rack";
  private static final String HOST_ATTRIBUTE = "host";
  private static final String OFFER = "offer";

  private StorageTestUtil storageUtil;
  private SchedulingFilter schedulingFilter;
  private FakeStatsProvider statsProvider;
  private ClusterState clusterState;
  private OfferManager offerManager;
  private PreemptorMetrics preemptorMetrics;

  @Before
  public void setUp() {
    offerManager = createMock(OfferManager.class);
    clusterState = createMock(ClusterState.class);
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    statsProvider = new FakeStatsProvider();
    preemptorMetrics = new PreemptorMetrics(new CachedCounters(statsProvider));
  }

  private Optional<PreemptionSlot> runSlotFinder(ScheduledTask pendingTask) {
    PreemptionSlotFinder slotFinder = new PreemptionSlotFinderImpl(
        offerManager,
        clusterState,
        schedulingFilter,
        TaskExecutors.NO_OVERHEAD_EXECUTOR,
        preemptorMetrics);

    return slotFinder.findPreemptionSlotFor(
        IAssignedTask.build(pendingTask.getAssignedTask()),
        EMPTY,
        storageUtil.mutableStoreProvider);
  }

  @Test
  public void testPreempted() throws Exception {
    setUpHost();

    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A);
    assignToHost(lowPriority);

    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 100);

    expectNoOffers();
    expectGetClusterState(lowPriority);
    expectFiltering();

    control.replay();
    assertSlot(runSlotFinder(highPriority), lowPriority);
  }

  @Test
  public void testLowestPriorityPreempted() throws Exception {
    setUpHost();

    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A, 10);
    assignToHost(lowPriority);

    ScheduledTask lowerPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 1);
    assignToHost(lowerPriority);

    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_C, 100);

    expectNoOffers();
    expectGetClusterState(lowerPriority, lowerPriority);
    expectFiltering();

    control.replay();
    assertSlot(runSlotFinder(highPriority), lowerPriority);
  }

  @Test
  public void testOnePreemptableTask() throws Exception {
    setUpHost();

    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_A, 100);
    assignToHost(highPriority);

    ScheduledTask lowerPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 99);
    assignToHost(lowerPriority);

    ScheduledTask lowestPriority = makeTask(USER_A, JOB_A, TASK_ID_C, 1);
    assignToHost(lowestPriority);

    ScheduledTask pendingPriority = makeTask(USER_A, JOB_A, TASK_ID_D, 98);

    expectNoOffers();
    expectGetClusterState(highPriority, lowerPriority, lowestPriority);
    expectFiltering();

    control.replay();
    assertSlot(runSlotFinder(pendingPriority), lowestPriority);
  }

  @Test
  public void testHigherPriorityRunning() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 100);
    assignToHost(highPriority);

    ScheduledTask task = makeTask(USER_A, JOB_A, TASK_ID_A);

    expectNoOffers();
    expectGetClusterState(highPriority);

    control.replay();
    assertNoSlot(runSlotFinder(task));
  }

  @Test
  public void testProductionPreemptingNonproduction() throws Exception {
    setUpHost();

    schedulingFilter = createMock(SchedulingFilter.class);
    // Use a very low priority for the production task to show that priority is irrelevant.
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", -1000);
    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_B + "_a1", 100);
    assignToHost(a1);

    expectNoOffers();
    expectGetClusterState(a1);
    expectFiltering();

    control.replay();
    assertSlot(runSlotFinder(p1), a1);
  }

  @Test
  public void testProductionPreemptingNonproductionAcrossUsers() throws Exception {
    setUpHost();

    schedulingFilter = createMock(SchedulingFilter.class);
    // Use a very low priority for the production task to show that priority is irrelevant.
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", -1000);
    ScheduledTask a1 = makeTask(USER_B, JOB_A, TASK_ID_B + "_a1", 100);
    assignToHost(a1);

    expectNoOffers();
    expectGetClusterState(a1);
    expectFiltering();

    control.replay();
    assertSlot(runSlotFinder(p1), a1);
  }

  @Test
  public void testProductionUsersDoNotPreemptEachOther() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", 1000);
    ScheduledTask a1 = makeProductionTask(USER_B, JOB_A, TASK_ID_B + "_a1", 0);
    assignToHost(a1);

    expectNoOffers();
    expectGetClusterState(a1);

    control.replay();
    assertNoSlot(runSlotFinder(p1));
  }

  // Ensures a production task can preempt 2 tasks on the same host.
  @Test
  public void testProductionPreemptingManyNonProduction() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(TaskExecutors.NO_OVERHEAD_EXECUTOR);
    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    a1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    ScheduledTask b1 = makeTask(USER_B, JOB_B, TASK_ID_B + "_b1");
    b1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    setUpHost();

    assignToHost(a1);
    assignToHost(b1);

    ScheduledTask p1 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1");
    p1.getAssignedTask().getTask().setNumCpus(2).setRamMb(1024);

    expectNoOffers();

    expectGetClusterState(a1, b1);

    control.replay();
    assertSlot(runSlotFinder(p1), a1, b1);
  }

  // Ensures we select the minimal number of tasks to preempt
  @Test
  public void testMinimalSetPreempted() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(TaskExecutors.NO_OVERHEAD_EXECUTOR);
    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    a1.getAssignedTask().getTask().setNumCpus(4).setRamMb(4096);

    ScheduledTask b1 = makeTask(USER_B, JOB_B, TASK_ID_B + "_b1");
    b1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    ScheduledTask b2 = makeTask(USER_B, JOB_B, TASK_ID_B + "_b2");
    b2.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    setUpHost();

    assignToHost(a1);
    assignToHost(b1);
    assignToHost(b2);

    ScheduledTask p1 = makeProductionTask(USER_C, JOB_C, TASK_ID_C + "_p1");
    p1.getAssignedTask().getTask().setNumCpus(2).setRamMb(1024);

    expectNoOffers();
    expectGetClusterState(b1, b2, a1);

    control.replay();
    assertSlot(runSlotFinder(p1), a1);
  }

  // Ensures a production task *never* preempts a production task from another job.
  @Test
  public void testProductionJobNeverPreemptsProductionJob() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(TaskExecutors.NO_OVERHEAD_EXECUTOR);
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1");
    p1.getAssignedTask().getTask().setNumCpus(2).setRamMb(1024);

    setUpHost();

    assignToHost(p1);

    ScheduledTask p2 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p2");
    p2.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    expectNoOffers();

    expectGetClusterState(p1);

    control.replay();
    assertNoSlot(runSlotFinder(p2));
  }

  // Ensures that we can preempt if a task + offer can satisfy a pending task.
  @Test
  public void testPreemptWithOfferAndTask() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(TaskExecutors.NO_OVERHEAD_EXECUTOR);

    setUpHost();

    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    a1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);
    assignToHost(a1);

    Offer o1 = makeOffer(OFFER, 1, Amount.of(512L, Data.MB), Amount.of(1L, Data.MB), 1);
    expectOffers(o1);

    ScheduledTask p1 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1");
    p1.getAssignedTask().getTask().setNumCpus(2).setRamMb(1024);

    expectGetClusterState(a1);

    control.replay();
    assertSlot(runSlotFinder(p1), a1);
  }

  // Ensures we can preempt if two tasks and an offer can satisfy a pending task.
  @Test
  public void testPreemptWithOfferAndMultipleTasks() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(TaskExecutors.NO_OVERHEAD_EXECUTOR);

    setUpHost();

    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    a1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);
    assignToHost(a1);

    ScheduledTask a2 = makeTask(USER_A, JOB_B, TASK_ID_A + "_a2");
    a2.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);
    assignToHost(a2);

    Offer o1 = makeOffer(OFFER, 2, Amount.of(1024L, Data.MB), Amount.of(1L, Data.MB), 1);
    expectOffers(o1);

    ScheduledTask p1 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1");
    p1.getAssignedTask().getTask().setNumCpus(4).setRamMb(2048);

    expectGetClusterState(a1, a2);

    control.replay();
    assertSlot(runSlotFinder(p1), a1, a2);
  }

  @Test
  public void testPreemptionSlotValidation() {
    schedulingFilter = new SchedulingFilterImpl(TaskExecutors.NO_OVERHEAD_EXECUTOR);

    setUpHost();

    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    a1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);
    assignToHost(a1);

    Offer o1 = makeOffer(OFFER, 1, Amount.of(512L, Data.MB), Amount.of(1L, Data.MB), 1);
    expectOffers(o1);
    expect(offerManager.getOffer(Protos.SlaveID.newBuilder().setValue(SLAVE_ID).build()))
        .andReturn(Optional.of(Iterables.getOnlyElement(makeOffers(o1))));

    ScheduledTask p1 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1");
    p1.getAssignedTask().getTask().setNumCpus(2).setRamMb(1024);

    expectGetClusterState(a1);

    control.replay();

    Optional<PreemptionSlot> slot = runSlotFinder(p1);
    assertSlot(slot, a1);

    PreemptionSlotFinder slotFinder = new PreemptionSlotFinderImpl(
        offerManager,
        clusterState,
        schedulingFilter,
        TaskExecutors.NO_OVERHEAD_EXECUTOR,
        preemptorMetrics);

    Optional<ImmutableSet<PreemptionVictim>> victims = slotFinder.validatePreemptionSlotFor(
        IAssignedTask.build(p1.getAssignedTask()),
        EMPTY,
        slot.get(),
        storageUtil.mutableStoreProvider);

    assertEquals(
        Optional.of(ImmutableSet.of(PreemptionVictim.fromTask(
            IAssignedTask.build(a1.getAssignedTask())))),
        victims);
  }

  @Test
  public void testNoPreemptionVictims() {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask task = makeProductionTask(USER_A, JOB_A, TASK_ID_A);
    expect(clusterState.getSlavesToActiveTasks())
        .andReturn(ImmutableMultimap.<String, PreemptionVictim>of());

    control.replay();

    assertNoSlot(runSlotFinder(task));
  }

  @Test
  public void testMissingAttributes() {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask task = makeProductionTask(USER_A, JOB_A, TASK_ID_A);
    assignToHost(task);

    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    a1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);
    assignToHost(a1);

    expectGetClusterState(a1);

    Offer o1 = makeOffer(OFFER, 2, Amount.of(1024L, Data.MB), Amount.of(1L, Data.MB), 1);
    expectOffers(o1);

    expect(storageUtil.attributeStore.getHostAttributes(HOST))
        .andReturn(Optional.<IHostAttributes>absent());

    control.replay();

    assertNoSlot(runSlotFinder(task));
    assertEquals(1L, statsProvider.getLongValue(MISSING_ATTRIBUTES_NAME));
  }

  @Test
  public void testAllVictimsVetoed() {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask task = makeProductionTask(USER_A, JOB_A, TASK_ID_A);
    assignToHost(task);

    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    a1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);
    assignToHost(a1);

    expectGetClusterState(a1);

    Offer o1 = makeOffer(OFFER, 2, Amount.of(1024L, Data.MB), Amount.of(1L, Data.MB), 1);
    expectOffers(o1);

    setUpHost();
    expectFiltering(Optional.of(Veto.constraintMismatch("ban")));

    control.replay();

    assertNoSlot(runSlotFinder(task));
  }

  private static void assertSlot(Optional<PreemptionSlot> actual, ScheduledTask... tasks) {
    ImmutableSet<PreemptionVictim> victims = FluentIterable.from(ImmutableSet.copyOf(tasks))
        .transform(
            new Function<ScheduledTask, PreemptionVictim>() {
              @Override
              public PreemptionVictim apply(ScheduledTask task) {
                return PreemptionVictim.fromTask(IAssignedTask.build(task.getAssignedTask()));
              }
            }).toSet();

    assertEquals(new PreemptionSlot(victims, SLAVE_ID), actual.get());
  }

  private static void assertNoSlot(Optional<PreemptionSlot> actual) {
    assertEquals(Optional.<PreemptionSlot>absent(), actual);
  }

  private static final Function<ScheduledTask, String> GET_SLAVE_ID =
      new Function<ScheduledTask, String>() {
        @Override
        public String apply(ScheduledTask task) {
          return task.getAssignedTask().getSlaveId();
        }
      };

  private void expectGetClusterState(ScheduledTask... returnedTasks) {
    Multimap<String, PreemptionVictim> state = Multimaps.transformValues(
        Multimaps.index(Arrays.asList(returnedTasks), GET_SLAVE_ID),
        new Function<ScheduledTask, PreemptionVictim>() {
          @Override
          public PreemptionVictim apply(ScheduledTask task) {
            return PreemptionVictim.fromTask(IAssignedTask.build(task.getAssignedTask()));
          }
        }
    );

    expect(clusterState.getSlavesToActiveTasks()).andReturn(state);
  }

  // TODO(zmanji) spread tasks across slave ids on the same host and see if preemption fails.

  private Offer makeOffer(
      String offerId,
      double cpu,
      Amount<Long, Data> ram,
      Amount<Long, Data> disk,
      int numPorts) {

    List<Resource> resources = new Resources(cpu, ram, disk, numPorts).toResourceList();
    Offer.Builder builder = Offer.newBuilder();
    builder.getIdBuilder().setValue(offerId);
    builder.getFrameworkIdBuilder().setValue("framework-id");
    builder.getSlaveIdBuilder().setValue(SLAVE_ID);
    builder.setHostname(HOST);
    for (Resource r: resources) {
      builder.addResources(r);
    }
    return builder.build();
  }

  private Iterable<HostOffer> makeOffers(Offer... offers) {
    return FluentIterable.from(Lists.newArrayList(offers))
        .transform(new Function<Offer, HostOffer>() {
          @Override
          public HostOffer apply(Offer offer) {
            return new HostOffer(
                offer,
                IHostAttributes.build(new HostAttributes().setMode(MaintenanceMode.NONE)));
          }
        });
  }

  private void expectOffers(Offer... offers) {
    expect(offerManager.getOffers()).andReturn(makeOffers(offers));
  }

  private void expectNoOffers() {
    expect(offerManager.getOffers()).andReturn(ImmutableList.<HostOffer>of());
  }

  private IExpectationSetters<Set<SchedulingFilter.Veto>> expectFiltering() {
    return expectFiltering(Optional.<Veto>absent());
  }

  private IExpectationSetters<Set<SchedulingFilter.Veto>> expectFiltering(
      final Optional<Veto> veto) {

    return expect(schedulingFilter.filter(
        EasyMock.<SchedulingFilter.UnusedResource>anyObject(),
        EasyMock.<SchedulingFilter.ResourceRequest>anyObject()))
        .andAnswer(
            new IAnswer<Set<SchedulingFilter.Veto>>() {
              @Override
              public Set<SchedulingFilter.Veto> answer() {
                return veto.asSet();
              }
            });
  }

  static ScheduledTask makeTask(String role, String job, String taskId, int priority,
                                String env, boolean production) {
    AssignedTask assignedTask = new AssignedTask()
        .setTaskId(taskId)
        .setTask(new TaskConfig()
            .setJob(new JobKey(role, env, job))
            .setPriority(priority)
            .setProduction(production)
            .setJobName(job)
            .setEnvironment(env)
            .setConstraints(new HashSet<Constraint>()));
    ScheduledTask scheduledTask = new ScheduledTask()
        .setAssignedTask(assignedTask);
    return scheduledTask;
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
    task.getAssignedTask().setSlaveHost(HOST);
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
        new HostAttributes().setHost(HOST).setSlaveId(HOST + "_id")
            .setMode(NONE).setAttributes(ImmutableSet.of(rack(RACK), host(RACK))));

    expect(storageUtil.attributeStore.getHostAttributes(HOST))
        .andReturn(Optional.of(hostAttrs)).anyTimes();
  }
}
