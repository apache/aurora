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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilterImpl;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.mem.MemStorage;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.THROTTLED;
import static org.apache.aurora.scheduler.async.Preemptor.PreemptorImpl;
import static org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import static org.apache.mesos.Protos.Offer;
import static org.apache.mesos.Protos.Resource;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class PreemptorImplTest extends EasyMockTest {

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
  private static final String HOST_A = "host_a";
  private static final String RACK_A = "rackA";
  private static final String RACK_ATTRIBUTE = "rack";
  private static final String HOST_ATTRIBUTE = "host";
  private static final String OFFER_A = "offer_a";

  private static final Amount<Long, Time> PREEMPTION_DELAY = Amount.of(30L, Time.SECONDS);

  private StorageTestUtil storageUtil;
  private StateManager stateManager;
  private SchedulingFilter schedulingFilter;
  private FakeClock clock;
  private StatsProvider statsProvider;
  private OfferQueue offerQueue;
  private AttributeAggregate emptyJob;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    stateManager = createMock(StateManager.class);
    clock = new FakeClock();
    statsProvider = new FakeStatsProvider();
    offerQueue = createMock(OfferQueue.class);
    emptyJob = new AttributeAggregate(
        Suppliers.ofInstance(ImmutableSet.<IScheduledTask>of()),
        createMock(AttributeStore.class));
  }

  private void runPreemptor(ScheduledTask pendingTask) {
    PreemptorImpl preemptor = new PreemptorImpl(
        storageUtil.storage,
        stateManager,
        offerQueue,
        schedulingFilter,
        PREEMPTION_DELAY,
        clock,
        statsProvider);

    preemptor.findPreemptionSlotFor(pendingTask.getAssignedTask().getTaskId(), emptyJob);
  }

  // TODO(zmanji): Put together a SchedulerPreemptorIntegrationTest as well.

  private void expectGetPendingTasks(ScheduledTask... returnedTasks) {
    Iterable<String> taskIds = FluentIterable.from(Arrays.asList(returnedTasks))
        .transform(IScheduledTask.FROM_BUILDER)
        .transform(Tasks.SCHEDULED_TO_ID);
    storageUtil.expectTaskFetch(
        Query.statusScoped(PENDING).byId(taskIds),
        IScheduledTask.setFromBuilders(Arrays.asList(returnedTasks)));
  }

  private void expectGetActiveTasks(ScheduledTask... returnedTasks) {
    storageUtil.expectTaskFetch(
        PreemptorImpl.CANDIDATE_QUERY,
        IScheduledTask.setFromBuilders(Arrays.asList(returnedTasks)));
  }

  @Test
  public void testPreempted() throws Exception {
    setUpHost(HOST_A, RACK_A);

    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A);
    runOnHost(lowPriority, HOST_A);

    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 100);
    clock.advance(PREEMPTION_DELAY);

    expectNoOffers();

    expectGetPendingTasks(highPriority);
    expectGetActiveTasks(lowPriority);

    expectFiltering();
    expectPreempted(lowPriority);

    control.replay();
    runPreemptor(highPriority);
  }

  @Test
  public void testLowestPriorityPreempted() throws Exception {
    setUpHost(HOST_A, RACK_A);

    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A, 10);
    runOnHost(lowPriority, HOST_A);

    ScheduledTask lowerPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 1);
    runOnHost(lowerPriority, HOST_A);

    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_C, 100);
    clock.advance(PREEMPTION_DELAY);

    expectNoOffers();

    expectGetPendingTasks(highPriority);
    expectGetActiveTasks(lowerPriority, lowerPriority);

    expectFiltering();
    expectPreempted(lowerPriority);

    control.replay();
    runPreemptor(highPriority);
  }

  @Test
  public void testOnePreemptableTask() throws Exception {
    setUpHost(HOST_A, RACK_A);

    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_A, 100);
    runOnHost(highPriority, HOST_A);

    ScheduledTask lowerPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 99);
    runOnHost(lowerPriority, HOST_A);

    ScheduledTask lowestPriority = makeTask(USER_A, JOB_A, TASK_ID_C, 1);
    runOnHost(lowestPriority, HOST_A);

    ScheduledTask pendingPriority = makeTask(USER_A, JOB_A, TASK_ID_D, 98);
    clock.advance(PREEMPTION_DELAY);

    expectNoOffers();

    expectGetPendingTasks(pendingPriority);
    expectGetActiveTasks(highPriority, lowerPriority, lowestPriority);

    expectFiltering();
    expectPreempted(lowestPriority);

    control.replay();
    runPreemptor(pendingPriority);
  }

  @Test
  public void testHigherPriorityRunning() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 100);
    runOnHost(highPriority, HOST_A);

    ScheduledTask task = makeTask(USER_A, JOB_A, TASK_ID_A);
    clock.advance(PREEMPTION_DELAY);

    expectNoOffers();

    expectGetPendingTasks(task);
    expectGetActiveTasks(highPriority);

    control.replay();
    runPreemptor(task);
  }

  @Test
  public void testProductionPreemptingNonproduction() throws Exception {
    setUpHost(HOST_A, RACK_A);

    schedulingFilter = createMock(SchedulingFilter.class);
    // Use a very low priority for the production task to show that priority is irrelevant.
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", -1000);
    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_B + "_a1", 100);
    runOnHost(a1, HOST_A);

    clock.advance(PREEMPTION_DELAY);

    expectNoOffers();

    expectGetPendingTasks(p1);
    expectGetActiveTasks(a1);

    expectFiltering();
    expectPreempted(a1);

    control.replay();
    runPreemptor(p1);
  }

  @Test
  public void testProductionPreemptingNonproductionAcrossUsers() throws Exception {
    setUpHost(HOST_A, RACK_A);

    schedulingFilter = createMock(SchedulingFilter.class);
    // Use a very low priority for the production task to show that priority is irrelevant.
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", -1000);
    ScheduledTask a1 = makeTask(USER_B, JOB_A, TASK_ID_B + "_a1", 100);
    runOnHost(a1, HOST_A);

    clock.advance(PREEMPTION_DELAY);

    expectNoOffers();

    expectGetPendingTasks(p1);
    expectGetActiveTasks(a1);

    expectFiltering();
    expectPreempted(a1);

    control.replay();
    runPreemptor(p1);
  }

  @Test
  public void testProductionUsersDoNotPreemptEachOther() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", 1000);
    ScheduledTask a1 = makeProductionTask(USER_B, JOB_A, TASK_ID_B + "_a1", 0);
    runOnHost(a1, HOST_A);

    clock.advance(PREEMPTION_DELAY);

    expectNoOffers();

    expectGetPendingTasks(p1);
    expectGetActiveTasks(a1);

    control.replay();
    runPreemptor(p1);
  }

  // Ensures a production task can preempt 2 tasks on the same host.
  @Test
  public void testProductionPreemptingManyNonProduction() throws Exception {
    schedulingFilter = new SchedulingFilterImpl();
    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    a1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    ScheduledTask b1 = makeTask(USER_B, JOB_B, TASK_ID_B + "_b1");
    b1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    setUpHost(HOST_A, RACK_A);

    runOnHost(a1, HOST_A);
    runOnHost(b1, HOST_A);

    ScheduledTask p1 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1");
    p1.getAssignedTask().getTask().setNumCpus(2).setRamMb(1024);

    clock.advance(PREEMPTION_DELAY);

    expectNoOffers();

    expectGetPendingTasks(p1);
    expectGetActiveTasks(a1, b1);

    expectPreempted(a1);
    expectPreempted(b1);

    control.replay();
    runPreemptor(p1);
  }

  // Ensures we select the minimal number of tasks to preempt
  @Test
  public void testMinimalSetPreempted() throws Exception {
    schedulingFilter = new SchedulingFilterImpl();
    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    a1.getAssignedTask().getTask().setNumCpus(4).setRamMb(4096);

    ScheduledTask b1 = makeTask(USER_B, JOB_B, TASK_ID_B + "_b1");
    b1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    ScheduledTask b2 = makeTask(USER_B, JOB_B, TASK_ID_B + "_b2");
    b2.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    setUpHost(HOST_A, RACK_A);

    runOnHost(a1, HOST_A);
    runOnHost(b1, HOST_A);
    runOnHost(b2, HOST_A);

    ScheduledTask p1 = makeProductionTask(USER_C, JOB_C, TASK_ID_C + "_p1");
    p1.getAssignedTask().getTask().setNumCpus(2).setRamMb(1024);

    clock.advance(PREEMPTION_DELAY);

    expectNoOffers();

    expectGetPendingTasks(p1);
    expectGetActiveTasks(b1, b2, a1);

    expectPreempted(a1);

    control.replay();
    runPreemptor(p1);
  }

  // Ensures a production task *never* preempts a production task from another job.
  @Test
  public void testProductionJobNeverPreemptsProductionJob() throws Exception {
    schedulingFilter = new SchedulingFilterImpl();
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1");
    p1.getAssignedTask().getTask().setNumCpus(2).setRamMb(1024);

    setUpHost(HOST_A, RACK_A);

    runOnHost(p1, HOST_A);

    ScheduledTask p2 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p2");
    p2.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    clock.advance(PREEMPTION_DELAY);

    expectNoOffers();

    expectGetActiveTasks(p1);
    expectGetPendingTasks(p2);

    control.replay();
    runPreemptor(p2);
  }

  // Ensures that we can preempt if a task + offer can satisfy a pending task.
  @Test
  public void testPreemptWithOfferAndTask() throws Exception {
    schedulingFilter = new SchedulingFilterImpl();

    setUpHost(HOST_A, RACK_A);

    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    a1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);
    runOnHost(a1, HOST_A);

    Offer o1 = makeOffer(OFFER_A, HOST_A, 1, Amount.of(512L, Data.MB), Amount.of(1L, Data.MB), 1);
    expectOffers(o1);

    ScheduledTask p1 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1");
    p1.getAssignedTask().getTask().setNumCpus(2).setRamMb(1024);

    clock.advance(PREEMPTION_DELAY);

    expectGetActiveTasks(a1);
    expectGetPendingTasks(p1);

    expectPreempted(a1);

    control.replay();
    runPreemptor(p1);
  }

  // Ensures we can preempt if two tasks and an offer can satisfy a pending task.
  @Test
  public void testPreemptWithOfferAndMultipleTasks() throws Exception {
    schedulingFilter = new SchedulingFilterImpl();

    setUpHost(HOST_A, RACK_A);

    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    a1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);
    runOnHost(a1, HOST_A);

    ScheduledTask a2 = makeTask(USER_A, JOB_B, TASK_ID_A + "_a2");
    a2.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);
    runOnHost(a2, HOST_A);

    Offer o1 = makeOffer(OFFER_A, HOST_A, 2, Amount.of(1024L, Data.MB), Amount.of(1L, Data.MB), 1);
    expectOffers(o1);

    ScheduledTask p1 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1");
    p1.getAssignedTask().getTask().setNumCpus(4).setRamMb(2048);

    clock.advance(PREEMPTION_DELAY);

    expectGetActiveTasks(a1, a2);
    expectGetPendingTasks(p1);

    expectPreempted(a1);
    expectPreempted(a2);

    control.replay();
    runPreemptor(p1);
  }

  // Ensures we don't preempt if a host has enough slack to satisfy a pending task.
  @Test
  public void testPreemptWithLargeOffer() throws Exception {
    schedulingFilter = new SchedulingFilterImpl();

    setUpHost(HOST_A, RACK_A);

    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    a1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);
    runOnHost(a1, HOST_A);

    Offer o1 = makeOffer(OFFER_A, HOST_A, 2, Amount.of(2048L, Data.MB), Amount.of(1L, Data.MB), 1);
    expectOffers(o1);

    ScheduledTask p1 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1");
    p1.getAssignedTask().getTask().setNumCpus(1).setRamMb(1024);

    clock.advance(PREEMPTION_DELAY);

    expectGetActiveTasks(a1);
    expectGetPendingTasks(p1);

    control.replay();
    runPreemptor(p1);
  }

  @Test
  public void testIgnoresThrottledTasks() throws Exception {
    // Ensures that the preemptor does not consider a throttled task to be a preemption candidate.
    schedulingFilter = createMock(SchedulingFilter.class);

    Storage storage = MemStorage.newEmptyStorage();

    final ScheduledTask throttled = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1").setStatus(THROTTLED);
    throttled.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    final ScheduledTask pending = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1");
    pending.getAssignedTask().getTask().setNumCpus(1).setRamMb(1024);

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider store) {
        store.getUnsafeTaskStore().saveTasks(ImmutableSet.of(
            IScheduledTask.build(pending),
            IScheduledTask.build(throttled)));
      }
    });

    clock.advance(PREEMPTION_DELAY);

    control.replay();

    PreemptorImpl preemptor = new PreemptorImpl(
        storage,
        stateManager,
        offerQueue,
        schedulingFilter,
        PREEMPTION_DELAY,
        clock,
        statsProvider);

    assertEquals(
        Optional.<String>absent(),
        preemptor.findPreemptionSlotFor(pending.getAssignedTask().getTaskId(), emptyJob));
  }

  // TODO(zmanji) spread tasks across slave ids on the same host and see if preemption fails.

  private Offer makeOffer(String offerId,
                          String host,
                          double cpu,
                          Amount<Long, Data> ram,
                          Amount<Long, Data> disk,
                          int numPorts) {
    List<Resource> resources = new Resources(cpu, ram, disk, numPorts).toResourceList();
    Offer.Builder builder = Offer.newBuilder();
    builder.getIdBuilder().setValue(offerId);
    builder.getFrameworkIdBuilder().setValue("framework-id");
    builder.getSlaveIdBuilder().setValue(hostToId(host));
    builder.setHostname(host);
    for (Resource r: resources) {
      builder.addResources(r);
    }
    return builder.build();
  }

  private void expectOffers(Offer ... offers) {
    Iterable<HostOffer> hostOffers = FluentIterable.from(Lists.newArrayList(offers))
        .transform(new Function<Offer, HostOffer>() {
          @Override
          public HostOffer apply(Offer offer) {
            return new HostOffer(
                offer,
                IHostAttributes.build(new HostAttributes().setMode(MaintenanceMode.NONE)));
          }
        });
    expect(offerQueue.getOffers()).andReturn(hostOffers);
  }

  private void expectNoOffers() {
    expect(offerQueue.getOffers()).andReturn(ImmutableList.<HostOffer>of());
  }

  private IExpectationSetters<Set<Veto>> expectFiltering() {
    return expect(schedulingFilter.filter(
        EasyMock.<ResourceSlot>anyObject(),
        EasyMock.<IHostAttributes>anyObject(),
        EasyMock.<ITaskConfig>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.eq(emptyJob))).andAnswer(
        new IAnswer<Set<Veto>>() {
          @Override
          public Set<Veto> answer() {
            return ImmutableSet.of();
          }
        }
    );
  }

  private void expectPreempted(ScheduledTask preempted) throws Exception {
    expect(stateManager.changeState(
        eq(storageUtil.mutableStoreProvider),
        eq(Tasks.id(preempted)),
        eq(Optional.<ScheduleStatus>absent()),
        eq(ScheduleStatus.PREEMPTING),
        EasyMock.<Optional<String>>anyObject()))
        .andReturn(true);
  }

  private ScheduledTask makeProductionTask(String role, String job, String taskId) {
    return makeTask(role, job, taskId, 0, "prod", true);
  }

  private ScheduledTask makeProductionTask(String role, String job, String taskId, int priority) {
    return makeTask(role, job, taskId, priority, "prod", true);
  }

  private ScheduledTask makeTask(String role, String job, String taskId, int priority,
                                 String env, boolean production) {
    AssignedTask assignedTask = new AssignedTask()
        .setTaskId(taskId)
        .setTask(new TaskConfig()
            .setJob(new JobKey(role, env, job))
            .setOwner(new Identity(role, role))
            .setPriority(priority)
            .setProduction(production)
            .setJobName(job)
            .setEnvironment(env)
            .setConstraints(new HashSet<Constraint>()));
    ScheduledTask scheduledTask = new ScheduledTask()
        .setStatus(PENDING)
        .setAssignedTask(assignedTask);
    addEvent(scheduledTask, PENDING);
    return scheduledTask;
  }

  private ScheduledTask makeTask(String role, String job, String taskId) {
    return makeTask(role, job, taskId, 0, "dev", false);
  }

  private ScheduledTask makeTask(String role, String job, String taskId, int priority) {
    return makeTask(role, job, taskId, priority, "dev", false);
  }

  private void addEvent(ScheduledTask task, ScheduleStatus status) {
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), status));
  }

  // Slave Hostname to a slave id
  private String hostToId(String hostname) {
    return hostname + "_id";
  }

  private void runOnHost(ScheduledTask task, String host) {
    task.setStatus(RUNNING);
    addEvent(task, RUNNING);
    task.getAssignedTask().setSlaveHost(host);
    task.getAssignedTask().setSlaveId(hostToId(host));
  }

  private Attribute host(String host) {
    return new Attribute(HOST_ATTRIBUTE, ImmutableSet.of(host));
  }

  private Attribute rack(String rack) {
    return new Attribute(RACK_ATTRIBUTE, ImmutableSet.of(rack));
  }

  // Sets up a normal host, no dedicated hosts and no maintenance.
  private void setUpHost(String host, String rack) {
    IHostAttributes hostAttrs = IHostAttributes.build(
        new HostAttributes().setHost(host).setSlaveId(host + "_id")
            .setMode(NONE).setAttributes(ImmutableSet.of(rack(rack), host(host))));

    expect(this.storageUtil.attributeStore.getHostAttributes(host))
        .andReturn(Optional.of(hostAttrs)).anyTimes();
  }
}
