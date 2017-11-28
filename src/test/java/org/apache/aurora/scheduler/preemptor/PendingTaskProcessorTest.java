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

import java.util.Arrays;

import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.mesos.v1.Protos;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.preemptor.PreemptorMetrics.TASK_PROCESSOR_RUN_NAME;
import static org.apache.aurora.scheduler.preemptor.PreemptorMetrics.UNMATCHED_TASKS;
import static org.apache.aurora.scheduler.preemptor.PreemptorMetrics.attemptsStatName;
import static org.apache.aurora.scheduler.preemptor.PreemptorMetrics.slotSearchStatName;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PendingTaskProcessorTest extends EasyMockTest {
  private static final String CACHE_NAME = "TEST";
  private static final String CACHE_SIZE_STAT_NAME = "TEST_cache_size";
  private static final String SLAVE_ID_1 = "slave_id_1";
  private static final String SLAVE_ID_2 = "slave_id_2";
  private static final IJobKey JOB_A = JobKeys.from("role_a", "env", "job_a");
  private static final IJobKey JOB_B = JobKeys.from("role_b", "env", "job_b");
  private static final IJobKey JOB_C = JobKeys.from("role_c", "env", "job_c");
  private static final IScheduledTask TASK_A = makeTask(JOB_A, SLAVE_ID_1, "id1");
  private static final IScheduledTask TASK_B = makeTask(JOB_B, SLAVE_ID_2, "id2");
  private static final IScheduledTask TASK_C = makeTask(JOB_C, SLAVE_ID_2, "id3");
  private static final PreemptionProposal SLOT_A = createPreemptionProposal(TASK_A, SLAVE_ID_1);
  private static final Amount<Long, Time> PREEMPTION_DELAY = Amount.of(30L, Time.SECONDS);
  private static final Amount<Long, Time> EXPIRATION = Amount.of(10L, Time.MINUTES);
  private static final Integer RESERVATION_BATCH_SIZE = 5;

  private StorageTestUtil storageUtil;
  private OfferManager offerManager;
  private FakeStatsProvider statsProvider;
  private PreemptionVictimFilter preemptionVictimFilter;
  private PendingTaskProcessor slotFinder;
  private BiCache<PreemptionProposal, TaskGroupKey> slotCache;
  private ClusterState clusterState;
  private FakeClock clock;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    offerManager = createMock(OfferManager.class);
    preemptionVictimFilter = createMock(PreemptionVictimFilter.class);
    statsProvider = new FakeStatsProvider();
    clusterState = createMock(ClusterState.class);
    clock = new FakeClock();
    slotCache = new BiCache<>(
        statsProvider,
        new BiCache.BiCacheSettings(EXPIRATION, CACHE_NAME),
        clock);

    slotFinder = new PendingTaskProcessor(
        storageUtil.storage,
        offerManager,
        preemptionVictimFilter,
        new PreemptorMetrics(new CachedCounters(statsProvider)),
        PREEMPTION_DELAY,
        slotCache,
        clusterState,
        clock,
        RESERVATION_BATCH_SIZE);
  }

  @Test
  public void testSearchSlotSuccessful() throws Exception {
    expectGetPendingTasks(TASK_A, TASK_B);
    expectGetClusterState(TASK_A, TASK_B);
    HostOffer offer1 = makeOffer(SLAVE_ID_1);
    HostOffer offer2 = makeOffer(SLAVE_ID_2);
    expectOffers(offer1, offer2);
    expectSlotSearch(TASK_A.getAssignedTask().getTask(), TASK_A);
    expectSlotSearch(TASK_B.getAssignedTask().getTask(), TASK_B);

    control.replay();

    clock.advance(PREEMPTION_DELAY);

    slotFinder.run();
    assertEquals(1L, statsProvider.getLongValue(TASK_PROCESSOR_RUN_NAME));
    assertEquals(2L, statsProvider.getLongValue(attemptsStatName(true)));
    assertEquals(2L, statsProvider.getLongValue(slotSearchStatName(true, true)));
    assertEquals(0L, statsProvider.getLongValue(slotSearchStatName(false, true)));
    assertEquals(0L, statsProvider.getLongValue(UNMATCHED_TASKS));
    assertEquals(2L, statsProvider.getLongValue(CACHE_SIZE_STAT_NAME));
  }

  @Test
  public void testSearchSlotFailed() throws Exception {
    expectGetPendingTasks(TASK_A);
    expectGetClusterState(TASK_A);
    HostOffer offer1 = makeOffer(SLAVE_ID_1);
    expectOffers(offer1);
    expectSlotSearch(TASK_A.getAssignedTask().getTask());

    control.replay();

    clock.advance(PREEMPTION_DELAY);

    slotFinder.run();
    assertEquals(1L, statsProvider.getLongValue(TASK_PROCESSOR_RUN_NAME));
    assertEquals(1L, statsProvider.getLongValue(attemptsStatName(true)));
    assertEquals(0L, statsProvider.getLongValue(slotSearchStatName(true, true)));
    assertEquals(1L, statsProvider.getLongValue(slotSearchStatName(false, true)));
    assertEquals(1L, statsProvider.getLongValue(UNMATCHED_TASKS));
  }

  @Test
  public void testHasCachedSlots() throws Exception {
    slotCache.put(SLOT_A, group(TASK_A));
    expectGetPendingTasks(TASK_A);
    expectGetClusterState(TASK_A);
    HostOffer offer1 = makeOffer(SLAVE_ID_1);
    expectOffers(offer1);

    control.replay();

    clock.advance(PREEMPTION_DELAY);

    slotFinder.run();
    assertEquals(1L, statsProvider.getLongValue(TASK_PROCESSOR_RUN_NAME));
    assertEquals(0L, statsProvider.getLongValue(attemptsStatName(true)));
    assertEquals(0L, statsProvider.getLongValue(slotSearchStatName(true, true)));
    assertEquals(0L, statsProvider.getLongValue(slotSearchStatName(false, true)));
    assertEquals(0L, statsProvider.getLongValue(UNMATCHED_TASKS));
  }

  @Test
  public void testMultipleTaskGroups() throws Exception {
    IScheduledTask task1 = makeTask(JOB_A, "1");
    IScheduledTask task2 = makeTask(JOB_A, "2");
    IScheduledTask task3 = makeTask(JOB_A, "3");
    IScheduledTask task4 = makeTask(JOB_B, "4");
    IScheduledTask task5 = makeTask(JOB_B, "5");

    expectGetPendingTasks(task1, task4, task2, task5, task3);
    expectGetClusterState(TASK_A, TASK_B);

    HostOffer offer1 = makeOffer(SLAVE_ID_1);
    HostOffer offer2 = makeOffer(SLAVE_ID_2);
    expectOffers(offer1, offer2);
    expectSlotSearch(task1.getAssignedTask().getTask());
    expectSlotSearch(task4.getAssignedTask().getTask(), TASK_B);
    PreemptionProposal proposal1 = createPreemptionProposal(TASK_B, SLAVE_ID_1);
    PreemptionProposal proposal2 = createPreemptionProposal(TASK_B, SLAVE_ID_2);

    control.replay();

    clock.advance(PREEMPTION_DELAY);

    slotFinder.run();
    assertEquals(slotCache.get(proposal1), Optional.of(group(task4)));
    assertEquals(slotCache.get(proposal2), Optional.of(group(task5)));
    assertEquals(1L, statsProvider.getLongValue(TASK_PROCESSOR_RUN_NAME));
    assertEquals(3L, statsProvider.getLongValue(attemptsStatName(true)));
    assertEquals(2L, statsProvider.getLongValue(slotSearchStatName(true, true)));

    // TODO(wfarner): This test depends on the iteration order of a hash set (the set containing
    // task groups), and as a result this stat could be 0 or 2 depending on which group is
    // evaluated first.
    assertTrue(ImmutableSet.of(0L, 2L).contains(
        statsProvider.getLongValue(slotSearchStatName(false, true))));
    assertEquals(1L, statsProvider.getLongValue(UNMATCHED_TASKS));
    assertEquals(2L, statsProvider.getLongValue(CACHE_SIZE_STAT_NAME));
  }

  @Test
  public void testNoVictims() throws Exception {
    expectGetClusterState();
    control.replay();

    clock.advance(PREEMPTION_DELAY);

    slotFinder.run();
    assertEquals(1L, statsProvider.getLongValue(TASK_PROCESSOR_RUN_NAME));
    assertEquals(0L, statsProvider.getLongValue(attemptsStatName(true)));
    assertEquals(0L, statsProvider.getLongValue(slotSearchStatName(true, true)));
    assertEquals(0L, statsProvider.getLongValue(slotSearchStatName(false, true)));
    assertEquals(0L, statsProvider.getLongValue(UNMATCHED_TASKS));
  }

  @Test
  public void testGetPreemptionSequence() {
    TaskGroupKey a = group(TASK_A);
    TaskGroupKey b = group(TASK_B);
    TaskGroupKey c = group(TASK_C);

    control.replay();

    assertEquals(
        ImmutableList.of(),
        PendingTaskProcessor.getPreemptionSequence(ImmutableMultiset.of(), 2));
    assertEquals(
        ImmutableList.of(a),
        PendingTaskProcessor.getPreemptionSequence(ImmutableMultiset.of(a), 3));
    assertEquals(
        ImmutableList.of(a, a, a),
        PendingTaskProcessor.getPreemptionSequence(ImmutableMultiset.of(a, a, a), 5));
    assertEquals(
        ImmutableList.of(a, a, a),
        PendingTaskProcessor.getPreemptionSequence(ImmutableMultiset.of(a, a, a), 2));
    assertEquals(
        ImmutableList.of(a, b, a, b, a),
        PendingTaskProcessor.getPreemptionSequence(ImmutableMultiset.of(a, a, a, b, b), 1));
    assertEquals(
        ImmutableList.of(a, a, b, b, a),
        PendingTaskProcessor.getPreemptionSequence(ImmutableMultiset.of(a, a, a, b, b), 2));
    assertEquals(
        ImmutableList.of(a, a, a, b, b),
        PendingTaskProcessor.getPreemptionSequence(ImmutableMultiset.of(a, a, a, b, b), 3));
    assertEquals(
        ImmutableList.of(a, a, a, b, b),
        PendingTaskProcessor.getPreemptionSequence(ImmutableMultiset.of(a, a, a, b, b), 5));
    assertEquals(
        ImmutableList.of(a, a, b, b, a, a, a),
        PendingTaskProcessor.getPreemptionSequence(ImmutableMultiset.of(a, a, a, a, a, b, b), 2));
    assertEquals(
        ImmutableList.of(a, b, c, b, c),
        PendingTaskProcessor.getPreemptionSequence(ImmutableMultiset.of(a, b, b, c, c), 1));
    assertEquals(
        ImmutableList.of(a, b, b, c, c),
        PendingTaskProcessor.getPreemptionSequence(ImmutableMultiset.of(a, b, b, c, c), 2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPreemptionSequenceInvalidArgument() {
    control.replay();

    PendingTaskProcessor.getPreemptionSequence(ImmutableMultiset.of(), 0);
  }

  private Multimap<String, PreemptionVictim> getVictims(IScheduledTask... tasks) {
    return Multimaps.transformValues(
        Multimaps.index(Arrays.asList(tasks), task -> task.getAssignedTask().getSlaveId()),
        task -> PreemptionVictim.fromTask(task.getAssignedTask())
    );
  }

  private HostOffer makeOffer(String slaveId) {
    Protos.Offer.Builder builder = Protos.Offer.newBuilder();
    builder.getIdBuilder().setValue("id");
    builder.getFrameworkIdBuilder().setValue("framework-id");
    builder.getAgentIdBuilder().setValue(slaveId);
    builder.setHostname(slaveId);
    return new HostOffer(
        builder.build(),
        IHostAttributes.build(new HostAttributes().setMode(MaintenanceMode.NONE)));
  }

  private void expectOffers(HostOffer... offers) {
    expect(offerManager.getAll()).andReturn(ImmutableSet.copyOf(offers));
  }

  private void expectGetClusterState(IScheduledTask... returnedTasks) {
    expect(clusterState.getSlavesToActiveTasks()).andReturn(getVictims(returnedTasks));
  }

  private void expectSlotSearch(ITaskConfig config, IScheduledTask... victims) {
    expect(preemptionVictimFilter.filterPreemptionVictims(
        eq(config),
        anyObject(),
        anyObject(AttributeAggregate.class),
        anyObject(),
        eq(storageUtil.storeProvider)));
    expectLastCall().andReturn(
        victims.length == 0
            ? Optional.absent()
            : Optional.of(ImmutableSet.copyOf(getVictims(victims).values())))
        .anyTimes();
  }

  private static PreemptionProposal createPreemptionProposal(IScheduledTask task, String slaveId) {
    return new PreemptionProposal(
        ImmutableSet.of(PreemptionVictim.fromTask(task.getAssignedTask())),
        slaveId);
  }

  private static IScheduledTask makeTask(IJobKey key, String taskId) {
    return makeTask(key, null, taskId);
  }

  private static TaskGroupKey group(IScheduledTask task) {
    return TaskGroupKey.from(task.getAssignedTask().getTask());
  }

  private static IScheduledTask makeTask(IJobKey key, @Nullable String slaveId, String taskId) {
    ScheduledTask task = new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setSlaveId(slaveId)
            .setTaskId(taskId)
            .setTask(new TaskConfig()
                .setPriority(1)
                .setProduction(true)
                .setJob(key.newBuilder())));
    task.addToTaskEvents(new TaskEvent(0, PENDING));
    return IScheduledTask.build(task);
  }

  private void expectGetPendingTasks(IScheduledTask... returnedTasks) {
    storageUtil.expectTaskFetch(Query.statusScoped(PENDING), ImmutableSet.copyOf(returnedTasks));
  }
}
