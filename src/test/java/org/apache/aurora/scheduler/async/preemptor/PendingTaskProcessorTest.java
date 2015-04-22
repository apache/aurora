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

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.async.OfferManager;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.mesos.Protos;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.async.preemptor.PreemptorMetrics.TASK_PROCESSOR_RUN_NAME;
import static org.apache.aurora.scheduler.async.preemptor.PreemptorMetrics.attemptsStatName;
import static org.apache.aurora.scheduler.async.preemptor.PreemptorMetrics.slotSearchStatName;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class PendingTaskProcessorTest extends EasyMockTest {
  private static final String CACHE_STAT = "cache_size";
  private static final String SLAVE_ID_1 = "slave_id_1";
  private static final String SLAVE_ID_2 = "slave_id_2";
  private static final JobKey JOB_A = new JobKey("role_a", "env", "job_a");
  private static final JobKey JOB_B = new JobKey("role_b", "env", "job_b");
  private static final ScheduledTask TASK_A = makeTask(JOB_A, SLAVE_ID_1, "id1");
  private static final ScheduledTask TASK_B = makeTask(JOB_B, SLAVE_ID_2, "id2");
  private static final PreemptionProposal SLOT_A = createPreemptionProposal(TASK_A, SLAVE_ID_1);
  private static final Amount<Long, Time> PREEMPTION_DELAY = Amount.of(30L, Time.SECONDS);
  private static final Amount<Long, Time> EXPIRATION = Amount.of(10L, Time.MINUTES);

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
        new BiCache.BiCacheSettings(EXPIRATION, CACHE_STAT),
        clock);

    slotFinder = new PendingTaskProcessor(
        storageUtil.storage,
        offerManager,
        preemptionVictimFilter,
        new PreemptorMetrics(new CachedCounters(statsProvider)),
        PREEMPTION_DELAY,
        slotCache,
        clusterState,
        clock);
  }
  @Test
  public void testSearchSlotSuccessful() throws Exception {
    expectGetPendingTasks(TASK_A, TASK_B);
    expectGetClusterState(TASK_A, TASK_B);
    HostOffer offer1 = makeOffer(SLAVE_ID_1);
    HostOffer offer2 = makeOffer(SLAVE_ID_2);
    expectOffers(offer1, offer2);
    expectSlotSearch(TASK_A, offer1, TASK_A);
    expectSlotSearch(TASK_B, offer2, TASK_B);

    control.replay();

    clock.advance(PREEMPTION_DELAY);

    slotFinder.run();
    assertEquals(1L, statsProvider.getLongValue(TASK_PROCESSOR_RUN_NAME));
    assertEquals(2L, statsProvider.getLongValue(attemptsStatName(true)));
    assertEquals(2L, statsProvider.getLongValue(slotSearchStatName(true, true)));
    assertEquals(0L, statsProvider.getLongValue(slotSearchStatName(false, true)));
    assertEquals(2L, statsProvider.getLongValue(CACHE_STAT));
  }

  @Test
  public void testSearchSlotFailed() throws Exception {
    expectGetPendingTasks(TASK_A);
    expectGetClusterState(TASK_A);
    HostOffer offer1 = makeOffer(SLAVE_ID_1);
    expectOffers(offer1);
    expectSlotSearch(TASK_A, offer1);

    control.replay();

    clock.advance(PREEMPTION_DELAY);

    slotFinder.run();
    assertEquals(1L, statsProvider.getLongValue(TASK_PROCESSOR_RUN_NAME));
    assertEquals(1L, statsProvider.getLongValue(attemptsStatName(true)));
    assertEquals(0L, statsProvider.getLongValue(slotSearchStatName(true, true)));
    assertEquals(1L, statsProvider.getLongValue(slotSearchStatName(false, true)));
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
  }

  @Test
  public void testMultipleTaskGroups() throws Exception {
    ScheduledTask task1 = makeTask(JOB_A, "1");
    ScheduledTask task2 = makeTask(JOB_A, "2");
    ScheduledTask task3 = makeTask(JOB_A, "3");
    ScheduledTask task4 = makeTask(JOB_B, "4");
    ScheduledTask task5 = makeTask(JOB_B, "5");

    expectGetPendingTasks(task1, task4, task2, task5, task3);
    expectGetClusterState(TASK_A, TASK_B);

    HostOffer offer1 = makeOffer(SLAVE_ID_1);
    HostOffer offer2 = makeOffer(SLAVE_ID_2);
    expectOffers(offer1, offer2);
    expectSlotSearch(task1, offer1);
    expectSlotSearch(task4, offer1, TASK_B);
    expectSlotSearch(task5, offer2, TASK_B);
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
    assertEquals(1L, statsProvider.getLongValue(slotSearchStatName(false, true)));
    assertEquals(2L, statsProvider.getLongValue(CACHE_STAT));
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
  }

  private static final Function<ScheduledTask, String> GET_SLAVE_ID =
      new Function<ScheduledTask, String>() {
        @Override
        public String apply(ScheduledTask task) {
          return task.getAssignedTask().getSlaveId();
        }
      };

  private Multimap<String, PreemptionVictim> getVictims(ScheduledTask... tasks) {
    return Multimaps.transformValues(
        Multimaps.index(Arrays.asList(tasks), GET_SLAVE_ID),
        new Function<ScheduledTask, PreemptionVictim>() {
          @Override
          public PreemptionVictim apply(ScheduledTask task) {
            return PreemptionVictim.fromTask(IAssignedTask.build(task.getAssignedTask()));
          }
        }
    );
  }

  private HostOffer makeOffer(String slaveId) {
    Protos.Offer.Builder builder = Protos.Offer.newBuilder();
    builder.getIdBuilder().setValue("id");
    builder.getFrameworkIdBuilder().setValue("framework-id");
    builder.getSlaveIdBuilder().setValue(slaveId);
    builder.setHostname(slaveId);
    return new HostOffer(
        builder.build(),
        IHostAttributes.build(new HostAttributes().setMode(MaintenanceMode.NONE)));
  }

  private void expectOffers(HostOffer... offers) {
    expect(offerManager.getOffers()).andReturn(ImmutableSet.copyOf(offers));
  }

  private void expectGetClusterState(ScheduledTask... returnedTasks) {
    expect(clusterState.getSlavesToActiveTasks()).andReturn(getVictims(returnedTasks));
  }

  private void expectSlotSearch(ScheduledTask task, HostOffer offer, ScheduledTask... victims) {
    expect(preemptionVictimFilter.filterPreemptionVictims(
        eq(ITaskConfig.build(task.getAssignedTask().getTask())),
        EasyMock.<Iterable<PreemptionVictim>>anyObject(),
        anyObject(AttributeAggregate.class),
        eq(Optional.of(offer)),
        eq(storageUtil.storeProvider))).andReturn(
        victims.length == 0
            ? Optional.<ImmutableSet<PreemptionVictim>>absent()
            : Optional.of(ImmutableSet.copyOf(getVictims(victims).values())));
  }

  private static PreemptionProposal createPreemptionProposal(ScheduledTask task, String slaveId) {
    IAssignedTask assigned = IAssignedTask.build(task.getAssignedTask());
    return new PreemptionProposal(ImmutableSet.of(PreemptionVictim.fromTask(assigned)), slaveId);
  }

  private static ScheduledTask makeTask(JobKey key, String taskId) {
    return makeTask(key, null, taskId);
  }

  private static TaskGroupKey group(ScheduledTask task) {
    return TaskGroupKey.from(ITaskConfig.build(task.getAssignedTask().getTask()));
  }

  private static ScheduledTask makeTask(JobKey key, @Nullable String slaveId, String taskId) {
    ScheduledTask task = new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setSlaveId(slaveId)
            .setTaskId(taskId)
            .setTask(new TaskConfig()
                .setPriority(1)
                .setProduction(true)
                .setJob(key)));
    task.addToTaskEvents(new TaskEvent(0, PENDING));
    return task;
  }

  private void expectGetPendingTasks(ScheduledTask... returnedTasks) {
    storageUtil.expectTaskFetch(
        Query.statusScoped(PENDING),
        IScheduledTask.setFromBuilders(Arrays.asList(returnedTasks)));
  }
}
