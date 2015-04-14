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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.async.preemptor.PreemptionSlotFinder.PreemptionSlot;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.async.preemptor.PreemptorMetrics.PENDING_PROCESSOR_RUN_NAME;
import static org.apache.aurora.scheduler.async.preemptor.PreemptorMetrics.attemptsStatName;
import static org.apache.aurora.scheduler.async.preemptor.PreemptorMetrics.slotSearchStatName;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class PendingTaskProcessorTest extends EasyMockTest {
  private static final String TASK_ID_A = "task_a";
  private static final String TASK_ID_B = "task_b";
  private static final ScheduledTask TASK_A = makeTask(TASK_ID_A);
  private static final ScheduledTask TASK_B = makeTask(TASK_ID_B);
  private static final PreemptionSlot SLOT_A = createPreemptionSlot(TASK_A);
  private static final PreemptionSlot SLOT_B = createPreemptionSlot(TASK_B);
  private static final String SLAVE_ID = "slave_id";
  private static final IJobKey JOB_KEY = IJobKey.build(TASK_A.getAssignedTask().getTask().getJob());

  private static final Amount<Long, Time> PREEMPTION_DELAY = Amount.of(30L, Time.SECONDS);

  private static final Optional<PreemptionSlot> EMPTY_SLOT = Optional.absent();

  private StorageTestUtil storageUtil;
  private FakeStatsProvider statsProvider;
  private PreemptionSlotFinder preemptionSlotFinder;
  private PendingTaskProcessor slotFinder;
  private PreemptionSlotCache slotCache;
  private FakeClock clock;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    preemptionSlotFinder = createMock(PreemptionSlotFinder.class);
    slotCache = createMock(PreemptionSlotCache.class);
    statsProvider = new FakeStatsProvider();
    clock = new FakeClock();

    slotFinder = new PendingTaskProcessor(
        storageUtil.storage,
        preemptionSlotFinder,
        new PreemptorMetrics(new CachedCounters(statsProvider)),
        PREEMPTION_DELAY,
        slotCache,
        clock);
  }
  @Test
  public void testSearchSlotSuccessful() throws Exception {
    expect(slotCache.get(TASK_ID_A)).andReturn(EMPTY_SLOT);
    expect(slotCache.get(TASK_ID_B)).andReturn(EMPTY_SLOT);
    expectGetPendingTasks(TASK_A, TASK_B);
    expectAttributeAggegateFetchTasks();
    expectSlotSearch(TASK_A, Optional.of(SLOT_A));
    expectSlotSearch(TASK_B, Optional.of(SLOT_B));
    slotCache.add(TASK_ID_A, SLOT_A);
    slotCache.add(TASK_ID_B, SLOT_B);

    control.replay();

    clock.advance(PREEMPTION_DELAY);

    slotFinder.run();
    assertEquals(1L, statsProvider.getLongValue(PENDING_PROCESSOR_RUN_NAME));
    assertEquals(2L, statsProvider.getLongValue(attemptsStatName(true)));
    assertEquals(2L, statsProvider.getLongValue(slotSearchStatName(true, true)));
    assertEquals(0L, statsProvider.getLongValue(slotSearchStatName(false, true)));
  }

  @Test
  public void testSearchSlotFailed() throws Exception {
    expect(slotCache.get(TASK_ID_A)).andReturn(EMPTY_SLOT);
    expectGetPendingTasks(TASK_A);
    expectAttributeAggegateFetchTasks();
    expectSlotSearch(TASK_A, EMPTY_SLOT);

    control.replay();

    clock.advance(PREEMPTION_DELAY);

    slotFinder.run();
    assertEquals(1L, statsProvider.getLongValue(PENDING_PROCESSOR_RUN_NAME));
    assertEquals(1L, statsProvider.getLongValue(attemptsStatName(true)));
    assertEquals(0L, statsProvider.getLongValue(slotSearchStatName(true, true)));
    assertEquals(1L, statsProvider.getLongValue(slotSearchStatName(false, true)));
  }

  private void expectSlotSearch(ScheduledTask task, Optional<PreemptionSlot> slot) {
    expect(preemptionSlotFinder.findPreemptionSlotFor(
        IAssignedTask.build(task.getAssignedTask()),
        AttributeAggregate.EMPTY,
        storageUtil.storeProvider)).andReturn(slot);
  }

  private static PreemptionSlot createPreemptionSlot(ScheduledTask task) {
    IAssignedTask assigned = IAssignedTask.build(task.getAssignedTask());
    return new PreemptionSlot(ImmutableSet.of(PreemptionVictim.fromTask(assigned)), SLAVE_ID);
  }

  private static ScheduledTask makeTask(String taskId) {
    ScheduledTask task = new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTaskId(taskId)
            .setTask(new TaskConfig()
                .setPriority(1)
                .setProduction(true)
                .setJob(new JobKey("role", "env", "name"))));
    task.addToTaskEvents(new TaskEvent(0, PENDING));
    return task;
  }

  private IExpectationSetters<?> expectAttributeAggegateFetchTasks() {
    return storageUtil.expectTaskFetch(
        Query.jobScoped(JOB_KEY).byStatus(Tasks.SLAVE_ASSIGNED_STATES));
  }

  private void expectGetPendingTasks(ScheduledTask... returnedTasks) {
    storageUtil.expectTaskFetch(
        Query.statusScoped(PENDING),
        IScheduledTask.setFromBuilders(Arrays.asList(returnedTasks)));
  }
}
