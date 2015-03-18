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
import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.async.preemptor.PreemptionSlotFinder.PreemptionSlot;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.async.preemptor.PreemptorMetrics.successStatName;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class PreemptorImplTest extends EasyMockTest {
  private static final String TASK_ID = "task_a";
  private static final String SLAVE_ID = "slave_id";

  private static final Amount<Long, Time> PREEMPTION_DELAY = Amount.of(30L, Time.SECONDS);

  private StorageTestUtil storageUtil;
  private StateManager stateManager;
  private FakeStatsProvider statsProvider;
  private PreemptionSlotFinder preemptionSlotFinder;
  private PreemptorImpl preemptor;
  private AttributeAggregate attrAggregate;
  private FakeClock clock;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    stateManager = createMock(StateManager.class);
    preemptionSlotFinder = createMock(PreemptionSlotFinder.class);
    statsProvider = new FakeStatsProvider();
    clock = new FakeClock();
    attrAggregate = new AttributeAggregate(
        Suppliers.ofInstance(ImmutableSet.<IScheduledTask>of()),
        createMock(AttributeStore.class));

    preemptor = new PreemptorImpl(
        storageUtil.storage,
        stateManager,
        preemptionSlotFinder,
        new PreemptorMetrics(new CachedCounters(statsProvider)),
        PREEMPTION_DELAY,
        clock);
  }

  @Test
  public void testPreemption() throws Exception {
    ScheduledTask task = makeTask();

    expectGetPendingTasks(task);
    expect(preemptionSlotFinder.findPreemptionSlotFor(
        IAssignedTask.build(task.getAssignedTask()),
        attrAggregate,
        storageUtil.mutableStoreProvider)).andReturn(Optional.of(createPreemptionSlot(task)));

    expectPreempted(task);

    control.replay();

    clock.advance(PREEMPTION_DELAY);

    assertEquals(Optional.of(SLAVE_ID), preemptor.attemptPreemptionFor(TASK_ID, attrAggregate));
    assertEquals(1L, statsProvider.getLongValue(successStatName(true)));
  }

  @Test
  public void testNoPreemption() throws Exception {
    ScheduledTask task = makeTask();
    expectGetPendingTasks(task);
    expect(preemptionSlotFinder.findPreemptionSlotFor(
        IAssignedTask.build(task.getAssignedTask()),
        attrAggregate,
        storageUtil.mutableStoreProvider)).andReturn(Optional.<PreemptionSlot>absent());

    control.replay();

    clock.advance(PREEMPTION_DELAY);

    assertEquals(Optional.<String>absent(), preemptor.attemptPreemptionFor(TASK_ID, attrAggregate));
    assertEquals(0L, statsProvider.getLongValue(successStatName(true)));
  }

  @Test
  public void testNoPendingTasks() {
    storageUtil.expectTaskFetch(Query.statusScoped(PENDING).byId(TASK_ID));

    control.replay();

    assertEquals(Optional.<String>absent(), preemptor.attemptPreemptionFor(TASK_ID, attrAggregate));
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

  private static PreemptionSlot createPreemptionSlot(ScheduledTask task) {
    IAssignedTask assigned = IAssignedTask.build(task.getAssignedTask());
    return new PreemptionSlot(ImmutableSet.of(PreemptionVictim.fromTask(assigned)), SLAVE_ID);
  }

  private static ScheduledTask makeTask() {
    ScheduledTask task = new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTaskId(TASK_ID)
            .setTask(new TaskConfig()
                .setPriority(1)
                .setProduction(true)
                .setJob(new JobKey("role", "env", "name"))));
    task.addToTaskEvents(new TaskEvent(0, PENDING));
    return task;
  }

  private void expectGetPendingTasks(ScheduledTask... returnedTasks) {
    Iterable<String> taskIds = FluentIterable.from(Arrays.asList(returnedTasks))
        .transform(IScheduledTask.FROM_BUILDER)
        .transform(Tasks.SCHEDULED_TO_ID);
    storageUtil.expectTaskFetch(
        Query.statusScoped(PENDING).byId(taskIds),
        IScheduledTask.setFromBuilders(Arrays.asList(returnedTasks)));
  }
}
