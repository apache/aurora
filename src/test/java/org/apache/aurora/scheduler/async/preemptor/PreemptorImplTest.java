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

import com.google.common.base.Optional;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.async.preemptor.PreemptionSlotFinder.PreemptionSlot;
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

import static org.apache.aurora.scheduler.async.preemptor.PreemptorMetrics.successStatName;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class PreemptorImplTest extends EasyMockTest {
  private static final String TASK_ID = "task_a";
  private static final String SLAVE_ID = "slave_id";

  private StorageTestUtil storageUtil;
  private StateManager stateManager;
  private FakeStatsProvider statsProvider;
  private PreemptionSlotFinder preemptionSlotFinder;
  private PreemptorImpl preemptor;
  private AttributeAggregate attrAggregate;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    stateManager = createMock(StateManager.class);
    preemptionSlotFinder = createMock(PreemptionSlotFinder.class);
    statsProvider = new FakeStatsProvider();
    attrAggregate = new AttributeAggregate(
        Suppliers.ofInstance(ImmutableSet.<IScheduledTask>of()),
        createMock(AttributeStore.class));

    preemptor = new PreemptorImpl(
        storageUtil.storage,
        stateManager,
        preemptionSlotFinder,
        new PreemptorMetrics(new CachedCounters(statsProvider)));
  }

  @Test
  public void testPreemption() throws Exception {
    ScheduledTask task = makeTask();

    expect(preemptionSlotFinder.findPreemptionSlotFor(
        TASK_ID,
        attrAggregate,
        storageUtil.mutableStoreProvider)).andReturn(Optional.of(createPreemptionSlot(task)));

    expectPreempted(task);

    control.replay();

    assertEquals(Optional.of(SLAVE_ID), preemptor.attemptPreemptionFor(TASK_ID, attrAggregate));
    assertEquals(1L, statsProvider.getLongValue(successStatName(true)));
  }

  @Test
  public void testNoPreemption() throws Exception {
    expect(preemptionSlotFinder.findPreemptionSlotFor(
        TASK_ID,
        attrAggregate,
        storageUtil.mutableStoreProvider)).andReturn(Optional.<PreemptionSlot>absent());

    control.replay();

    assertEquals(Optional.<String>absent(), preemptor.attemptPreemptionFor(TASK_ID, attrAggregate));
    assertEquals(0L, statsProvider.getLongValue(successStatName(true)));
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
    return new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTaskId(TASK_ID)
            .setTask(new TaskConfig()
                .setPriority(1)
                .setProduction(true)
                .setJob(new JobKey("role", "env", "name"))));
  }
}
