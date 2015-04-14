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

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.async.preemptor.PreemptionSlotFinder.PreemptionSlot;
import org.apache.aurora.scheduler.async.preemptor.Preemptor.PreemptorImpl;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.async.preemptor.PreemptorMetrics.slotValidationStatName;
import static org.apache.aurora.scheduler.async.preemptor.PreemptorMetrics.successStatName;
import static org.apache.aurora.scheduler.filter.AttributeAggregate.EMPTY;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class PreemptorImplTest extends EasyMockTest {
  private static final String SLAVE_ID = "slave_id";
  private static final IScheduledTask TASK = IScheduledTask.build(makeTask());
  private static final PreemptionSlot SLOT = createPreemptionSlot(TASK);
  private static final TaskGroupKey GROUP_KEY =
      TaskGroupKey.from(ITaskConfig.build(makeTask().getAssignedTask().getTask()));

  private static final Set<PreemptionSlot> NO_SLOTS = ImmutableSet.of();
  private static final Optional<String> EMPTY_RESULT = Optional.absent();

  private StateManager stateManager;
  private FakeStatsProvider statsProvider;
  private PreemptionSlotFinder preemptionSlotFinder;
  private PreemptorImpl preemptor;
  private BiCache<PreemptionSlot, TaskGroupKey> slotCache;
  private Storage.MutableStoreProvider storeProvider;

  @Before
  public void setUp() {
    storeProvider = createMock(Storage.MutableStoreProvider.class);
    stateManager = createMock(StateManager.class);
    preemptionSlotFinder = createMock(PreemptionSlotFinder.class);
    slotCache = createMock(new Clazz<BiCache<PreemptionSlot, TaskGroupKey>>() { });
    statsProvider = new FakeStatsProvider();
    preemptor = new PreemptorImpl(
        stateManager,
        preemptionSlotFinder,
        new PreemptorMetrics(new CachedCounters(statsProvider)),
        slotCache);
  }

  @Test
  public void testPreemptTasksSuccessful() throws Exception {
    expect(slotCache.getByValue(GROUP_KEY)).andReturn(ImmutableSet.of(SLOT));
    slotCache.remove(SLOT, GROUP_KEY);
    expectSlotValidation(Optional.of(ImmutableSet.of(
        PreemptionVictim.fromTask(TASK.getAssignedTask()))));

    expectPreempted(TASK);

    control.replay();

    assertEquals(Optional.of(SLAVE_ID), callPreemptor());
    assertEquals(1L, statsProvider.getLongValue(slotValidationStatName(true)));
    assertEquals(1L, statsProvider.getLongValue(successStatName(true)));
  }

  @Test
  public void testPreemptTasksValidationFailed() throws Exception {
    expect(slotCache.getByValue(GROUP_KEY)).andReturn(ImmutableSet.of(SLOT));
    slotCache.remove(SLOT, GROUP_KEY);
    expectSlotValidation(Optional.<ImmutableSet<PreemptionVictim>>absent());

    control.replay();

    assertEquals(EMPTY_RESULT, callPreemptor());
    assertEquals(1L, statsProvider.getLongValue(slotValidationStatName(false)));
    assertEquals(0L, statsProvider.getLongValue(successStatName(true)));
  }

  @Test
  public void testNoCachedSlot() throws Exception {
    expect(slotCache.getByValue(GROUP_KEY)).andReturn(NO_SLOTS);

    control.replay();

    assertEquals(EMPTY_RESULT, callPreemptor());
    assertEquals(0L, statsProvider.getLongValue(slotValidationStatName(false)));
    assertEquals(0L, statsProvider.getLongValue(successStatName(true)));
  }

  private Optional<String> callPreemptor() {
    return preemptor.attemptPreemptionFor(TASK.getAssignedTask(), EMPTY, storeProvider);
  }

  private void expectSlotValidation(Optional<ImmutableSet<PreemptionVictim>> victims) {
    expect(preemptionSlotFinder.validatePreemptionSlotFor(
        TASK.getAssignedTask(),
        EMPTY,
        SLOT,
        storeProvider)).andReturn(victims);
  }

  private void expectPreempted(IScheduledTask preempted) throws Exception {
    expect(stateManager.changeState(
        anyObject(Storage.MutableStoreProvider.class),
        eq(Tasks.id(preempted)),
        eq(Optional.<ScheduleStatus>absent()),
        eq(ScheduleStatus.PREEMPTING),
        EasyMock.<Optional<String>>anyObject()))
        .andReturn(true);
  }

  private static PreemptionSlot createPreemptionSlot(IScheduledTask task) {
    IAssignedTask assigned = task.getAssignedTask();
    return new PreemptionSlot(ImmutableSet.of(PreemptionVictim.fromTask(assigned)), SLAVE_ID);
  }

  private static ScheduledTask makeTask() {
    ScheduledTask task = new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTask(new TaskConfig()
                .setPriority(1)
                .setProduction(true)
                .setJob(new JobKey("role", "env", "name"))));
    task.addToTaskEvents(new TaskEvent(0, PENDING));
    return task;
  }
}
