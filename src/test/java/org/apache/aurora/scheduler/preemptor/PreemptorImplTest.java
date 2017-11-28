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

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.preemptor.Preemptor.PreemptorImpl;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.mesos.v1.Protos;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.filter.AttributeAggregate.empty;
import static org.apache.aurora.scheduler.preemptor.PreemptorMetrics.slotValidationStatName;
import static org.apache.aurora.scheduler.preemptor.PreemptorMetrics.successStatName;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class PreemptorImplTest extends EasyMockTest {
  private static final String SLAVE_ID_1 = "slave_id_1";
  private static final String SLAVE_ID_2 = "slave_id_2";
  private static final IScheduledTask TASK = IScheduledTask.build(makeTask());
  private static final PreemptionProposal PROPOSAL_1 = createPreemptionProposal(TASK, SLAVE_ID_1);
  private static final PreemptionProposal PROPOSAL_2 = createPreemptionProposal(TASK, SLAVE_ID_2);
  private static final TaskGroupKey GROUP_KEY =
      TaskGroupKey.from(ITaskConfig.build(makeTask().getAssignedTask().getTask()));

  private static final Set<PreemptionProposal> NO_SLOTS = ImmutableSet.of();
  private static final Optional<String> EMPTY_RESULT = Optional.absent();
  private static final HostOffer OFFER =
      new HostOffer(Protos.Offer.getDefaultInstance(), IHostAttributes.build(new HostAttributes()));

  private StateManager stateManager;
  private FakeStatsProvider statsProvider;
  private PreemptionVictimFilter preemptionVictimFilter;
  private PreemptorImpl preemptor;
  private BiCache<PreemptionProposal, TaskGroupKey> slotCache;
  private Storage.MutableStoreProvider storeProvider;

  @Before
  public void setUp() {
    storeProvider = createMock(Storage.MutableStoreProvider.class);
    stateManager = createMock(StateManager.class);
    preemptionVictimFilter = createMock(PreemptionVictimFilter.class);
    slotCache = createMock(new Clazz<BiCache<PreemptionProposal, TaskGroupKey>>() { });
    statsProvider = new FakeStatsProvider();
    OfferManager offerManager = createMock(OfferManager.class);
    expect(offerManager.get(anyObject(Protos.AgentID.class)))
        .andReturn(Optional.of(OFFER))
        .anyTimes();

    preemptor = new PreemptorImpl(
        stateManager,
        offerManager,
        preemptionVictimFilter,
        new PreemptorMetrics(new CachedCounters(statsProvider)),
        slotCache);
  }

  @Test
  public void testPreemptTasksSuccessful() throws Exception {
    expect(slotCache.getByValue(GROUP_KEY)).andReturn(ImmutableSet.of(PROPOSAL_1, PROPOSAL_2));
    slotCache.remove(PROPOSAL_1, GROUP_KEY);
    expectSlotValidation(PROPOSAL_1, Optional.of(ImmutableSet.of(
        PreemptionVictim.fromTask(TASK.getAssignedTask()))));

    expectPreempted(TASK);

    control.replay();

    assertEquals(Optional.of(SLAVE_ID_1), callPreemptor());
    assertEquals(0L, statsProvider.getLongValue(slotValidationStatName(false)));
    assertEquals(1L, statsProvider.getLongValue(slotValidationStatName(true)));
    assertEquals(1L, statsProvider.getLongValue(successStatName(true)));
  }

  @Test
  public void testPreemptTasksValidationFailed() throws Exception {
    expect(slotCache.getByValue(GROUP_KEY)).andReturn(ImmutableSet.of(PROPOSAL_1));
    slotCache.remove(PROPOSAL_1, GROUP_KEY);
    expectSlotValidation(PROPOSAL_1, Optional.absent());

    control.replay();

    assertEquals(EMPTY_RESULT, callPreemptor());
    assertEquals(1L, statsProvider.getLongValue(slotValidationStatName(false)));
    assertEquals(0L, statsProvider.getLongValue(slotValidationStatName(true)));
    assertEquals(0L, statsProvider.getLongValue(successStatName(true)));
  }

  @Test
  public void testMultiplePreemptionProposalsSuccessful() throws Exception {
    expect(slotCache.getByValue(GROUP_KEY)).andReturn(ImmutableSet.of(PROPOSAL_1, PROPOSAL_2));
    slotCache.remove(PROPOSAL_1, GROUP_KEY);
    expectSlotValidation(PROPOSAL_1, Optional.absent());
    slotCache.remove(PROPOSAL_2, GROUP_KEY);
    expectSlotValidation(PROPOSAL_2, Optional.of(ImmutableSet.of(
        PreemptionVictim.fromTask(TASK.getAssignedTask()))));

    expectPreempted(TASK);

    control.replay();

    assertEquals(Optional.of(SLAVE_ID_2), callPreemptor());
    assertEquals(1L, statsProvider.getLongValue(slotValidationStatName(false)));
    assertEquals(1L, statsProvider.getLongValue(slotValidationStatName(true)));
    assertEquals(1L, statsProvider.getLongValue(successStatName(true)));
  }

  @Test
  public void testMultiplePreemptionProposalsFailed() throws Exception {
    expect(slotCache.getByValue(GROUP_KEY)).andReturn(ImmutableSet.of(PROPOSAL_1, PROPOSAL_2));
    slotCache.remove(PROPOSAL_1, GROUP_KEY);
    expectSlotValidation(PROPOSAL_1, Optional.absent());
    slotCache.remove(PROPOSAL_2, GROUP_KEY);
    expectSlotValidation(PROPOSAL_2, Optional.absent());

    control.replay();

    assertEquals(EMPTY_RESULT, callPreemptor());
    assertEquals(2L, statsProvider.getLongValue(slotValidationStatName(false)));
    assertEquals(0L, statsProvider.getLongValue(slotValidationStatName(true)));
    assertEquals(0L, statsProvider.getLongValue(successStatName(true)));
  }

  @Test
  public void testNoCachedSlot() throws Exception {
    expect(slotCache.getByValue(GROUP_KEY)).andReturn(NO_SLOTS);

    control.replay();

    assertEquals(EMPTY_RESULT, callPreemptor());
    assertEquals(0L, statsProvider.getLongValue(slotValidationStatName(false)));
    assertEquals(0L, statsProvider.getLongValue(slotValidationStatName(true)));
    assertEquals(0L, statsProvider.getLongValue(successStatName(true)));
  }

  private Optional<String> callPreemptor() {
    return preemptor.attemptPreemptionFor(TASK.getAssignedTask(), empty(), storeProvider);
  }

  private void expectSlotValidation(
      PreemptionProposal slot,
      Optional<ImmutableSet<PreemptionVictim>> victims) {

    expect(preemptionVictimFilter.filterPreemptionVictims(
        TASK.getAssignedTask().getTask(),
        slot.getVictims(),
        empty(),
        Optional.of(OFFER),
        storeProvider)).andReturn(victims);
  }

  private void expectPreempted(IScheduledTask preempted) throws Exception {
    expect(stateManager.changeState(
        anyObject(Storage.MutableStoreProvider.class),
        eq(Tasks.id(preempted)),
        eq(Optional.absent()),
        eq(ScheduleStatus.PREEMPTING),
        anyObject()))
        .andReturn(StateChangeResult.SUCCESS);
  }

  private static PreemptionProposal createPreemptionProposal(IScheduledTask task, String slaveId) {
    IAssignedTask assigned = task.getAssignedTask();
    return new PreemptionProposal(ImmutableSet.of(PreemptionVictim.fromTask(assigned)), slaveId);
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
