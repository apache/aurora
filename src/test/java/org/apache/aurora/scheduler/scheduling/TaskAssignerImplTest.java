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
package org.apache.aurora.scheduler.scheduling;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.InstanceKeys;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.aurora.scheduler.updater.UpdateAgentReserver;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.Protos.TaskInfo;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.base.TaskTestUtil.JOB;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.apache.aurora.scheduler.filter.AttributeAggregate.empty;
import static org.apache.aurora.scheduler.mesos.TaskExecutors.NO_OVERHEAD_EXECUTOR;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosRange;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.offer;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.apache.aurora.scheduler.scheduling.TaskAssignerImpl.ASSIGNER_LAUNCH_FAILURES;
import static org.apache.aurora.scheduler.scheduling.TaskAssignerImpl.LAUNCH_FAILED_MSG;
import static org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import static org.apache.mesos.v1.Protos.Offer;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TaskAssignerImplTest extends EasyMockTest {

  private static final int PORT = 1000;
  private static final Offer MESOS_OFFER =
      offer(mesosScalar(CPUS, 1), mesosScalar(RAM_MB, 1024), mesosRange(PORTS, PORT));
  private static final String SLAVE_ID = MESOS_OFFER.getAgentId().getValue();
  private static final HostOffer OFFER =
      new HostOffer(MESOS_OFFER, IHostAttributes.build(new HostAttributes()
          .setHost(MESOS_OFFER.getHostname())
          .setAttributes(ImmutableSet.of(
              new Attribute("host", ImmutableSet.of(MESOS_OFFER.getHostname()))))));
  private static final IAssignedTask TASK = makeTask("id", JOB).getAssignedTask();
  private static final TaskGroupKey GROUP_KEY = TaskGroupKey.from(TASK.getTask());
  private static final TaskInfo TASK_INFO = TaskInfo.newBuilder()
      .setName("taskName")
      .setTaskId(TaskID.newBuilder().setValue(TASK.getTaskId()))
      .setAgentId(MESOS_OFFER.getAgentId())
      .build();
  private static final IInstanceKey INSTANCE_KEY = InstanceKeys.from(JOB, TASK.getInstanceId());
  private static final Map<String, TaskGroupKey> NO_RESERVATION = ImmutableMap.of();
  private static final Offer MESOS_OFFER_2 =
      offer("offer-2", mesosScalar(CPUS, 1), mesosScalar(RAM_MB, 1024), mesosRange(PORTS, PORT));
  private static final HostOffer OFFER_2 =
      new HostOffer(MESOS_OFFER_2, IHostAttributes.build(new HostAttributes()
          .setHost(MESOS_OFFER_2.getHostname())
          .setAttributes(ImmutableSet.of(
              new Attribute("host", ImmutableSet.of(MESOS_OFFER_2.getHostname()))))));

  private static final Set<String> NO_ASSIGNMENT = ImmutableSet.of();

  private AttributeAggregate aggregate;
  private ResourceRequest resourceRequest;

  private MutableStoreProvider storeProvider;
  private StateManager stateManager;
  private MesosTaskFactory taskFactory;
  private OfferManager offerManager;
  private TaskAssignerImpl assigner;
  private FakeStatsProvider statsProvider;
  private UpdateAgentReserver updateAgentReserver;

  @Before
  public void setUp() {
    storeProvider = createMock(MutableStoreProvider.class);
    taskFactory = createMock(MesosTaskFactory.class);
    stateManager = createMock(StateManager.class);
    offerManager = createMock(OfferManager.class);
    updateAgentReserver = createMock(UpdateAgentReserver.class);
    statsProvider = new FakeStatsProvider();
    assigner = new TaskAssignerImpl(
        stateManager,
        taskFactory,
        offerManager,
        updateAgentReserver,
        statsProvider);
    aggregate = empty();
    resourceRequest = ResourceRequest.fromTask(
        TASK.getTask(),
        NO_OVERHEAD_EXECUTOR,
        aggregate,
        TaskTestUtil.TIER_MANAGER);
  }

  @Test
  public void testAssignNoTasks() {
    control.replay();

    assertEquals(
        NO_ASSIGNMENT,
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            GROUP_KEY,
            ImmutableSet.of(),
            NO_RESERVATION));
  }

  @Test
  public void testAssignmentClearedOnError() throws Exception {
    expect(updateAgentReserver.isReserved(anyString())).andReturn(false).atLeastOnce();
    expect(updateAgentReserver.getAgent(anyObject())).andReturn(Optional.empty()).atLeastOnce();

    expect(offerManager.getAllMatching(GROUP_KEY, resourceRequest))
        .andReturn(ImmutableSet.of(OFFER, OFFER_2)).atLeastOnce();
    offerManager.launchTask(MESOS_OFFER.getId(), TASK_INFO);
    expectLastCall().andThrow(new OfferManager.LaunchException("expected"));
    expectAssignTask(MESOS_OFFER);
    expect(stateManager.changeState(
        storeProvider,
        TASK.getTaskId(),
        Optional.of(PENDING),
        LOST,
        LAUNCH_FAILED_MSG))
        .andReturn(StateChangeResult.SUCCESS);
    expect(taskFactory.createFrom(TASK, MESOS_OFFER, false))
        .andReturn(TASK_INFO);

    control.replay();

    assertEquals(0L, statsProvider.getLongValue(ASSIGNER_LAUNCH_FAILURES));
    // Ensures scheduling loop terminates on the first launch failure.
    assertEquals(
        NO_ASSIGNMENT,
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            TaskGroupKey.from(TASK.getTask()),
            ImmutableSet.of(
                TASK,
                makeTask("id2", JOB).getAssignedTask(),
                makeTask("id3", JOB).getAssignedTask()),
            NO_RESERVATION));
    assertEquals(1L, statsProvider.getLongValue(ASSIGNER_LAUNCH_FAILURES));
  }

  @Test
  public void testAssignmentSkippedForReservedSlave() {
    expectNoUpdateReservations(0);
    expect(offerManager.getAllMatching(GROUP_KEY, resourceRequest))
        .andReturn(ImmutableSet.of(OFFER));

    control.replay();

    assertEquals(
        NO_ASSIGNMENT,
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            TaskGroupKey.from(TASK.getTask()),
            ImmutableSet.of(TASK),
            ImmutableMap.of(SLAVE_ID, TaskGroupKey.from(
                ITaskConfig.build(new TaskConfig().setJob(new JobKey("other", "e", "n")))))));
  }

  @Test
  public void testTaskWithReservedSlaveLandsElsewhere() throws Exception {
    // Ensures slave/task reservation relationship is only enforced in slave->task direction
    // and permissive in task->slave direction. In other words, a task with a slave reservation
    // should still be tried against other unreserved slaves.
    expectNoUpdateReservations(1);
    expect(offerManager.getAllMatching(GROUP_KEY, resourceRequest))
        .andReturn(ImmutableSet.of(OFFER_2, OFFER));
    expectAssignTask(OFFER_2.getOffer());
    expect(taskFactory.createFrom(TASK, OFFER_2.getOffer(), false))
        .andReturn(TASK_INFO);
    offerManager.launchTask(OFFER_2.getOffer().getId(), TASK_INFO);

    control.replay();

    assertEquals(
        ImmutableSet.of(TASK.getTaskId()),
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            TaskGroupKey.from(TASK.getTask()),
            ImmutableSet.of(TASK),
            ImmutableMap.of(SLAVE_ID, GROUP_KEY)));
  }

  @Test
  public void testResourceMapperCallback() {
    AssignedTask builder = TASK.newBuilder();
    builder.unsetAssignedPorts();

    control.replay();

    assertEquals(
        TASK,
        assigner.mapAndAssignResources(MESOS_OFFER, IAssignedTask.build(builder)));
  }

  @Test
  public void testAssignToReservedAgent() throws Exception {
    expect(updateAgentReserver.getAgent(INSTANCE_KEY)).andReturn(Optional.of(SLAVE_ID));
    updateAgentReserver.release(SLAVE_ID, INSTANCE_KEY);
    expect(offerManager.getMatching(MESOS_OFFER.getAgentId(), resourceRequest))
        .andReturn(Optional.of(OFFER));
    expectAssignTask(MESOS_OFFER);
    offerManager.launchTask(MESOS_OFFER.getId(), TASK_INFO);

    expect(taskFactory.createFrom(TASK, MESOS_OFFER, false))
        .andReturn(TASK_INFO);

    control.replay();

    assertEquals(
        ImmutableSet.of(TASK.getTaskId()),
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            TaskGroupKey.from(TASK.getTask()),
            ImmutableSet.of(
                TASK),
            ImmutableMap.of(SLAVE_ID, GROUP_KEY)));
    assertNotEquals(empty(), aggregate);
  }

  @Test
  public void testAssignReservedAgentWhenOfferNotReady() {
    expect(updateAgentReserver.getAgent(INSTANCE_KEY)).andReturn(Optional.of(SLAVE_ID));
    expect(offerManager.getMatching(MESOS_OFFER.getAgentId(), resourceRequest))
        .andReturn(Optional.empty());
    expectLastCall();

    control.replay();

    assertEquals(
        ImmutableSet.of(),
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            TaskGroupKey.from(TASK.getTask()),
            ImmutableSet.of(TASK),
            ImmutableMap.of(SLAVE_ID, GROUP_KEY)));
    assertEquals(empty(), aggregate);
  }

  @Test
  public void testAssignWithMixOfReservedAndNotReserved() throws Exception {
    expect(updateAgentReserver.getAgent(INSTANCE_KEY)).andReturn(Optional.of(SLAVE_ID));
    updateAgentReserver.release(SLAVE_ID, INSTANCE_KEY);
    expect(offerManager.getMatching(MESOS_OFFER.getAgentId(), resourceRequest))
        .andReturn(Optional.of(OFFER));
    expectAssignTask(MESOS_OFFER);
    offerManager.launchTask(MESOS_OFFER.getId(), TASK_INFO);
    expect(taskFactory.createFrom(TASK, MESOS_OFFER, false))
        .andReturn(TASK_INFO);

    // Normal scheduling loop for the remaining task.
    IAssignedTask secondTask = makeTask("another-task", JOB, 9999).getAssignedTask();
    TaskInfo secondTaskInfo = TaskInfo.newBuilder()
        .setName("another-task")
        .setTaskId(TaskID.newBuilder().setValue(secondTask.getTaskId()))
        .setAgentId(MESOS_OFFER.getAgentId())
        .build();
    expect(updateAgentReserver.getAgent(InstanceKeys.from(JOB, 9999))).andReturn(Optional.empty());
    expect(offerManager.getAllMatching(GROUP_KEY, resourceRequest))
        .andReturn(ImmutableSet.of(OFFER_2));
    expect(updateAgentReserver.isReserved(OFFER_2.getOffer().getAgentId().getValue()))
        .andReturn(false);
    expectAssignTask(MESOS_OFFER_2, secondTask);
    offerManager.launchTask(MESOS_OFFER_2.getId(), secondTaskInfo);
    expect(taskFactory.createFrom(secondTask, MESOS_OFFER_2, false)).andReturn(secondTaskInfo);

    control.replay();

    assertEquals(
        ImmutableSet.of(TASK.getTaskId(), secondTask.getTaskId()),
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            GROUP_KEY,
            ImmutableSet.of(TASK, secondTask),
            ImmutableMap.of(SLAVE_ID, GROUP_KEY)));
    assertNotEquals(empty(), aggregate);
  }

  private void expectAssignTask(Offer offer) {
    expectAssignTask(offer, TASK);
  }

  private void expectAssignTask(Offer offer, IAssignedTask task) {
    expect(stateManager.assignTask(
        eq(storeProvider),
        eq(task.getTaskId()),
        eq(offer.getHostname()),
        eq(offer.getAgentId()),
        anyObject())).andReturn(task);
  }

  private void expectNoUpdateReservations(int offers) {
    if (offers > 0) {
      expect(updateAgentReserver.isReserved(anyString())).andReturn(false).times(offers);
    }
    expect(updateAgentReserver.getAgent(anyObject())).andReturn(Optional.empty());
  }
}
