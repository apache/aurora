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
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.InstanceKeys;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.aurora.scheduler.updater.UpdateAgentReserver;
import org.apache.mesos.v1.Protos.AgentID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.OfferID;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.Protos.TaskInfo;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.base.TaskTestUtil.DEV_TIER;
import static org.apache.aurora.scheduler.base.TaskTestUtil.JOB;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.apache.aurora.scheduler.filter.AttributeAggregate.empty;
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
  private static final IScheduledTask TASK = makeTask("id", JOB);
  private static final TaskGroupKey GROUP_KEY = TaskGroupKey.from(TASK.getAssignedTask().getTask());
  private static final TaskInfo TASK_INFO = TaskInfo.newBuilder()
      .setName("taskName")
      .setTaskId(TaskID.newBuilder().setValue(Tasks.id(TASK)))
      .setAgentId(MESOS_OFFER.getAgentId())
      .build();
  private static final IInstanceKey INSTANCE_KEY =
      InstanceKeys.from(JOB, TASK.getAssignedTask().getInstanceId());
  private static final Map<String, TaskGroupKey> NO_RESERVATION = ImmutableMap.of();
  private static final HostOffer OFFER_2 = new HostOffer(
      Offer.newBuilder()
          .setId(OfferID.newBuilder().setValue("offerId0"))
          .setFrameworkId(FrameworkID.newBuilder().setValue("frameworkId"))
          .setAgentId(AgentID.newBuilder().setValue("slaveId0"))
          .setHostname("hostName0")
          .addResources(mesosRange(PORTS, PORT))
          .addResources(mesosScalar(CPUS, 1))
          .addResources(mesosScalar(RAM_MB, 1024))
          .build(),
      IHostAttributes.build(new HostAttributes()));

  private static final Set<String> NO_ASSIGNMENT = ImmutableSet.of();

  private AttributeAggregate aggregate;
  private ResourceRequest resourceRequest;

  private MutableStoreProvider storeProvider;
  private StateManager stateManager;
  private MesosTaskFactory taskFactory;
  private OfferManager offerManager;
  private TaskAssignerImpl assigner;
  private TierManager tierManager;
  private FakeStatsProvider statsProvider;
  private UpdateAgentReserver updateAgentReserver;

  @Before
  public void setUp() throws Exception {
    storeProvider = createMock(MutableStoreProvider.class);
    taskFactory = createMock(MesosTaskFactory.class);
    stateManager = createMock(StateManager.class);
    offerManager = createMock(OfferManager.class);
    tierManager = createMock(TierManager.class);
    updateAgentReserver = createMock(UpdateAgentReserver.class);
    statsProvider = new FakeStatsProvider();
    // TODO(jly): FirstFitOfferSelector returns the first offer which is what we want for testing,
    // but if its implementation becomes more complex we may need to replace it with a fake.
    OfferSelector offerSelector = new FirstFitOfferSelector();
    assigner = new TaskAssignerImpl(
        stateManager,
        taskFactory,
        offerManager,
        tierManager,
        updateAgentReserver,
        statsProvider,
        offerSelector);
    aggregate = empty();
    resourceRequest = new ResourceRequest(
        TASK.getAssignedTask().getTask(),
        ResourceBag.EMPTY,
        aggregate);
  }

  @Test
  public void testAssignNoTasks() throws Exception {
    control.replay();

    assertEquals(
        NO_ASSIGNMENT,
        assigner.maybeAssign(storeProvider, null, null, ImmutableSet.of(), null));
  }

  @Test
  public void testAssignmentClearedOnError() throws Exception {
    expectNoUpdateReservations(1);
    expect(offerManager.getAllMatching(GROUP_KEY, resourceRequest, false))
        .andReturn(ImmutableSet.of(OFFER, OFFER_2));
    offerManager.launchTask(MESOS_OFFER.getId(), TASK_INFO);
    expectLastCall().andThrow(new OfferManager.LaunchException("expected"));
    expect(tierManager.getTier(TASK.getAssignedTask().getTask())).andReturn(DEV_TIER);
    expectAssignTask(MESOS_OFFER);
    expect(stateManager.changeState(
        storeProvider,
        Tasks.id(TASK),
        Optional.of(PENDING),
        LOST,
        LAUNCH_FAILED_MSG))
        .andReturn(StateChangeResult.SUCCESS);
    expect(taskFactory.createFrom(TASK.getAssignedTask(), MESOS_OFFER, false))
        .andReturn(TASK_INFO);

    control.replay();

    assertEquals(0L, statsProvider.getLongValue(ASSIGNER_LAUNCH_FAILURES));
    // Ensures scheduling loop terminates on the first launch failure.
    assertEquals(
        NO_ASSIGNMENT,
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            TaskGroupKey.from(TASK.getAssignedTask().getTask()),
            ImmutableSet.of(
                TASK.getAssignedTask(),
                makeTask("id2", JOB).getAssignedTask(),
                makeTask("id3", JOB).getAssignedTask()),
            NO_RESERVATION));
    assertEquals(1L, statsProvider.getLongValue(ASSIGNER_LAUNCH_FAILURES));
  }

  @Test
  public void testAssignmentSkippedForReservedSlave() throws Exception {
    expectNoUpdateReservations(0);
    expect(tierManager.getTier(TASK.getAssignedTask().getTask())).andReturn(DEV_TIER);
    expect(offerManager.getAllMatching(GROUP_KEY, resourceRequest, false))
        .andReturn(ImmutableSet.of(OFFER));

    control.replay();

    assertEquals(
        NO_ASSIGNMENT,
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            TaskGroupKey.from(TASK.getAssignedTask().getTask()),
            ImmutableSet.of(TASK.getAssignedTask()),
            ImmutableMap.of(SLAVE_ID, TaskGroupKey.from(
                ITaskConfig.build(new TaskConfig().setJob(new JobKey("other", "e", "n")))))));
  }

  @Test
  public void testTaskWithReservedSlaveLandsElsewhere() throws Exception {
    // Ensures slave/task reservation relationship is only enforced in slave->task direction
    // and permissive in task->slave direction. In other words, a task with a slave reservation
    // should still be tried against other unreserved slaves.
    expectNoUpdateReservations(1);
    expect(offerManager.getAllMatching(GROUP_KEY, resourceRequest, false))
        .andReturn(ImmutableSet.of(OFFER_2, OFFER));
    expect(tierManager.getTier(TASK.getAssignedTask().getTask())).andReturn(DEV_TIER);
    expectAssignTask(OFFER_2.getOffer());
    expect(taskFactory.createFrom(TASK.getAssignedTask(), OFFER_2.getOffer(), false))
        .andReturn(TASK_INFO);
    offerManager.launchTask(OFFER_2.getOffer().getId(), TASK_INFO);

    control.replay();

    assertEquals(
        ImmutableSet.of(Tasks.id(TASK)),
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            TaskGroupKey.from(TASK.getAssignedTask().getTask()),
            ImmutableSet.of(TASK.getAssignedTask()),
            ImmutableMap.of(SLAVE_ID, GROUP_KEY)));
  }

  @Test
  public void testResourceMapperCallback() {
    AssignedTask builder = TASK.newBuilder().getAssignedTask();
    builder.unsetAssignedPorts();

    control.replay();

    assertEquals(
        TASK.getAssignedTask(),
        assigner.mapAndAssignResources(MESOS_OFFER, IAssignedTask.build(builder)));
  }

  @Test
  public void testAssignToReservedAgent() throws Exception {
    expect(updateAgentReserver.hasReservations(GROUP_KEY)).andReturn(true);
    expect(updateAgentReserver.getAgent(INSTANCE_KEY)).andReturn(Optional.of(SLAVE_ID));
    updateAgentReserver.release(SLAVE_ID, INSTANCE_KEY);
    expect(offerManager.getMatching(MESOS_OFFER.getAgentId(), resourceRequest, false))
        .andReturn(Optional.of(OFFER));
    expectAssignTask(MESOS_OFFER);
    offerManager.launchTask(MESOS_OFFER.getId(), TASK_INFO);
    expect(tierManager.getTier(TASK.getAssignedTask().getTask())).andReturn(DEV_TIER);

    expect(taskFactory.createFrom(TASK.getAssignedTask(), MESOS_OFFER, false))
        .andReturn(TASK_INFO);

    control.replay();

    assertEquals(
        ImmutableSet.of(Tasks.id(TASK)),
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            TaskGroupKey.from(TASK.getAssignedTask().getTask()),
            ImmutableSet.of(
                TASK.getAssignedTask()),
            ImmutableMap.of(SLAVE_ID, GROUP_KEY)));
    assertNotEquals(empty(), aggregate);
  }

  @Test
  public void testAssignReservedAgentWhenOfferNotReady() throws Exception {
    expect(updateAgentReserver.hasReservations(GROUP_KEY)).andReturn(true);
    expect(updateAgentReserver.getAgent(INSTANCE_KEY)).andReturn(Optional.of(SLAVE_ID));
    expect(offerManager.getMatching(MESOS_OFFER.getAgentId(), resourceRequest, false))
        .andReturn(Optional.absent());
    expect(tierManager.getTier(TASK.getAssignedTask().getTask())).andReturn(DEV_TIER);
    expectLastCall();

    control.replay();

    assertEquals(
        ImmutableSet.of(),
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            TaskGroupKey.from(TASK.getAssignedTask().getTask()),
            ImmutableSet.of(TASK.getAssignedTask()),
            ImmutableMap.of(SLAVE_ID, GROUP_KEY)));
    assertEquals(empty(), aggregate);
  }

  @Test
  public void testAssignWithMixOfReservedAndNotReserved() throws Exception {
    expect(tierManager.getTier(TASK.getAssignedTask().getTask())).andReturn(DEV_TIER);

    expect(updateAgentReserver.hasReservations(GROUP_KEY)).andReturn(true);
    expect(updateAgentReserver.getAgent(INSTANCE_KEY)).andReturn(Optional.of(SLAVE_ID));
    updateAgentReserver.release(SLAVE_ID, INSTANCE_KEY);
    expect(offerManager.getMatching(MESOS_OFFER.getAgentId(), resourceRequest, false))
        .andReturn(Optional.of(OFFER));
    expectAssignTask(MESOS_OFFER);
    offerManager.launchTask(MESOS_OFFER.getId(), TASK_INFO);
    expect(taskFactory.createFrom(TASK.getAssignedTask(), MESOS_OFFER, false))
        .andReturn(TASK_INFO);

    // Normal scheduling loop for the remaining task...
    IScheduledTask secondTask = makeTask("another-task", JOB, 9999);
    TaskInfo secondTaskInfo = TaskInfo.newBuilder()
        .setName("another-task")
        .setTaskId(TaskID.newBuilder().setValue(Tasks.id(secondTask)))
        .setAgentId(MESOS_OFFER.getAgentId())
        .build();
    expect(updateAgentReserver.getAgent(InstanceKeys.from(JOB, 9999))).andReturn(Optional.absent());
    ImmutableSet<HostOffer> matchingOffers = ImmutableSet.of(OFFER);
    expect(offerManager.getAllMatching(GROUP_KEY, resourceRequest, false))
        .andReturn(matchingOffers);
    expect(updateAgentReserver.getReservations(OFFER.getOffer().getAgentId().getValue()))
        .andReturn(ImmutableSet.of());
    expectAssignTask(MESOS_OFFER, secondTask);
    offerManager.launchTask(MESOS_OFFER.getId(), secondTaskInfo);
    expect(taskFactory.createFrom(secondTask.getAssignedTask(), MESOS_OFFER, false))
        .andReturn(secondTaskInfo);

    control.replay();

    assertEquals(
        Tasks.ids(TASK, secondTask),
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            GROUP_KEY,
            ImmutableSet.of(
                TASK.getAssignedTask(),
                secondTask.getAssignedTask()),
            ImmutableMap.of(SLAVE_ID, GROUP_KEY)));
    assertNotEquals(empty(), aggregate);
  }

  private void expectAssignTask(Offer offer) {
    expectAssignTask(offer, TASK);
  }

  private void expectAssignTask(Offer offer, IScheduledTask task) {
    expect(stateManager.assignTask(
        eq(storeProvider),
        eq(Tasks.id(task)),
        eq(offer.getHostname()),
        eq(offer.getAgentId()),
        anyObject())).andReturn(task.getAssignedTask());
  }

  private void expectNoUpdateReservations(int offers) {
    expect(updateAgentReserver.hasReservations(anyObject())).andReturn(false);
    for (int i = 0; i < offers; i++) {
      expect(updateAgentReserver.getReservations(anyString())).andReturn(ImmutableSet.of());
    }
  }
}
