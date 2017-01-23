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
package org.apache.aurora.scheduler.state;

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
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.state.TaskAssigner.TaskAssignerImpl;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Type;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.base.TaskTestUtil.DEV_TIER;
import static org.apache.aurora.scheduler.base.TaskTestUtil.JOB;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.apache.aurora.scheduler.filter.AttributeAggregate.empty;
import static org.apache.aurora.scheduler.resources.ResourceManager.bagFromMesosResources;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosRange;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.offer;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.state.TaskAssigner.TaskAssignerImpl.ASSIGNER_EVALUATED_OFFERS;
import static org.apache.aurora.scheduler.state.TaskAssigner.TaskAssignerImpl.ASSIGNER_LAUNCH_FAILURES;
import static org.apache.aurora.scheduler.state.TaskAssigner.TaskAssignerImpl.LAUNCH_FAILED_MSG;
import static org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import static org.apache.mesos.Protos.Offer;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TaskAssignerImplTest extends EasyMockTest {

  private static final int PORT = 1000;
  private static final Offer MESOS_OFFER = offer(mesosRange(PORTS, PORT));
  private static final String SLAVE_ID = MESOS_OFFER.getSlaveId().getValue();
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
      .setSlaveId(MESOS_OFFER.getSlaveId())
      .build();
  private static final Map<String, TaskGroupKey> NO_RESERVATION = ImmutableMap.of();
  private static final UnusedResource UNUSED = new UnusedResource(
      bagFromMesosResources(MESOS_OFFER.getResourcesList()),
      OFFER.getAttributes());
  private static final HostOffer OFFER_2 = new HostOffer(
      Offer.newBuilder()
          .setId(OfferID.newBuilder().setValue("offerId0"))
              .setFrameworkId(FrameworkID.newBuilder().setValue("frameworkId"))
              .setSlaveId(SlaveID.newBuilder().setValue("slaveId0"))
              .setHostname("hostName0")
          .addResources(Resource.newBuilder()
          .setName("ports")
          .setType(Type.RANGES)
          .setRanges(
              Ranges.newBuilder().addRange(Range.newBuilder().setBegin(PORT).setEnd(PORT))))
              .build(),
      IHostAttributes.build(new HostAttributes()));

  private static final Set<String> NO_ASSIGNMENT = ImmutableSet.of();

  private ResourceRequest resourceRequest;

  private MutableStoreProvider storeProvider;
  private StateManager stateManager;
  private SchedulingFilter filter;
  private MesosTaskFactory taskFactory;
  private OfferManager offerManager;
  private TaskAssignerImpl assigner;
  private TierManager tierManager;
  private FakeStatsProvider statsProvider;

  @Before
  public void setUp() throws Exception {
    storeProvider = createMock(MutableStoreProvider.class);
    filter = createMock(SchedulingFilter.class);
    taskFactory = createMock(MesosTaskFactory.class);
    stateManager = createMock(StateManager.class);
    offerManager = createMock(OfferManager.class);
    tierManager = createMock(TierManager.class);
    statsProvider = new FakeStatsProvider();
    assigner = new TaskAssignerImpl(
        stateManager,
        filter,
        taskFactory,
        offerManager,
        tierManager,
        statsProvider);
    resourceRequest = new ResourceRequest(
        TASK.getAssignedTask().getTask(),
        ResourceBag.EMPTY,
        empty());
  }

  @Test
  public void testAssignNoTasks() throws Exception {
    control.replay();

    assertEquals(
        NO_ASSIGNMENT,
        assigner.maybeAssign(storeProvider, null, null, ImmutableSet.of(), null));
  }

  @Test
  public void testAssignPartialNoVetoes() throws Exception {
    expect(offerManager.getOffers(GROUP_KEY)).andReturn(ImmutableSet.of(OFFER));
    offerManager.launchTask(MESOS_OFFER.getId(), TASK_INFO);
    expect(tierManager.getTier(TASK.getAssignedTask().getTask())).andReturn(DEV_TIER);
    expect(filter.filter(UNUSED, resourceRequest)).andReturn(ImmutableSet.of());
    expectAssignTask(MESOS_OFFER);
    expect(taskFactory.createFrom(TASK.getAssignedTask(), MESOS_OFFER))
        .andReturn(TASK_INFO);

    control.replay();

    AttributeAggregate aggregate = empty();
    assertEquals(0L, statsProvider.getLongValue(ASSIGNER_EVALUATED_OFFERS));
    assertEquals(
        ImmutableSet.of(Tasks.id(TASK)),
        assigner.maybeAssign(
            storeProvider,
            new ResourceRequest(TASK.getAssignedTask().getTask(), ResourceBag.EMPTY, aggregate),
            TaskGroupKey.from(TASK.getAssignedTask().getTask()),
            ImmutableSet.of(Tasks.id(TASK), "id2", "id3"),
            ImmutableMap.of(SLAVE_ID, GROUP_KEY)));
    assertNotEquals(empty(), aggregate);
    assertEquals(1L, statsProvider.getLongValue(ASSIGNER_EVALUATED_OFFERS));
  }

  @Test
  public void testAssignVetoesWithStaticBan() throws Exception {
    expect(offerManager.getOffers(GROUP_KEY)).andReturn(ImmutableSet.of(OFFER));
    offerManager.banOffer(MESOS_OFFER.getId(), GROUP_KEY);
    expect(tierManager.getTier(TASK.getAssignedTask().getTask())).andReturn(DEV_TIER);
    expect(filter.filter(UNUSED, resourceRequest))
        .andReturn(ImmutableSet.of(Veto.constraintMismatch("denied")));

    control.replay();

    assertEquals(0L, statsProvider.getLongValue(ASSIGNER_EVALUATED_OFFERS));
    assertEquals(
        NO_ASSIGNMENT,
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            TaskGroupKey.from(TASK.getAssignedTask().getTask()),
            ImmutableSet.of(Tasks.id(TASK)),
            NO_RESERVATION));
    assertEquals(1L, statsProvider.getLongValue(ASSIGNER_EVALUATED_OFFERS));
  }

  @Test
  public void testAssignVetoesWithNoStaticBan() throws Exception {
    expect(offerManager.getOffers(GROUP_KEY)).andReturn(ImmutableSet.of(OFFER));
    expect(tierManager.getTier(TASK.getAssignedTask().getTask())).andReturn(DEV_TIER);
    expect(filter.filter(UNUSED, resourceRequest))
        .andReturn(ImmutableSet.of(Veto.unsatisfiedLimit("limit")));

    control.replay();

    assertEquals(0L, statsProvider.getLongValue(ASSIGNER_EVALUATED_OFFERS));
    assertEquals(
        NO_ASSIGNMENT,
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            TaskGroupKey.from(TASK.getAssignedTask().getTask()),
            ImmutableSet.of(Tasks.id(TASK)),
            NO_RESERVATION));
    assertEquals(1L, statsProvider.getLongValue(ASSIGNER_EVALUATED_OFFERS));
  }

  @Test
  public void testAssignmentClearedOnError() throws Exception {
    expect(offerManager.getOffers(GROUP_KEY)).andReturn(ImmutableSet.of(OFFER, OFFER_2));
    offerManager.launchTask(MESOS_OFFER.getId(), TASK_INFO);
    expectLastCall().andThrow(new OfferManager.LaunchException("expected"));
    expect(tierManager.getTier(TASK.getAssignedTask().getTask())).andReturn(DEV_TIER);
    expect(filter.filter(UNUSED, resourceRequest)).andReturn(ImmutableSet.of());
    expectAssignTask(MESOS_OFFER);
    expect(stateManager.changeState(
        storeProvider,
        Tasks.id(TASK),
        Optional.of(PENDING),
        LOST,
        LAUNCH_FAILED_MSG))
        .andReturn(StateChangeResult.SUCCESS);
    expect(taskFactory.createFrom(TASK.getAssignedTask(), MESOS_OFFER))
        .andReturn(TASK_INFO);

    control.replay();

    assertEquals(0L, statsProvider.getLongValue(ASSIGNER_LAUNCH_FAILURES));
    assertEquals(0L, statsProvider.getLongValue(ASSIGNER_EVALUATED_OFFERS));
    // Ensures scheduling loop terminates on the first launch failure.
    assertEquals(
        NO_ASSIGNMENT,
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            TaskGroupKey.from(TASK.getAssignedTask().getTask()),
            ImmutableSet.of(Tasks.id(TASK), "id2", "id3"),
            NO_RESERVATION));
    assertEquals(1L, statsProvider.getLongValue(ASSIGNER_LAUNCH_FAILURES));
    assertEquals(1L, statsProvider.getLongValue(ASSIGNER_EVALUATED_OFFERS));
  }

  @Test
  public void testAssignmentSkippedForReservedSlave() throws Exception {
    expect(tierManager.getTier(TASK.getAssignedTask().getTask())).andReturn(DEV_TIER);
    expect(offerManager.getOffers(GROUP_KEY)).andReturn(ImmutableSet.of(OFFER));

    control.replay();

    assertEquals(0L, statsProvider.getLongValue(ASSIGNER_EVALUATED_OFFERS));
    assertEquals(
        NO_ASSIGNMENT,
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            TaskGroupKey.from(TASK.getAssignedTask().getTask()),
            ImmutableSet.of(Tasks.id(TASK)),
            ImmutableMap.of(SLAVE_ID, TaskGroupKey.from(
                ITaskConfig.build(new TaskConfig().setJob(new JobKey("other", "e", "n")))))));
    assertEquals(1L, statsProvider.getLongValue(ASSIGNER_EVALUATED_OFFERS));
  }

  @Test
  public void testTaskWithReservedSlaveLandsElsewhere() throws Exception {
    // Ensures slave/task reservation relationship is only enforced in slave->task direction
    // and permissive in task->slave direction. In other words, a task with a slave reservation
    // should still be tried against other unreserved slaves.
    expect(offerManager.getOffers(GROUP_KEY)).andReturn(ImmutableSet.of(OFFER_2, OFFER));
    expect(tierManager.getTier(TASK.getAssignedTask().getTask())).andReturn(DEV_TIER);
    expect(filter.filter(
        new UnusedResource(
            bagFromMesosResources(OFFER_2.getOffer().getResourcesList()),
            OFFER_2.getAttributes()),
        resourceRequest)).andReturn(ImmutableSet.of());
    expectAssignTask(OFFER_2.getOffer());
    expect(taskFactory.createFrom(TASK.getAssignedTask(), OFFER_2.getOffer()))
        .andReturn(TASK_INFO);
    offerManager.launchTask(OFFER_2.getOffer().getId(), TASK_INFO);

    control.replay();

    assertEquals(0L, statsProvider.getLongValue(ASSIGNER_EVALUATED_OFFERS));
    assertEquals(
        ImmutableSet.of(Tasks.id(TASK)),
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            TaskGroupKey.from(TASK.getAssignedTask().getTask()),
            ImmutableSet.of(Tasks.id(TASK)),
            ImmutableMap.of(SLAVE_ID, GROUP_KEY)));
    assertEquals(1L, statsProvider.getLongValue(ASSIGNER_EVALUATED_OFFERS));
  }

  @Test
  public void testAssignerDoesNotReturnOnFirstMismatch() throws Exception {
    // Ensures scheduling loop does not terminate prematurely when the first mismatch is identified.
    HostOffer mismatched = new HostOffer(
        Offer.newBuilder()
            .setId(OfferID.newBuilder().setValue("offerId0"))
            .setFrameworkId(FrameworkID.newBuilder().setValue("frameworkId"))
            .setSlaveId(SlaveID.newBuilder().setValue("slaveId0"))
            .setHostname("hostName0")
            .addResources(Resource.newBuilder()
                .setName("ports")
                .setType(Type.RANGES)
                .setRanges(
                    Ranges.newBuilder().addRange(Range.newBuilder().setBegin(PORT).setEnd(PORT))))
            .build(),
        IHostAttributes.build(new HostAttributes()));

    expect(offerManager.getOffers(GROUP_KEY)).andReturn(ImmutableSet.of(mismatched, OFFER));
    expect(tierManager.getTier(TASK.getAssignedTask().getTask())).andReturn(DEV_TIER);
    expect(filter.filter(
        new UnusedResource(
            bagFromMesosResources(mismatched.getOffer().getResourcesList()),
            mismatched.getAttributes()),
        resourceRequest))
        .andReturn(ImmutableSet.of(Veto.constraintMismatch("constraint mismatch")));
    offerManager.banOffer(mismatched.getOffer().getId(), GROUP_KEY);
    expect(filter.filter(
        new UnusedResource(
            bagFromMesosResources(MESOS_OFFER.getResourcesList()), OFFER.getAttributes()),
        resourceRequest))
        .andReturn(ImmutableSet.of());

    expectAssignTask(MESOS_OFFER);
    expect(taskFactory.createFrom(TASK.getAssignedTask(), OFFER.getOffer()))
        .andReturn(TASK_INFO);
    offerManager.launchTask(OFFER.getOffer().getId(), TASK_INFO);

    control.replay();

    assertEquals(0L, statsProvider.getLongValue(ASSIGNER_EVALUATED_OFFERS));
    assertEquals(
        ImmutableSet.of(Tasks.id(TASK)),
        assigner.maybeAssign(
            storeProvider,
            resourceRequest,
            TaskGroupKey.from(TASK.getAssignedTask().getTask()),
            ImmutableSet.of(Tasks.id(TASK)),
            ImmutableMap.of(SLAVE_ID, GROUP_KEY)));
    assertEquals(2L, statsProvider.getLongValue(ASSIGNER_EVALUATED_OFFERS));
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

  private void expectAssignTask(Offer offer) {
    expect(stateManager.assignTask(
        eq(storeProvider),
        eq(Tasks.id(TASK)),
        eq(offer.getHostname()),
        eq(offer.getSlaveId()),
        anyObject())).andReturn(TASK.getAssignedTask());
  }
}
