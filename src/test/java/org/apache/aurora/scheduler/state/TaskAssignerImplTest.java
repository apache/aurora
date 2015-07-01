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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.state.TaskAssigner.Assignment;
import org.apache.aurora.scheduler.state.TaskAssigner.TaskAssignerImpl;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
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

import static org.apache.aurora.scheduler.filter.AttributeAggregate.EMPTY;
import static org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import static org.apache.mesos.Protos.Offer;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class TaskAssignerImplTest extends EasyMockTest {

  private static final int PORT = 5000;
  private static final Offer MESOS_OFFER = Offer.newBuilder()
      .setId(OfferID.newBuilder().setValue("offerId"))
      .setFrameworkId(FrameworkID.newBuilder().setValue("frameworkId"))
      .setSlaveId(SlaveID.newBuilder().setValue("slaveId"))
      .setHostname("hostName")
      .addResources(Resource.newBuilder()
          .setName("ports")
          .setType(Type.RANGES)
          .setRanges(
              Ranges.newBuilder().addRange(Range.newBuilder().setBegin(PORT).setEnd(PORT))))
      .build();
  private static final HostOffer OFFER =
      new HostOffer(MESOS_OFFER, IHostAttributes.build(new HostAttributes()));
  private static final String PORT_NAME = "http";
  private static final IScheduledTask TASK = IScheduledTask.build(
      new ScheduledTask()
          .setAssignedTask(new AssignedTask()
              .setTaskId("taskId")
              .setTask(new TaskConfig()
                  .setExecutorConfig(new ExecutorConfig().setData("opaque data"))
                  .setRequestedPorts(ImmutableSet.of(PORT_NAME)))));
  private static final TaskInfo TASK_INFO = TaskInfo.newBuilder()
      .setName("taskName")
      .setTaskId(TaskID.newBuilder().setValue(Tasks.id(TASK)))
      .setSlaveId(MESOS_OFFER.getSlaveId())
      .build();

  private MutableStoreProvider storeProvider;
  private StateManager stateManager;
  private SchedulingFilter filter;
  private MesosTaskFactory taskFactory;
  private TaskAssigner assigner;

  @Before
  public void setUp() throws Exception {
    storeProvider = createMock(MutableStoreProvider.class);
    stateManager = createMock(StateManager.class);
    filter = createMock(SchedulingFilter.class);
    taskFactory = createMock(MesosTaskFactory.class);
    assigner = new TaskAssignerImpl(stateManager, filter, taskFactory);
  }

  @Test
  public void testAssignNoVetoes() {
    expect(filter.filter(
        new UnusedResource(ResourceSlot.from(MESOS_OFFER), OFFER.getAttributes()),
        new ResourceRequest(TASK.getAssignedTask().getTask(), EMPTY)))
        .andReturn(ImmutableSet.of());
    expect(stateManager.assignTask(
        storeProvider,
        Tasks.id(TASK),
        MESOS_OFFER.getHostname(),
        MESOS_OFFER.getSlaveId(),
        ImmutableMap.of(PORT_NAME, PORT)))
        .andReturn(TASK.getAssignedTask());
    expect(taskFactory.createFrom(TASK.getAssignedTask(), MESOS_OFFER.getSlaveId()))
        .andReturn(TASK_INFO);

    control.replay();

    assertEquals(
        Assignment.success(TASK_INFO),
        assigner.maybeAssign(
            storeProvider,
            OFFER,
            new ResourceRequest(TASK.getAssignedTask().getTask(), EMPTY),
            Tasks.id(TASK)));
  }

  @Test
  public void testAssignVetoes() {
    expect(filter.filter(
        new UnusedResource(ResourceSlot.from(MESOS_OFFER), OFFER.getAttributes()),
        new ResourceRequest(TASK.getAssignedTask().getTask(), EMPTY)))
        .andReturn(ImmutableSet.of(Veto.constraintMismatch("denied")));

    control.replay();

    assertEquals(
        Assignment.failure(ImmutableSet.of(Veto.constraintMismatch("denied"))),
        assigner.maybeAssign(
            storeProvider,
            OFFER,
            new ResourceRequest(TASK.getAssignedTask().getTask(), EMPTY),
            Tasks.id(TASK)));
  }
}
