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

import com.google.common.base.Optional;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.MesosTaskFactory;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.state.TaskAssigner.TaskAssignerImpl;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
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

import static org.apache.aurora.scheduler.async.OfferQueue.HostOffer;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class TaskAssignerImplTest extends EasyMockTest {

  private static final int PORT = 5000;
  private static final Offer OFFER = Offer.newBuilder()
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
  private static final HostOffer HOST_OFFER = new HostOffer(OFFER, MaintenanceMode.NONE);
  private static final IScheduledTask TASK = IScheduledTask.build(
      new ScheduledTask()
          .setAssignedTask(new AssignedTask()
              .setTaskId("taskId")
              .setTask(new TaskConfig()
                  .setExecutorConfig(new ExecutorConfig().setData("opaque data"))
                  .setRequestedPorts(ImmutableSet.of("http")))));
  private static final TaskInfo TASK_INFO = TaskInfo.newBuilder()
      .setName("taskName")
      .setTaskId(TaskID.newBuilder().setValue(Tasks.id(TASK)))
      .setSlaveId(OFFER.getSlaveId())
      .build();

  private StateManager stateManager;
  private SchedulingFilter filter;
  private MesosTaskFactory taskFactory;
  private TaskAssigner assigner;
  private AttributeAggregate emptyJob;

  @Before
  public void setUp() throws Exception {
    stateManager = createMock(StateManager.class);
    filter = createMock(SchedulingFilter.class);
    taskFactory = createMock(MesosTaskFactory.class);
    assigner = new TaskAssignerImpl(stateManager, filter, taskFactory);
    emptyJob = new AttributeAggregate(
        Suppliers.ofInstance(ImmutableSet.<IScheduledTask>of()),
        createMock(AttributeStore.class));
  }

  @Test
  public void testAssignNoVetoes() {
    expect(filter.filter(
        ResourceSlot.from(OFFER),
        OFFER.getHostname(),
        MaintenanceMode.NONE,
        TASK.getAssignedTask().getTask(),
        Tasks.id(TASK),
        emptyJob))
        .andReturn(ImmutableSet.<Veto>of());
    expect(stateManager.assignTask(
        Tasks.id(TASK),
        OFFER.getHostname(),
        OFFER.getSlaveId(),
        ImmutableSet.of(PORT)))
        .andReturn(TASK.getAssignedTask());
    expect(taskFactory.createFrom(TASK.getAssignedTask(), OFFER.getSlaveId())).andReturn(TASK_INFO);

    control.replay();

    assertEquals(Optional.of(TASK_INFO), assigner.maybeAssign(HOST_OFFER, TASK, emptyJob));
  }

  @Test
  public void testAssignVetoes() {
    expect(filter.filter(
        ResourceSlot.from(OFFER),
        OFFER.getHostname(),
        MaintenanceMode.NONE,
        TASK.getAssignedTask().getTask(),
        Tasks.id(TASK),
        emptyJob))
        .andReturn(ImmutableSet.of(Veto.constraintMismatch("denied")));

    control.replay();

    assertEquals(Optional.<TaskInfo>absent(), assigner.maybeAssign(HOST_OFFER, TASK, emptyJob));
  }
}
