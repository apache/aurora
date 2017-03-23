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

import java.util.Set;
import java.util.concurrent.Executor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.HostStatus;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.SchedulerModule.TaskEventBatchWorker;
import org.apache.aurora.scheduler.async.AsyncModule.AsyncExecutor;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IHostStatus;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.mesos.v1.Protos;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.DRAINED;
import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.gen.MaintenanceMode.SCHEDULED;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.state.MaintenanceController.MaintenanceControllerImpl;
import static org.apache.aurora.scheduler.testing.BatchWorkerUtil.expectBatchExecute;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class MaintenanceControllerImplTest extends EasyMockTest {

  private static final String HOST_A = "a";
  private static final Set<String> A = ImmutableSet.of(HOST_A);
  private static final Protos.OfferID OFFER_ID = Protos.OfferID.newBuilder()
      .setValue("offer-id")
      .build();
  private static final Protos.AgentID AGENT_ID = Protos.AgentID.newBuilder()
      .setValue("agent-id")
      .build();
  private static final Protos.FrameworkID FRAMEWORK_ID = Protos.FrameworkID.newBuilder()
      .setValue("framework-id")
      .build();
  private static final Protos.URL AGENT_URL = Protos.URL.newBuilder()
      .setAddress(Protos.Address.newBuilder()
          .setHostname(HOST_A)
          .setPort(5051))
      .setScheme("http")
      .build();
  private static final Protos.Unavailability UNAVAILABILITY = Protos.Unavailability.newBuilder()
      .setStart(Protos.TimeInfo.newBuilder()
          .setNanoseconds(Amount.of(1L, Time.MINUTES).as(Time.NANOSECONDS)))
      .build();

  private static final Protos.InverseOffer INVERSE_OFFER = Protos.InverseOffer.newBuilder()
      .setId(OFFER_ID)
      .setAgentId(AGENT_ID)
      .setUrl(AGENT_URL)
      .setFrameworkId(FRAMEWORK_ID)
      .setUnavailability(UNAVAILABILITY)
      .build();

  private StorageTestUtil storageUtil;
  private StateManager stateManager;
  private MaintenanceController maintenance;
  private EventSink eventSink;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    stateManager = createMock(StateManager.class);
    TaskEventBatchWorker batchWorker = createMock(TaskEventBatchWorker.class);
    expectBatchExecute(batchWorker, storageUtil.storage, control).anyTimes();

    Injector injector = Guice.createInjector(
        new PubsubEventModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            StateModule.bindMaintenanceController(binder());
            bind(Storage.class).toInstance(storageUtil.storage);
            bind(StateManager.class).toInstance(stateManager);
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(Executor.class).annotatedWith(AsyncExecutor.class)
                .toInstance(MoreExecutors.directExecutor());
            bind(TaskEventBatchWorker.class).toInstance(batchWorker);
          }
        });
    maintenance = injector.getInstance(MaintenanceController.class);
    eventSink = PubsubTestUtil.startPubsub(injector);
  }

  private static IScheduledTask makeTask(String host, String taskId) {
    ScheduledTask builder = TaskTestUtil.addStateTransition(
        TaskTestUtil.makeTask(taskId, TaskTestUtil.JOB),
        RUNNING,
        1000).newBuilder();
    builder.getAssignedTask().setSlaveHost(host);
    return IScheduledTask.build(builder);
  }

  @Test
  public void testMaintenanceCycle() {
    IScheduledTask task1 = makeTask(HOST_A, "taskA");
    IScheduledTask task2 = makeTask(HOST_A, "taskB");

    expectMaintenanceModeChange(HOST_A, SCHEDULED);
    expectFetchTasksByHost(HOST_A, ImmutableSet.of(task1, task2));
    expectTaskDraining(task1);
    expectTaskDraining(task2);
    expectMaintenanceModeChange(HOST_A, DRAINING);
    IHostAttributes attributes =
        IHostAttributes.build(new HostAttributes().setHost(HOST_A).setMode(DRAINING));

    expect(storageUtil.attributeStore.getHostAttributes(HOST_A))
        .andReturn(Optional.of(attributes)).times(2);

    expect(storageUtil.attributeStore.getHostAttributes()).andReturn(ImmutableSet.of(attributes));
    expectFetchTasksByHost(HOST_A, ImmutableSet.of(task2));
    // TaskA is KILLED and therefore no longer active
    expectFetchTasksByHost(HOST_A, ImmutableSet.of());
    expectMaintenanceModeChange(HOST_A, DRAINED);
    expectMaintenanceModeChange(HOST_A, NONE);

    control.replay();

    assertStatus(HOST_A, SCHEDULED, maintenance.startMaintenance(A));
    assertStatus(HOST_A, DRAINING, maintenance.drain(A));
    assertStatus(HOST_A, DRAINING, maintenance.getStatus(A));
    eventSink.post(
        TaskStateChange.transition(
            IScheduledTask.build(task1.newBuilder().setStatus(KILLED)), RUNNING));
    eventSink.post(
        TaskStateChange.transition(
            IScheduledTask.build(task2.newBuilder().setStatus(KILLED)), RUNNING));
    assertStatus(HOST_A, NONE, maintenance.endMaintenance(A));
  }

  @Test
  public void testUnknownHost() {
    expect(storageUtil.attributeStore.getHostAttributes("b"))
        .andReturn(Optional.absent());

    control.replay();

    assertEquals(ImmutableSet.of(),
        maintenance.startMaintenance(ImmutableSet.of("b")));
  }

  @Test
  public void testDrainEmptyHost() {
    expectMaintenanceModeChange(HOST_A, SCHEDULED);
    expectFetchTasksByHost(HOST_A, ImmutableSet.of());
    expectMaintenanceModeChange(HOST_A, DRAINED);

    control.replay();

    assertStatus(HOST_A, SCHEDULED, maintenance.startMaintenance(A));
    assertStatus(HOST_A, DRAINED, maintenance.drain(A));
  }

  @Test
  public void testEndEarly() {
    expectMaintenanceModeChange(HOST_A, SCHEDULED);
    expectMaintenanceModeChange(HOST_A, NONE);
    expect(storageUtil.attributeStore.getHostAttributes(HOST_A)).andReturn(Optional.of(
        IHostAttributes.build(new HostAttributes().setHost(HOST_A).setMode(NONE))));

    control.replay();

    assertStatus(HOST_A, SCHEDULED, maintenance.startMaintenance(A));

    // End maintenance without DRAINING.
    assertStatus(HOST_A, NONE, maintenance.endMaintenance(A));

    // Make sure a later transition on the host does not cause any ill effects that could surface
    // from stale internal state.
    eventSink.post(TaskStateChange.transition(
        IScheduledTask.build(makeTask(HOST_A, "taskA").newBuilder().setStatus(KILLED)), RUNNING));
  }

  @Test
  public void testGetMode() {
    expect(storageUtil.attributeStore.getHostAttributes(HOST_A)).andReturn(Optional.of(
        IHostAttributes.build(new HostAttributes().setHost(HOST_A).setMode(DRAINING))));
    expect(storageUtil.attributeStore.getHostAttributes("unknown")).andReturn(Optional.absent());

    control.replay();

    assertEquals(DRAINING, maintenance.getMode(HOST_A));
    assertEquals(NONE, maintenance.getMode("unknown"));
  }

  @Test
  public void testInverseOfferDrain() {
    IScheduledTask task1 = makeTask(HOST_A, "taskA");
    expectFetchTasksByHost(HOST_A, ImmutableSet.of(task1));
    expectTaskDraining(task1);

    control.replay();
    maintenance.drainForInverseOffer(INVERSE_OFFER);
  }

  private void expectTaskDraining(IScheduledTask task) {
    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        Tasks.id(task),
        Optional.absent(),
        ScheduleStatus.DRAINING,
        MaintenanceControllerImpl.DRAINING_MESSAGE))
        .andReturn(StateChangeResult.SUCCESS);
  }

  private void expectFetchTasksByHost(String hostName, Set<IScheduledTask> tasks) {
    expect(storageUtil.taskStore.fetchTasks(Query.slaveScoped(hostName).active())).andReturn(tasks);
  }

  private void expectMaintenanceModeChange(String hostName, MaintenanceMode mode) {
    IHostAttributes attributes = IHostAttributes.build(new HostAttributes().setHost(hostName));

    expect(storageUtil.attributeStore.getHostAttributes(hostName))
        .andReturn(Optional.of(attributes));
    IHostAttributes updated = IHostAttributes.build(attributes.newBuilder().setMode(mode));
    expect(storageUtil.attributeStore.saveHostAttributes(updated)).andReturn(true);
  }

  private void assertStatus(String host, MaintenanceMode mode, Set<IHostStatus> statuses) {
    assertEquals(ImmutableSet.of(IHostStatus.build(new HostStatus(host, mode))), statuses);
  }
}
