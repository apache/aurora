/**
 * Copyright 2013 Apache Software Foundation
 *
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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.HostStatus;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.DRAINED;
import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.gen.MaintenanceMode.SCHEDULED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.state.MaintenanceController.MaintenanceControllerImpl;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class MaintenanceControllerImplTest extends EasyMockTest {

  private static final String HOST_A = "a";
  private static final Set<String> A = ImmutableSet.of(HOST_A);

  private StorageTestUtil storageUtil;
  private StateManager stateManager;
  private MaintenanceController maintenance;
  private EventSink eventSink;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    stateManager = createMock(StateManager.class);

    Injector injector = Guice.createInjector(
        new AbstractModule() {
          @Override
          protected void configure() {
            PubsubTestUtil.installPubsub(binder());
            StateModule.bindMaintenanceController(binder());
            bind(Storage.class).toInstance(storageUtil.storage);
            bind(StateManager.class).toInstance(stateManager);
          }
        });
    maintenance = injector.getInstance(MaintenanceController.class);
    eventSink = PubsubTestUtil.startPubsub(injector);
  }

  private static ScheduledTask makeTask(String host, String taskId) {
    return new ScheduledTask()
        .setStatus(RUNNING)
        .setAssignedTask(
            new AssignedTask()
                .setSlaveHost(host)
                .setTaskId(taskId)
                .setTask(
                    new TaskConfig()
                        .setJobName("jobName")
                        .setOwner(new Identity().setRole("role").setUser("role"))));
  }

  @Test
  public void testMaintenanceCycle() {
    ScheduledTask task = makeTask(HOST_A, "taskA");

    expectMaintenanceModeChange(HOST_A, SCHEDULED);
    expectFetchTasksByHost(HOST_A, ImmutableSet.<ScheduledTask>of(task));
    expect(stateManager.changeState(
        Tasks.id(task),
        Optional.<ScheduleStatus>absent(),
        ScheduleStatus.RESTARTING,
        MaintenanceControllerImpl.DRAINING_MESSAGE))
        .andReturn(true);
    expectMaintenanceModeChange(HOST_A, DRAINING);
    expect(storageUtil.attributeStore.getHostAttributes(HOST_A))
        .andReturn(Optional.of(new HostAttributes().setHost(HOST_A).setMode(DRAINING)));
    // TaskA is FINISHED and therefore no longer active
    expectFetchTasksByHost(HOST_A, ImmutableSet.<ScheduledTask>of());
    expectMaintenanceModeChange(HOST_A, DRAINED);
    expectMaintenanceModeChange(HOST_A, NONE);

    control.replay();

    assertStatus(HOST_A, SCHEDULED, maintenance.startMaintenance(A));
    assertStatus(HOST_A, DRAINING, maintenance.drain(A));
    eventSink.post(
        TaskStateChange.transition(IScheduledTask.build(task.setStatus(FINISHED)), RUNNING));
    assertStatus(HOST_A, NONE, maintenance.endMaintenance(A));
  }

  @Test
  public void testUnknownHost() {
    expect(storageUtil.attributeStore.setMaintenanceMode("b", MaintenanceMode.SCHEDULED))
        .andReturn(false);

    control.replay();

    assertEquals(ImmutableSet.<HostStatus>of(),
        maintenance.startMaintenance(ImmutableSet.of("b")));
  }

  @Test
  public void testDrainEmptyHost() {
    expectMaintenanceModeChange(HOST_A, SCHEDULED);
    expectFetchTasksByHost(HOST_A, ImmutableSet.<ScheduledTask>of());
    expectMaintenanceModeChange(HOST_A, DRAINED);

    control.replay();

    assertStatus(HOST_A, SCHEDULED, maintenance.startMaintenance(A));
    assertStatus(HOST_A, DRAINED, maintenance.drain(A));
  }

  @Test
  public void testEndEarly() {
    expectMaintenanceModeChange(HOST_A, SCHEDULED);
    expectMaintenanceModeChange(HOST_A, NONE);
    expect(storageUtil.attributeStore.getHostAttributes(HOST_A))
        .andReturn(Optional.of(new HostAttributes().setHost(HOST_A).setMode(NONE)));

    control.replay();

    assertStatus(HOST_A, SCHEDULED, maintenance.startMaintenance(A));

    // End maintenance without DRAINING.
    assertStatus(HOST_A, NONE, maintenance.endMaintenance(A));

    // Make sure a later transition on the host does not cause any ill effects that could surface
    // from stale internal state.
    eventSink.post(TaskStateChange.transition(
        IScheduledTask.build(makeTask(HOST_A, "taskA").setStatus(FINISHED)), RUNNING));
  }

  @Test
  public void testGetMode() {
    expect(storageUtil.attributeStore.getHostAttributes(HOST_A))
        .andReturn(Optional.of(new HostAttributes().setHost(HOST_A).setMode(DRAINING)));
    expect(storageUtil.attributeStore.getHostAttributes("unknown"))
        .andReturn(Optional.<HostAttributes>absent());

    control.replay();

    assertEquals(DRAINING, maintenance.getMode(HOST_A));
    assertEquals(NONE, maintenance.getMode("unknown"));
  }

  private void expectFetchTasksByHost(String hostName, ImmutableSet<ScheduledTask> tasks) {
    expect(storageUtil.taskStore.fetchTasks(Query.slaveScoped(hostName).active()))
        .andReturn(IScheduledTask.setFromBuilders(tasks));
  }

  private void expectMaintenanceModeChange(String hostName, MaintenanceMode mode) {
    expect(storageUtil.attributeStore.setMaintenanceMode(hostName, mode)).andReturn(true);
    eventSink.post(
        new PubsubEvent.HostMaintenanceStateChange(
            new HostStatus().setHost(hostName).setMode(mode)));
  }

  private void assertStatus(String host, MaintenanceMode mode, Set<HostStatus> statuses) {
    assertEquals(ImmutableSet.of(new HostStatus(host, mode)), statuses);
  }
}
