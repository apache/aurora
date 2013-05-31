package com.twitter.mesos.scheduler;

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.HostStatus;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.MaintenanceMode;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.MaintenanceController.MaintenanceControllerImpl;
import com.twitter.mesos.scheduler.events.PubsubEvent;
import com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.storage.testing.StorageTestUtil;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import static com.twitter.mesos.gen.MaintenanceMode.DRAINED;
import static com.twitter.mesos.gen.MaintenanceMode.DRAINING;
import static com.twitter.mesos.gen.MaintenanceMode.NONE;
import static com.twitter.mesos.gen.MaintenanceMode.SCHEDULED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;

public class MaintenanceControllerImplTest extends EasyMockTest {

  private static final String HOST_A = "a";
  private static final String HOST_B = "b";
  private static final Set<String> A = ImmutableSet.of(HOST_A);

  private StorageTestUtil storageUtil;
  private StateManager stateManager;
  private MaintenanceControllerImpl maintenance;
  private Closure<PubsubEvent> eventSink;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectTransactions();
    stateManager = createMock(StateManager.class);
    eventSink = createMock(new Clazz<Closure<PubsubEvent>>() { });
    maintenance = new MaintenanceControllerImpl(storageUtil.storage, stateManager, eventSink);
  }

  private static ScheduledTask makeTask(String host, String taskId) {
    return new ScheduledTask()
        .setStatus(RUNNING)
        .setAssignedTask(
            new AssignedTask()
                .setSlaveHost(host)
                .setTaskId(taskId)
                .setTask(
                    new TwitterTaskInfo()
                        .setJobName("jobName")
                        .setOwner(new Identity().setRole("role").setUser("role"))));
  }

  @Test
  public void testMaintenanceCycle() {
    ScheduledTask task = makeTask(HOST_A, "taskA");

    expectMaintenanceModeChange(HOST_A, SCHEDULED);
    expectMaintenanceModeChange(HOST_A, DRAINING);
    expectFetchTasksByHost(HOST_A, Tasks.ids(task));
    expectMaintenanceModeChange(HOST_A, DRAINED);
    expectMaintenanceModeChange(HOST_A, NONE);
    expect(stateManager.changeState(
        Query.slaveScoped(HOST_A).active().get(),
        ScheduleStatus.RESTARTING,
        MaintenanceControllerImpl.DRAINING_MESSAGE))
        .andReturn(1);

    control.replay();

    assertStatus(HOST_A, SCHEDULED, maintenance.startMaintenance(A));
    assertStatus(HOST_A, DRAINING, maintenance.drain(A));
    maintenance.taskChangedState(new TaskStateChange(task.setStatus(FINISHED), RUNNING));
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
    expectFetchTasksByHost(HOST_A, ImmutableSet.<String>of());
    expectMaintenanceModeChange(HOST_A, DRAINED);

    control.replay();

    assertStatus(HOST_A, SCHEDULED, maintenance.startMaintenance(A));
    assertStatus(HOST_A, DRAINED, maintenance.drain(A));
  }

  @Test
  public void testEndEarly() {
    expectMaintenanceModeChange(HOST_A, SCHEDULED);
    expectMaintenanceModeChange(HOST_A, NONE);

    control.replay();

    assertStatus(HOST_A, SCHEDULED, maintenance.startMaintenance(A));

    // End maintenance without DRAINING.
    assertStatus(HOST_A, NONE, maintenance.endMaintenance(A));

    // Make sure a later transition on the host does not cause any ill effects that could surface
    // from stale internal state.
    maintenance.taskChangedState(
        new TaskStateChange(makeTask(HOST_A, "taskA").setStatus(FINISHED), RUNNING));
  }

  @Test
  public void testStorageStart() {
    ScheduledTask taskA = makeTask(HOST_A, "taskA").setStatus(ScheduleStatus.RESTARTING);

    expect(storageUtil.attributeStore.getHostAttributes())
        .andReturn(ImmutableSet.of(
            new HostAttributes().setHost(HOST_A).setMode(DRAINING),
            new HostAttributes().setHost(HOST_B).setMode(DRAINING)));
    expectFetchTasksByHost(HOST_B, Tasks.ids());
    expectMaintenanceModeChange(HOST_B, DRAINED);
    expectMaintenanceModeChange(HOST_A, DRAINING);
    expectFetchTasksByHost(HOST_A, Tasks.ids(taskA));
    expectMaintenanceModeChange(HOST_A, DRAINED);

    control.replay();

    maintenance.storageStarted(new StorageStarted());
    maintenance.taskChangedState(new TaskStateChange(taskA.setStatus(FINISHED), RUNNING));
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

  private void expectFetchTasksByHost(String hostName, Set<String> taskIds) {
    expect(storageUtil.taskStore.fetchTaskIds(Query.slaveScoped(hostName).active()))
        .andReturn(taskIds);
  }

  private void expectMaintenanceModeChange(String hostName, MaintenanceMode mode) {
    expect(storageUtil.attributeStore.setMaintenanceMode(hostName, mode)).andReturn(true);
    eventSink.execute(
        new PubsubEvent.HostMaintenanceStateChange(
            new HostStatus().setHost(hostName).setMode(mode)));
  }

  private void assertStatus(String host, MaintenanceMode mode, Set<HostStatus> statuses) {
    assertEquals(ImmutableSet.of(new HostStatus(host, mode)), statuses);
  }
}
