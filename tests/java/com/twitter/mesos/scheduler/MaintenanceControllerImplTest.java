package com.twitter.mesos.scheduler;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.HostStatus;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.MaintenanceMode;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.MaintenanceController.MaintenanceControllerImpl;
import com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.mem.MemStorage;

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
  private static final Set<String> A = ImmutableSet.of(HOST_A);

  private Storage storage;
  private StateManager stateManager;
  private MaintenanceControllerImpl maintenance;

  @Before
  public void setUp() throws Exception {
    storage = MemStorage.newEmptyStorage();
    stateManager = createMock(StateManager.class);
    maintenance = new MaintenanceControllerImpl(storage, stateManager);
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
    expect(stateManager.changeState(
        new TaskQuery().setSlaveHost(HOST_A).setStatuses(Tasks.ACTIVE_STATES),
        ScheduleStatus.RESTARTING,
        MaintenanceControllerImpl.DRAINING_MESSAGE))
        .andReturn(1);

    control.replay();

    setMode(HOST_A, NONE);
    ScheduledTask task = makeTask(HOST_A, "taskA");
    saveTask(task);
    assertStatus(HOST_A, SCHEDULED, maintenance.startMaintenance(A));
    assertStatus(HOST_A, DRAINING, maintenance.drain(A));
    task.setStatus(FINISHED);
    maintenance.taskChangedState(new TaskStateChange("taskA", RUNNING, task));
    assertStatus(HOST_A, DRAINED, maintenance.getStatus(A));
    assertStatus(HOST_A, NONE, maintenance.endMaintenance(A));
  }

  @Test
  public void testUnknownHosts() {
    control.replay();

    assertEquals(ImmutableSet.<HostStatus>of(),
        maintenance.startMaintenance(ImmutableSet.of(HOST_A, "b")));
    checkAttributes();
  }

  @Test
  public void testDrainEmptyHost() {
    control.replay();

    setMode(HOST_A, NONE);
    assertStatus(HOST_A, SCHEDULED, maintenance.startMaintenance(A));
    assertStatus(HOST_A, DRAINED, maintenance.drain(A));
  }

  @Test
  public void testEndEarly() {
    control.replay();

    setMode(HOST_A, NONE);
    ScheduledTask task = makeTask(HOST_A, "taskA");
    saveTask(task);
    assertStatus(HOST_A, SCHEDULED, maintenance.startMaintenance(A));

    // End maintenance without DRAINING.
    assertStatus(HOST_A, NONE, maintenance.endMaintenance(A));
    assertStatus(HOST_A, NONE, maintenance.getStatus(A));

    // Make sure a later transition on the host does not cause any ill effects that could surface
    // from stale internal state.
    task.setStatus(FINISHED);
    maintenance.taskChangedState(new TaskStateChange("taskA", RUNNING, task));
    assertStatus(HOST_A, NONE, maintenance.getStatus(A));
  }

  @Test
  public void testStorageStart() {
    control.replay();

    ScheduledTask task = makeTask(HOST_A, "taskA").setStatus(ScheduleStatus.RESTARTING);
    saveAttribute(new HostAttributes().setHost(HOST_A).setMode(DRAINING));
    saveTask(task);
    saveAttribute(new HostAttributes().setHost("b").setMode(DRAINING));
    saveTask(makeTask("b", "taskB").setStatus(ScheduleStatus.FINISHED));

    maintenance.storageStarted(new StorageStarted());

    assertStatus(HOST_A, DRAINING, maintenance.getStatus(A));
    assertStatus("b", DRAINED, maintenance.getStatus(ImmutableSet.of("b")));
    task.setStatus(ScheduleStatus.FINISHED);
    maintenance.taskChangedState(new TaskStateChange("taskA", RUNNING, task));
    assertStatus(HOST_A, DRAINED, maintenance.getStatus(A));
  }

  private void assertStatus(String host, MaintenanceMode mode, Set<HostStatus> statuses) {
    assertEquals(ImmutableSet.of(new HostStatus(host, mode)), statuses);
  }

  private void checkAttributes(HostAttributes... attributes) {
    assertEquals(
        ImmutableSet.<HostAttributes>builder().add(attributes).build(),
        getHostAttributes());
  }

  private Set<HostAttributes> getHostAttributes() {
    return storage.doInTransaction(new Work.Quiet<Set<HostAttributes>>() {
      @Override public Set<HostAttributes> apply(StoreProvider storeProvider) {
        return storeProvider.getAttributeStore().getHostAttributes();
      }
    });
  }

  private void setMode(String host, MaintenanceMode mode) {
    saveAttribute(new HostAttributes().setHost(host).setMode(mode));
  }

  private void saveAttribute(final HostAttributes attributes) {
    storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getAttributeStore().saveHostAttributes(attributes);
      }
    });
  }

  private void saveTask(final ScheduledTask task) {
    storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getTaskStore().saveTasks(ImmutableSet.of(task));
      }
    });
  }
}
