package com.twitter.mesos.scheduler.storage.backup;

import java.io.File;

import com.google.common.collect.ImmutableSet;
import com.google.common.testing.TearDown;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.io.FileUtils;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.storage.JobUpdateConfiguration;
import com.twitter.mesos.gen.storage.QuotaConfiguration;
import com.twitter.mesos.gen.storage.SchedulerMetadata;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.gen.storage.StoredJob;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.storage.SnapshotStore;
import com.twitter.mesos.scheduler.storage.backup.Recovery.RecoveryException;
import com.twitter.mesos.scheduler.storage.backup.Recovery.RecoveryImpl;
import com.twitter.mesos.scheduler.storage.backup.StorageBackup.StorageBackupImpl;
import com.twitter.mesos.scheduler.storage.backup.TemporaryStorage.TemporaryStorageFactory;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class RecoveryTest extends EasyMockTest {

  private static final Amount<Long, Time> INTERVAL = Amount.of(1L, Time.HOURS);
  private static final ScheduledTask TASK1 = makeTask("task1");
  private static final ScheduledTask TASK2 = makeTask("task2");
  private static final ScheduledTask TASK3 = makeTask("task3");
  private static final Snapshot SNAPSHOT1 = makeSnapshot(TASK1, TASK2);
  private static final Snapshot SNAPSHOT2 = makeSnapshot(TASK2, TASK3);
  private static final Snapshot SNAPSHOT3 = makeSnapshot(TASK3);

  private SnapshotStore<Snapshot> snapshotStore;
  private FakeClock clock;
  private StorageBackupImpl storageBackup;
  private RecoveryImpl recovery;

  @Before
  public void setUp() {
    final File backupDir = FileUtils.createTempDir();
    addTearDown(new TearDown() {
      @Override public void tearDown() throws Exception {
        org.apache.commons.io.FileUtils.deleteDirectory(backupDir);
      }
    });
    backupDir.deleteOnExit();
    snapshotStore = createMock(new Clazz<SnapshotStore<Snapshot>>() { });
    clock = new FakeClock();
    TemporaryStorageFactory factory = new TemporaryStorageFactory();
    storageBackup = new StorageBackupImpl(snapshotStore, clock, INTERVAL, backupDir);
    recovery = new RecoveryImpl(backupDir, snapshotStore, factory);
  }

  @Test
  public void testRecover() throws Exception {
    expect(snapshotStore.createSnapshot()).andReturn(SNAPSHOT1);
    snapshotStore.applySnapshot(SNAPSHOT1);

    control.replay();

    assertEquals(ImmutableSet.of(), recovery.listBackups());

    clock.advance(INTERVAL);
    storageBackup.createSnapshot();
    String backup1 = storageBackup.createBackupName();
    assertEquals(ImmutableSet.of(backup1), recovery.listBackups());

    recovery.stage(backup1);
    assertEquals(SNAPSHOT1.getTasks(), recovery.query(Query.GET_ALL));
    recovery.commit();
  }

  @Test
  public void testModifySnapshotBeforeCommit() throws Exception {
    expect(snapshotStore.createSnapshot()).andReturn(SNAPSHOT1);
    Snapshot modified = SNAPSHOT1.deepCopy().setTasks(ImmutableSet.of(TASK1));
    snapshotStore.applySnapshot(modified);

    control.replay();

    clock.advance(INTERVAL);
    storageBackup.createSnapshot();
    String backup1 = storageBackup.createBackupName();
    recovery.stage(backup1);
    assertEquals(SNAPSHOT1.getTasks(), recovery.query(Query.GET_ALL));
    recovery.deleteTasks(Query.byId(Tasks.id(TASK2)));
    assertEquals(modified.getTasks(), recovery.query(Query.GET_ALL));
    recovery.commit();
  }

  @Test
  public void testLoadOverwrite() throws Exception {
    expect(snapshotStore.createSnapshot()).andReturn(SNAPSHOT1);
    snapshotStore.applySnapshot(SNAPSHOT1);
    expect(snapshotStore.createSnapshot()).andReturn(SNAPSHOT2);
    snapshotStore.applySnapshot(SNAPSHOT2);
    expect(snapshotStore.createSnapshot()).andReturn(SNAPSHOT3);

    control.replay();

    clock.advance(INTERVAL);
    storageBackup.createSnapshot();
    String backup1 = storageBackup.createBackupName();
    recovery.stage(backup1);
    recovery.commit();

    clock.advance(INTERVAL);
    storageBackup.createSnapshot();
    String backup2 = storageBackup.createBackupName();
    recovery.stage(backup2);
    assertEquals(SNAPSHOT2.getTasks(), recovery.query(Query.GET_ALL));
    recovery.commit();

    clock.advance(INTERVAL);
    storageBackup.createSnapshot();
    String backup3 = storageBackup.createBackupName();
    recovery.stage(backup3);
    recovery.unload();
  }

  @Test(expected = RecoveryException.class)
  public void testLoadUnknownBackup() throws Exception {
    control.replay();
    recovery.stage("foo");
  }

  @Test(expected = RecoveryException.class)
  public void backupNotLoaded() throws Exception {
    control.replay();
    recovery.commit();
  }

  private static Snapshot makeSnapshot(ScheduledTask... tasks) {
    return new Snapshot()
        .setHostAttributes(ImmutableSet.<HostAttributes>of())
        .setJobs(ImmutableSet.<StoredJob>of())
        .setSchedulerMetadata(new SchedulerMetadata("frameworkId"))
        .setUpdateConfigurations(ImmutableSet.<JobUpdateConfiguration>of())
        .setQuotaConfigurations(ImmutableSet.<QuotaConfiguration>of())
        .setTasks(ImmutableSet.<ScheduledTask>builder().add(tasks).build());
  }

  private static ScheduledTask makeTask(String taskId) {
    return new ScheduledTask().setAssignedTask(
        new AssignedTask()
            .setTaskId(taskId)
            .setTask(new TwitterTaskInfo()
                .setOwner(new Identity().setRole("role-" + taskId).setUser("user-" + taskId))));
  }
}
