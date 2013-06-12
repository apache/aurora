package com.twitter.mesos.scheduler.storage.backup;

import java.io.File;

import com.google.common.collect.ImmutableSet;
import com.google.common.testing.TearDown;

import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.Command;
import com.twitter.common.io.FileUtils;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobUpdateConfiguration;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.storage.QuotaConfiguration;
import com.twitter.mesos.gen.storage.SchedulerMetadata;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.gen.storage.StoredJob;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.storage.DistributedSnapshotStore;
import com.twitter.mesos.scheduler.storage.SnapshotStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.backup.Recovery.RecoveryException;
import com.twitter.mesos.scheduler.storage.backup.Recovery.RecoveryImpl;
import com.twitter.mesos.scheduler.storage.backup.StorageBackup.StorageBackupImpl;
import com.twitter.mesos.scheduler.storage.backup.StorageBackup.StorageBackupImpl.BackupConfig;
import com.twitter.mesos.scheduler.storage.backup.TemporaryStorage.TemporaryStorageFactory;

import static org.easymock.EasyMock.capture;
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
  private DistributedSnapshotStore distributedStore;
  private Storage primaryStorage;
  private MutableStoreProvider storeProvider;
  private Command shutDownNow;
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
    snapshotStore = createMock(new Clazz<SnapshotStore<Snapshot>>() { });
    distributedStore = createMock(DistributedSnapshotStore.class);
    primaryStorage = createMock(Storage.class);
    storeProvider = createMock(MutableStoreProvider.class);
    shutDownNow = createMock(Command.class);
    clock = new FakeClock();
    TemporaryStorageFactory factory = new TemporaryStorageFactory();
    storageBackup =
        new StorageBackupImpl(snapshotStore, clock, new BackupConfig(backupDir, 5, INTERVAL));
    recovery = new RecoveryImpl(backupDir, factory, primaryStorage, distributedStore, shutDownNow);
  }

  @Test
  public void testRecover() throws Exception {
    expect(snapshotStore.createSnapshot()).andReturn(SNAPSHOT1);
    Capture<MutateWork<?, ?>> transaction = createCapture();
    expect(primaryStorage.write(capture(transaction))).andReturn(null);
    distributedStore.persist(SNAPSHOT1);
    shutDownNow.execute();

    control.replay();

    assertEquals(ImmutableSet.of(), recovery.listBackups());

    clock.advance(INTERVAL);
    storageBackup.createSnapshot();
    String backup1 = storageBackup.createBackupName();
    assertEquals(ImmutableSet.of(backup1), recovery.listBackups());

    recovery.stage(backup1);
    assertEquals(SNAPSHOT1.getTasks(), recovery.query(Query.GET_ALL));
    recovery.commit();
    transaction.getValue().apply(storeProvider);
  }

  @Test
  public void testModifySnapshotBeforeCommit() throws Exception {
    expect(snapshotStore.createSnapshot()).andReturn(SNAPSHOT1);
    Snapshot modified = SNAPSHOT1.deepCopy().setTasks(ImmutableSet.of(TASK1));
    Capture<MutateWork<?, ?>> transaction = createCapture();
    expect(primaryStorage.write(capture(transaction))).andReturn(null);
    distributedStore.persist(modified);
    shutDownNow.execute();

    control.replay();

    clock.advance(INTERVAL);
    storageBackup.createSnapshot();
    String backup1 = storageBackup.createBackupName();
    recovery.stage(backup1);
    assertEquals(SNAPSHOT1.getTasks(), recovery.query(Query.GET_ALL));
    recovery.deleteTasks(Query.byId(Tasks.id(TASK2)));
    assertEquals(modified.getTasks(), recovery.query(Query.GET_ALL));
    recovery.commit();
    transaction.getValue().apply(storeProvider);
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
