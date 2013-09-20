/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.aurora.scheduler.storage.backup;

import java.io.File;

import com.google.common.collect.ImmutableSet;
import com.google.common.testing.TearDown;

import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.HostAttributes;
import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.JobUpdateConfiguration;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.storage.QuotaConfiguration;
import com.twitter.aurora.gen.storage.SchedulerMetadata;
import com.twitter.aurora.gen.storage.Snapshot;
import com.twitter.aurora.gen.storage.StoredJob;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.storage.DistributedSnapshotStore;
import com.twitter.aurora.scheduler.storage.SnapshotStore;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.MutateWork;
import com.twitter.aurora.scheduler.storage.backup.Recovery.RecoveryException;
import com.twitter.aurora.scheduler.storage.backup.Recovery.RecoveryImpl;
import com.twitter.aurora.scheduler.storage.backup.StorageBackup.StorageBackupImpl;
import com.twitter.aurora.scheduler.storage.backup.StorageBackup.StorageBackupImpl.BackupConfig;
import com.twitter.aurora.scheduler.storage.backup.TemporaryStorage.TemporaryStorageFactory;
import com.twitter.common.base.Command;
import com.twitter.common.io.FileUtils;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class RecoveryTest extends EasyMockTest {

  private static final Amount<Long, Time> INTERVAL = Amount.of(1L, Time.HOURS);
  private static final ScheduledTask TASK1 = makeTask("task1");
  private static final ScheduledTask TASK2 = makeTask("task2");
  private static final Snapshot SNAPSHOT1 = makeSnapshot(TASK1, TASK2);

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
    assertEquals(SNAPSHOT1.getTasks(), recovery.query(Query.unscoped()));
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
    assertEquals(SNAPSHOT1.getTasks(), recovery.query(Query.unscoped()));
    recovery.deleteTasks(Query.taskScoped(Tasks.id(TASK2)));
    assertEquals(modified.getTasks(), recovery.query(Query.unscoped()));
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
            .setTask(new TaskConfig()
                .setJobName("job-" + taskId)
                .setEnvironment("test")
                .setOwner(new Identity().setRole("role-" + taskId).setUser("user-" + taskId))));
  }
}
