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
package org.apache.aurora.scheduler.storage.backup;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeBuildInfo;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.storage.SchedulerMetadata;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.Snapshotter;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.backup.Recovery.RecoveryException;
import org.apache.aurora.scheduler.storage.backup.Recovery.RecoveryImpl;
import org.apache.aurora.scheduler.storage.backup.StorageBackup.StorageBackupImpl;
import org.apache.aurora.scheduler.storage.backup.StorageBackup.StorageBackupImpl.BackupConfig;
import org.apache.aurora.scheduler.storage.backup.TemporaryStorage.TemporaryStorageFactory;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.testing.FakeScheduledExecutor;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class RecoveryTest extends EasyMockTest {

  private static final Amount<Long, Time> INTERVAL = Amount.of(1L, Time.HOURS);
  private static final IScheduledTask TASK1 = TaskTestUtil.makeTask("task1", TaskTestUtil.JOB);
  private static final IScheduledTask TASK2 = TaskTestUtil.makeTask("task2", TaskTestUtil.JOB);
  private static final Snapshot SNAPSHOT1 = makeSnapshot(TASK1, TASK2);

  private Snapshotter snapshotter;
  private SnapshotStore distributedStore;
  private Storage primaryStorage;
  private MutableStoreProvider storeProvider;
  private Command shutDownNow;
  private FakeClock clock;
  private StorageBackupImpl storageBackup;
  private RecoveryImpl recovery;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    final File backupDir = temporaryFolder.newFolder();
    snapshotter = createMock(Snapshotter.class);
    distributedStore = createMock(SnapshotStore.class);
    primaryStorage = createMock(Storage.class);
    storeProvider = createMock(MutableStoreProvider.class);
    shutDownNow = createMock(Command.class);
    ScheduledExecutorService executor = createMock(ScheduledExecutorService.class);
    clock = FakeScheduledExecutor.scheduleExecutor(executor);
    TemporaryStorageFactory factory =
        new TemporaryStorageFactory(TaskTestUtil.THRIFT_BACKFILL);
    storageBackup = new StorageBackupImpl(
        primaryStorage,
        snapshotter,
        clock,
        new BackupConfig(backupDir, 5, INTERVAL),
        executor);

    recovery = new RecoveryImpl(backupDir, factory, primaryStorage, distributedStore, shutDownNow);
  }

  @Test
  public void testRecover() throws Exception {
    expect(snapshotter.from(anyObject())).andReturn(SNAPSHOT1);
    Capture<MutateWork<Object, Exception>> transaction = createCapture();
    expect(primaryStorage.write(capture(transaction))).andReturn(null);
    Capture<Snapshot> snapshot = createCapture();
    distributedStore.snapshotWith(capture(snapshot));
    shutDownNow.execute();

    control.replay();

    assertEquals(ImmutableSet.of(), recovery.listBackups());

    clock.advance(INTERVAL);
    storageBackup.from(storeProvider);
    String backup1 = storageBackup.createBackupName();
    assertEquals(ImmutableSet.of(backup1), recovery.listBackups());

    recovery.stage(backup1);
    assertEquals(
        IScheduledTask.setFromBuilders(SNAPSHOT1.getTasks()),
        ImmutableSet.copyOf(recovery.query(Query.unscoped())));
    recovery.commit();
    transaction.getValue().apply(storeProvider);

    assertEquals(SNAPSHOT1, snapshot.getValue());
  }

  @Test
  public void testModifySnapshotBeforeCommit() throws Exception {
    expect(snapshotter.from(anyObject())).andReturn(SNAPSHOT1);
    Snapshot modified = SNAPSHOT1.deepCopy().setTasks(ImmutableSet.of(TASK1.newBuilder()));
    Capture<MutateWork<Object, Exception>> transaction = createCapture();
    expect(primaryStorage.write(capture(transaction))).andReturn(null);
    Capture<Snapshot> snapshot = createCapture();
    distributedStore.snapshotWith(capture(snapshot));
    shutDownNow.execute();

    control.replay();

    clock.advance(INTERVAL);
    storageBackup.from(storeProvider);
    String backup1 = storageBackup.createBackupName();
    recovery.stage(backup1);
    assertEquals(
        IScheduledTask.setFromBuilders(SNAPSHOT1.getTasks()),
        ImmutableSet.copyOf(recovery.query(Query.unscoped())));
    recovery.deleteTasks(Query.taskScoped(Tasks.id(TASK2)));
    assertEquals(
        IScheduledTask.setFromBuilders(modified.getTasks()),
        ImmutableSet.copyOf(recovery.query(Query.unscoped())));
    recovery.commit();
    transaction.getValue().apply(storeProvider);

    assertEquals(modified, snapshot.getValue());
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

  private static Snapshot makeSnapshot(IScheduledTask... tasks) {
    SchedulerMetadata metadata = new SchedulerMetadata()
        .setDetails(ImmutableMap.of(
            FakeBuildInfo.DATE, FakeBuildInfo.DATE,
            FakeBuildInfo.GIT_REVISION, FakeBuildInfo.GIT_REVISION,
            FakeBuildInfo.GIT_TAG, FakeBuildInfo.GIT_TAG));

    return new Snapshot()
        .setHostAttributes(ImmutableSet.of())
        .setQuotaConfigurations(ImmutableSet.of())
        .setJobUpdateDetails(ImmutableSet.of())
        .setCronJobs(ImmutableSet.of())
        .setSchedulerMetadata(metadata)
        .setTasks(IScheduledTask.toBuildersSet(ImmutableSet.copyOf(tasks)));
  }
}
