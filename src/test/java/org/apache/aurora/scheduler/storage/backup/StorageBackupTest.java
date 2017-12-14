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
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.storage.QuotaConfiguration;
import org.apache.aurora.gen.storage.SchedulerMetadata;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.StoredCronJob;
import org.apache.aurora.scheduler.storage.Snapshotter;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.backup.StorageBackup.StorageBackupImpl;
import org.apache.aurora.scheduler.storage.backup.StorageBackup.StorageBackupImpl.BackupConfig;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
import org.apache.aurora.scheduler.testing.FakeScheduledExecutor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.aurora.scheduler.resources.ResourceTestUtil.aggregate;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StorageBackupTest extends EasyMockTest {

  private static final int MAX_BACKUPS = 5;
  private static final Amount<Long, Time> INTERVAL = Amount.of(1L, Time.HOURS);

  private Storage storage;
  private Snapshotter delegate;
  private FakeClock clock;
  private BackupConfig config;
  private StorageBackupImpl storageBackup;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    storage = MemStorageModule.newEmptyStorage();
    delegate = createMock(Snapshotter.class);
    final File backupDir = temporaryFolder.newFolder();
    ScheduledExecutorService executor = createMock(ScheduledExecutorService.class);
    clock = FakeScheduledExecutor.scheduleExecutor(executor);
    config = new BackupConfig(backupDir, MAX_BACKUPS, INTERVAL);
    clock.advance(Amount.of(365 * 30L, Time.DAYS));
    storageBackup = new StorageBackupImpl(storage, delegate, clock, config, executor);
  }

  private void triggerSnapshot(Snapshot expectedResult) {
    storage.write((NoResult.Quiet) stores ->
        assertEquals(expectedResult, storageBackup.from(stores)));
  }

  @Test
  public void testBackup() throws Exception {
    Snapshot snapshot = makeSnapshot();
    expect(delegate.from(anyObject())).andReturn(snapshot).times(3);

    control.replay();

    triggerSnapshot(snapshot);
    assertBackupCount(0);
    clock.advance(Amount.of(INTERVAL.as(Time.MILLISECONDS) - 1, Time.MILLISECONDS));
    triggerSnapshot(snapshot);
    assertBackupCount(0);
    clock.advance(Amount.of(1L, Time.MILLISECONDS));
    triggerSnapshot(snapshot);
    assertBackupCount(1);
    assertEquals(1, storageBackup.getSuccesses().get());

    @Nullable
    File[] files = config.getDir().listFiles();
    assertNotNull(files);

    Snapshot restored = ThriftBinaryCodec.decode(Snapshot.class, Files.toByteArray(files[0]));
    assertEquals(snapshot, restored);
  }

  @Test
  public void testDirectoryMissing() {
    Snapshot snapshot = makeSnapshot();
    expect(delegate.from(anyObject())).andReturn(snapshot).times(1);

    control.replay();

    clock.advance(INTERVAL);
    config.getDir().delete();
    triggerSnapshot(snapshot);
    assertEquals(1, storageBackup.getFailures().get());
  }

  @Test
  public void testOldBackupsDeleted() {
    Snapshot snapshot = makeSnapshot();
    expect(delegate.from(anyObject())).andReturn(snapshot).times(MAX_BACKUPS + 1);

    control.replay();

    ImmutableList.Builder<String> nameBuilder = ImmutableList.builder();
    for (int i = 0; i < MAX_BACKUPS; i++) {
      clock.advance(Amount.of(INTERVAL.as(Time.MILLISECONDS), Time.MILLISECONDS));
      triggerSnapshot(snapshot);
      nameBuilder.add(storageBackup.createBackupName());
      assertBackupCount(i + 1);
      assertEquals(i + 1, storageBackup.getSuccesses().get());
    }

    clock.advance(Amount.of(INTERVAL.as(Time.MILLISECONDS), Time.MILLISECONDS));
    triggerSnapshot(snapshot);
    nameBuilder.add(storageBackup.createBackupName());
    assertBackupCount(MAX_BACKUPS);
    assertEquals(MAX_BACKUPS + 1, storageBackup.getSuccesses().get());
    List<String> backupNames = nameBuilder.build();

    File[] files = config.getDir().listFiles();
    assertNotNull(files);
    assertEquals(
        ImmutableSet.copyOf(backupNames.subList(1, backupNames.size())),
        FluentIterable.from(ImmutableList.copyOf(files))
            .transform(StorageBackupImpl.FILE_NAME)
            .toSet());
  }

  @Test
  public void testInterval() {
    // Ensures that a long initial interval does not result in shortened subsequent intervals.
    Snapshot snapshot = makeSnapshot();
    expect(delegate.from(anyObject())).andReturn(snapshot).times(3);

    control.replay();

    triggerSnapshot(snapshot);
    assertBackupCount(0);
    clock.advance(Amount.of(INTERVAL.as(Time.MILLISECONDS) * 3, Time.MILLISECONDS));
    triggerSnapshot(snapshot);
    assertBackupCount(1);
    assertEquals(1, storageBackup.getSuccesses().get());
    triggerSnapshot(snapshot);
    assertBackupCount(1);
    assertEquals(1, storageBackup.getSuccesses().get());
  }

  private void assertBackupCount(int count) {
    assertEquals(count, config.getDir().list().length);
  }

  private Snapshot makeSnapshot() {
    Snapshot snapshot = new Snapshot();
    snapshot.setTimestamp(clock.nowMillis());
    snapshot.setHostAttributes(ImmutableSet.of(
        new HostAttributes(
            "hostA",
            ImmutableSet.of(new Attribute("attr", ImmutableSet.of("value"))))));
    snapshot.setCronJobs(ImmutableSet.of(
        new StoredCronJob(new JobConfiguration().setKey(new JobKey("owner", "env", "jobA")))));
    snapshot.setQuotaConfigurations(
        ImmutableSet.of(new QuotaConfiguration("roleA", aggregate(10, 1024, 1024).newBuilder())));
    snapshot.setSchedulerMetadata(new SchedulerMetadata().setFrameworkId("frameworkId"));
    snapshot.setTasks(ImmutableSet.of(new ScheduledTask()));
    return snapshot;
  }
}
