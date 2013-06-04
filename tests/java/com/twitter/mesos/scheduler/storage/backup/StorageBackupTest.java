package com.twitter.mesos.scheduler.storage.backup;

import java.io.File;
import java.util.List;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.testing.TearDown;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.io.FileUtils;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.storage.QuotaConfiguration;
import com.twitter.mesos.gen.storage.SchedulerMetadata;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.gen.storage.StoredJob;
import com.twitter.mesos.scheduler.storage.SnapshotStore;
import com.twitter.mesos.scheduler.storage.backup.StorageBackup.StorageBackupImpl;
import com.twitter.mesos.scheduler.storage.backup.StorageBackup.StorageBackupImpl.BackupConfig;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class StorageBackupTest extends EasyMockTest {

  private static final int MAX_BACKUPS = 5;
  private static final Amount<Long, Time> INTERVAL = Amount.of(1L, Time.HOURS);

  private SnapshotStore<Snapshot> delegate;
  private FakeClock clock;
  private BackupConfig config;
  private StorageBackupImpl storageBackup;

  @Before
  public void setUp() {
    delegate = createMock(new Clazz<SnapshotStore<Snapshot>>() { });
    clock = new FakeClock();
    final File backupDir = FileUtils.createTempDir();
    addTearDown(new TearDown() {
      @Override public void tearDown() throws Exception {
        org.apache.commons.io.FileUtils.deleteDirectory(backupDir);
      }
    });
    config = new BackupConfig(backupDir, MAX_BACKUPS, INTERVAL);
    clock.advance(Amount.of(365 * 30L, Time.DAYS));
    storageBackup = new StorageBackupImpl(delegate, clock, config);
  }

  @Test
  public void testBackup() throws Exception {
    Snapshot snapshot = makeSnapshot();
    expect(delegate.createSnapshot()).andReturn(snapshot).times(3);

    control.replay();

    assertEquals(snapshot, storageBackup.createSnapshot());
    assertBackupCount(0);
    clock.advance(Amount.of(INTERVAL.as(Time.MILLISECONDS) - 1, Time.MILLISECONDS));
    assertEquals(snapshot, storageBackup.createSnapshot());
    assertBackupCount(0);
    clock.advance(Amount.of(1L, Time.MILLISECONDS));
    assertEquals(snapshot, storageBackup.createSnapshot());
    assertBackupCount(1);
    assertEquals(1, storageBackup.successes.get());

    Snapshot restored = ThriftBinaryCodec.decode(
        Snapshot.class,
        Files.toByteArray(config.dir.listFiles()[0]));
    assertEquals(snapshot, restored);
  }

  @Test
  public void testDirectoryMissing() {
    Snapshot snapshot = makeSnapshot();
    expect(delegate.createSnapshot()).andReturn(snapshot).times(1);

    control.replay();

    clock.advance(INTERVAL);
    config.dir.delete();
    assertEquals(snapshot, storageBackup.createSnapshot());
    assertEquals(1, storageBackup.failures.get());
  }

  @Test
  public void testOldBackupsDeleted() {
    Snapshot snapshot = makeSnapshot();
    expect(delegate.createSnapshot()).andReturn(snapshot).times(MAX_BACKUPS + 1);

    control.replay();

    ImmutableList.Builder<String> nameBuilder = ImmutableList.builder();
    for (int i = 0; i < MAX_BACKUPS; i++) {
      clock.advance(Amount.of(INTERVAL.as(Time.MILLISECONDS), Time.MILLISECONDS));
      assertEquals(snapshot, storageBackup.createSnapshot());
      nameBuilder.add(storageBackup.createBackupName());
      assertBackupCount(i + 1);
      assertEquals(i + 1, storageBackup.successes.get());
    }

    clock.advance(Amount.of(INTERVAL.as(Time.MILLISECONDS), Time.MILLISECONDS));
    assertEquals(snapshot, storageBackup.createSnapshot());
    nameBuilder.add(storageBackup.createBackupName());
    assertBackupCount(MAX_BACKUPS);
    assertEquals(MAX_BACKUPS + 1, storageBackup.successes.get());
    List<String> backupNames = nameBuilder.build();
    assertEquals(
        ImmutableSet.copyOf(backupNames.subList(1, backupNames.size())),
        FluentIterable.from(ImmutableList.copyOf(config.dir.listFiles()))
            .transform(StorageBackupImpl.FILE_NAME)
            .toSet());
  }

  @Test
  public void testInterval() {
    // Ensures that a long initial interval does not result in shortened subsequent intervals.
    Snapshot snapshot = makeSnapshot();
    expect(delegate.createSnapshot()).andReturn(snapshot).times(3);

    control.replay();

    assertEquals(snapshot, storageBackup.createSnapshot());
    assertBackupCount(0);
    clock.advance(Amount.of(INTERVAL.as(Time.MILLISECONDS) * 3, Time.MILLISECONDS));
    assertEquals(snapshot, storageBackup.createSnapshot());
    assertBackupCount(1);
    assertEquals(1, storageBackup.successes.get());
    assertEquals(snapshot, storageBackup.createSnapshot());
    assertBackupCount(1);
    assertEquals(1, storageBackup.successes.get());
  }

  private void assertBackupCount(int count) {
    assertEquals(count, config.dir.list().length);
  }

  private Snapshot makeSnapshot() {
    Snapshot snapshot = new Snapshot();
    snapshot.setTimestamp(clock.nowMillis());
    snapshot.setHostAttributes(ImmutableSet.of(
        new HostAttributes(
            "hostA",
            ImmutableSet.of(new Attribute("attr", ImmutableSet.of("value"))))));
    snapshot.setJobs(
        ImmutableSet.of(new StoredJob("jobManager", new JobConfiguration().setName("jobA"))));
    snapshot.setQuotaConfigurations(
        ImmutableSet.of(new QuotaConfiguration("roleA", new Quota(10, 1024, 1024))));
    snapshot.setSchedulerMetadata(new SchedulerMetadata("frameworkId"));
    snapshot.setTasks(ImmutableSet.of(new ScheduledTask()));
    return snapshot;
  }
}
