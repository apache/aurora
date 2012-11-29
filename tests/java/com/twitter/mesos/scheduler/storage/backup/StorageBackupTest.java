package com.twitter.mesos.scheduler.storage.backup;

import java.io.File;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;

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

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class StorageBackupTest extends EasyMockTest {

  private static final Amount<Long, Time> INTERVAL = Amount.of(1L, Time.HOURS);

  private SnapshotStore<Snapshot> delegate;
  private FakeClock clock;
  private File backupDir;
  private StorageBackupImpl storageBackup;

  @Before
  public void setUp() {
    delegate = createMock(new Clazz<SnapshotStore<Snapshot>>() { });
    clock = new FakeClock();
    backupDir = FileUtils.createTempDir();
    clock.advance(Amount.of(365 * 30L, Time.DAYS));
    storageBackup = new StorageBackupImpl(delegate, clock, INTERVAL, backupDir);
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
        Files.toByteArray(backupDir.listFiles()[0]));
    assertEquals(snapshot, restored);
  }

  @Test
  public void testDirectoryMissing() {
    Snapshot snapshot = makeSnapshot();
    expect(delegate.createSnapshot()).andReturn(snapshot).times(1);

    control.replay();

    clock.advance(INTERVAL);
    backupDir.delete();
    assertEquals(snapshot, storageBackup.createSnapshot());
    assertEquals(1, storageBackup.failures.get());
  }

  private void assertBackupCount(int count) {
    assertEquals(count, backupDir.list().length);
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
