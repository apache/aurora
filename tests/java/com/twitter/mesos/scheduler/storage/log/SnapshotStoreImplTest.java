package com.twitter.mesos.scheduler.storage.log;

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.storage.JobUpdateConfiguration;
import com.twitter.mesos.gen.storage.QuotaConfiguration;
import com.twitter.mesos.gen.storage.SchedulerMetadata;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.gen.storage.StoredJob;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.quota.Quotas;
import com.twitter.mesos.scheduler.storage.SnapshotStore;
import com.twitter.mesos.scheduler.storage.testing.StorageTestUtil;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

/**
 * @author William Farner
 */
public class SnapshotStoreImplTest extends EasyMockTest {

  private static final long NOW = 10335463456L;

  private SnapshotStore<byte[]> binarySnapshotStore;
  private StorageTestUtil storageUtil;
  private SnapshotStore<Snapshot> snapshotStore;

  @Before
  public void setUp() {
    FakeClock clock = new FakeClock();
    clock.setNowMillis(NOW);
    binarySnapshotStore = createMock(new Clazz<SnapshotStore<byte[]>>() {});
    storageUtil = new StorageTestUtil(this);
    snapshotStore = new SnapshotStoreImpl(clock, binarySnapshotStore, storageUtil.storage);
  }

  @Test
  public void testRestoreOldSnapshot() {
    byte[] snapshotData = "binary snapshot".getBytes();

    storageUtil.expectTransactions();
    storageUtil.attributeStore.deleteHostAttributes();
    binarySnapshotStore.applySnapshot(snapshotData);

    control.replay();

    snapshotStore.applySnapshot(new Snapshot()
        .setDataDEPRECATED(snapshotData));
  }

  @Test
  public void testCreateAndRestoreNewSnapshot() {
    ImmutableSet<ScheduledTask> tasks =
        ImmutableSet.of(new ScheduledTask().setStatus(ScheduleStatus.PENDING));
    Set<QuotaConfiguration> quotas =
        ImmutableSet.of(new QuotaConfiguration("steve", Quotas.NO_QUOTA));
    HostAttributes attribute = new HostAttributes("host",
        ImmutableSet.of(new Attribute("attr", ImmutableSet.of("value"))));
    StoredJob job = new StoredJob("jobManager", new JobConfiguration().setName("name"));
    JobUpdateConfiguration update = new JobUpdateConfiguration("role", "job", "token",
        ImmutableSet.<TaskUpdateConfiguration>of());
    String frameworkId = "framework_id";

    storageUtil.expectTransactions();
    expect(storageUtil.taskStore.fetchTasks(Query.GET_ALL)).andReturn(tasks);
    expect(storageUtil.quotaStore.fetchQuotaRoles()).andReturn(ImmutableSet.of("steve"));
    expect(storageUtil.quotaStore.fetchQuota("steve")).andReturn(Optional.of(Quotas.NO_QUOTA));
    expect(storageUtil.attributeStore.getHostAttributes()).andReturn(ImmutableSet.of(attribute));
    expect(storageUtil.jobStore.fetchManagerIds()).andReturn(ImmutableSet.of("jobManager"));
    expect(storageUtil.jobStore.fetchJobs("jobManager"))
        .andReturn(ImmutableSet.of(job.getJobConfiguration()));
    expect(storageUtil.updateStore.fetchUpdatingRoles()).andReturn(ImmutableSet.of("role"));
    expect(storageUtil.updateStore.fetchUpdateConfigs("role")).andReturn(ImmutableSet.of(update));
    expect(storageUtil.schedulerStore.fetchFrameworkId()).andReturn(frameworkId);

    expectDataWipe();
    storageUtil.taskStore.saveTasks(tasks);
    storageUtil.quotaStore.saveQuota("steve", Quotas.NO_QUOTA);
    storageUtil.attributeStore.saveHostAttributes(attribute);
    storageUtil.jobStore.saveAcceptedJob(job.getJobManagerId(), job.getJobConfiguration());
    storageUtil.updateStore.saveJobUpdateConfig(update);
    storageUtil.schedulerStore.saveFrameworkId(frameworkId);

    control.replay();

    Snapshot expected = new Snapshot()
        .setTimestamp(NOW)
        .setTasks(tasks)
        .setQuotaConfigurations(quotas)
        .setHostAttributes(ImmutableSet.of(attribute))
        .setJobs(ImmutableSet.of(job))
        .setUpdateConfigurations(ImmutableSet.of(update))
        .setSchedulerMetadata(new SchedulerMetadata(frameworkId));
    assertEquals(expected, snapshotStore.createSnapshot());

    snapshotStore.applySnapshot(expected);
  }

  @Test(expected = IllegalStateException.class)
  public void testRestoreMixedSnapshot() {
    storageUtil.expectTransactions();

    control.replay();

    snapshotStore.applySnapshot(new Snapshot()
        .setDataDEPRECATED("binary snapshot".getBytes())
        .setTasks(ImmutableSet.<ScheduledTask>of()));
  }

  private void expectDataWipe() {
    storageUtil.taskStore.removeTasks(Query.GET_ALL);
    storageUtil.quotaStore.deleteQuotas();
    storageUtil.attributeStore.deleteHostAttributes();
    storageUtil.jobStore.deleteJobs();
    storageUtil.updateStore.deleteShardUpdateConfigs();
  }
}
