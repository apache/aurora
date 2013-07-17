package com.twitter.aurora.scheduler.storage.log;

import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.Attribute;
import com.twitter.aurora.gen.HostAttributes;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobUpdateConfiguration;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskUpdateConfiguration;
import com.twitter.aurora.gen.storage.QuotaConfiguration;
import com.twitter.aurora.gen.storage.SchedulerMetadata;
import com.twitter.aurora.gen.storage.Snapshot;
import com.twitter.aurora.gen.storage.StoredJob;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.quota.Quotas;
import com.twitter.aurora.scheduler.storage.SnapshotStore;
import com.twitter.aurora.scheduler.storage.testing.StorageTestUtil;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class SnapshotStoreImplTest extends EasyMockTest {

  private static final long NOW = 10335463456L;

  private StorageTestUtil storageUtil;
  private SnapshotStore<Snapshot> snapshotStore;

  @Before
  public void setUp() {
    FakeClock clock = new FakeClock();
    clock.setNowMillis(NOW);
    storageUtil = new StorageTestUtil(this);
    snapshotStore = new SnapshotStoreImpl(clock, storageUtil.storage);
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
    JobUpdateConfiguration update = new JobUpdateConfiguration()
        .setUpdateToken("token")
        .setConfigs(ImmutableSet.<TaskUpdateConfiguration>of())
        .setRoleDeprecated("role")
        .setJobDeprecated("job");
    String frameworkId = "framework_id";

    storageUtil.expectOperations();
    expect(storageUtil.taskStore.fetchTasks(Query.GET_ALL)).andReturn(tasks);
    expect(storageUtil.quotaStore.fetchQuotas())
        .andReturn(ImmutableMap.of("steve", Quotas.NO_QUOTA));
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

  private void expectDataWipe() {
    storageUtil.taskStore.deleteAllTasks();
    storageUtil.quotaStore.deleteQuotas();
    storageUtil.attributeStore.deleteHostAttributes();
    storageUtil.jobStore.deleteJobs();
    storageUtil.updateStore.deleteShardUpdateConfigs();
  }
}
