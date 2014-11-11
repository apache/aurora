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
package org.apache.aurora.scheduler.storage.log;

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.storage.QuotaConfiguration;
import org.apache.aurora.gen.storage.SchedulerMetadata;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.StoredJob;
import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.ResourceAggregates;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.apiConstants.CURRENT_API_VERSION;
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
    ImmutableSet<IScheduledTask> tasks = ImmutableSet.of(
        IScheduledTask.build(new ScheduledTask().setStatus(ScheduleStatus.PENDING)));
    Set<QuotaConfiguration> quotas =
        ImmutableSet.of(
            new QuotaConfiguration("steve", ResourceAggregates.none().newBuilder()));
    IHostAttributes attribute = IHostAttributes.build(
        new HostAttributes("host", ImmutableSet.of(new Attribute("attr", ImmutableSet.of("value"))))
            .setSlaveId("slave id"));
    // A legacy attribute that has a maintenance mode set, but nothing else.  These should be
    // dropped.
    IHostAttributes legacyAttribute = IHostAttributes.build(
        new HostAttributes("host", ImmutableSet.<Attribute>of()));
    StoredJob job = new StoredJob(
        "jobManager",
        new JobConfiguration().setKey(new JobKey("owner", "env", "name")));
    String frameworkId = "framework_id";
    ILock lock = ILock.build(new Lock()
        .setKey(LockKey.job(JobKeys.from("testRole", "testEnv", "testJob").newBuilder()))
        .setToken("lockId")
        .setUser("testUser")
        .setTimestampMs(12345L));
    SchedulerMetadata metadata = new SchedulerMetadata()
        .setFrameworkId(frameworkId)
        .setVersion(CURRENT_API_VERSION);
    final String updateId1 = "updateId1";
    final String updateId2 = "updateId2";
    IJobUpdateDetails updateDetails1 = IJobUpdateDetails.build(new JobUpdateDetails()
        .setUpdate(new JobUpdate().setSummary(new JobUpdateSummary().setUpdateId(updateId1)))
        .setUpdateEvents(ImmutableList.of(new JobUpdateEvent().setStatus(JobUpdateStatus.ERROR)))
        .setInstanceEvents(ImmutableList.of(new JobInstanceUpdateEvent().setTimestampMs(123L))));

    IJobUpdateDetails updateDetails2 = IJobUpdateDetails.build(new JobUpdateDetails()
        .setUpdate(new JobUpdate().setSummary(new JobUpdateSummary().setUpdateId(updateId2))));

    storageUtil.expectOperations();
    expect(storageUtil.taskStore.fetchTasks(Query.unscoped())).andReturn(tasks);
    expect(storageUtil.quotaStore.fetchQuotas())
        .andReturn(ImmutableMap.of("steve", ResourceAggregates.none()));
    expect(storageUtil.attributeStore.getHostAttributes())
        .andReturn(ImmutableSet.of(attribute, legacyAttribute));
    expect(storageUtil.jobStore.fetchManagerIds()).andReturn(ImmutableSet.of("jobManager"));
    expect(storageUtil.jobStore.fetchJobs("jobManager"))
        .andReturn(ImmutableSet.of(IJobConfiguration.build(job.getJobConfiguration())));
    expect(storageUtil.schedulerStore.fetchFrameworkId()).andReturn(Optional.of(frameworkId));
    expect(storageUtil.lockStore.fetchLocks()).andReturn(ImmutableSet.of(lock));
    String lockToken = "token";
    expect(storageUtil.jobUpdateStore.fetchAllJobUpdateDetails())
        .andReturn(ImmutableSet.of(
            new StoredJobUpdateDetails(updateDetails1.newBuilder(), lockToken),
            new StoredJobUpdateDetails(updateDetails2.newBuilder(), null)));

    expectDataWipe();
    storageUtil.taskStore.saveTasks(tasks);
    storageUtil.quotaStore.saveQuota("steve", ResourceAggregates.none());
    expect(storageUtil.attributeStore.saveHostAttributes(attribute)).andReturn(true);
    storageUtil.jobStore.saveAcceptedJob(
        job.getJobManagerId(),
        IJobConfiguration.build(job.getJobConfiguration()));
    storageUtil.schedulerStore.saveFrameworkId(frameworkId);
    storageUtil.lockStore.saveLock(lock);
    storageUtil.jobUpdateStore.saveJobUpdate(
        updateDetails1.getUpdate(), Optional.fromNullable(lockToken));
    storageUtil.jobUpdateStore.saveJobUpdateEvent(
        Iterables.getOnlyElement(updateDetails1.getUpdateEvents()),
        updateId1);
    storageUtil.jobUpdateStore.saveJobInstanceUpdateEvent(
        Iterables.getOnlyElement(updateDetails1.getInstanceEvents()),
        updateId1);
    storageUtil.jobUpdateStore.saveJobUpdate(
        updateDetails2.getUpdate(), Optional.<String>absent());

    control.replay();

    Snapshot expected = new Snapshot()
        .setTimestamp(NOW)
        .setTasks(IScheduledTask.toBuildersSet(tasks))
        .setQuotaConfigurations(quotas)
        .setHostAttributes(ImmutableSet.of(attribute.newBuilder(), legacyAttribute.newBuilder()))
        .setJobs(ImmutableSet.of(job))
        .setSchedulerMetadata(metadata)
        .setLocks(ImmutableSet.of(lock.newBuilder()))
        .setJobUpdateDetails(ImmutableSet.of(
            new StoredJobUpdateDetails(updateDetails1.newBuilder(), lockToken),
            new StoredJobUpdateDetails(updateDetails2.newBuilder(), null)));

    assertEquals(expected, snapshotStore.createSnapshot());

    snapshotStore.applySnapshot(expected);
  }

  private void expectDataWipe() {
    storageUtil.taskStore.deleteAllTasks();
    storageUtil.quotaStore.deleteQuotas();
    storageUtil.attributeStore.deleteHostAttributes();
    storageUtil.jobStore.deleteJobs();
    storageUtil.lockStore.deleteLocks();
    storageUtil.jobUpdateStore.deleteAllUpdatesAndEvents();
  }
}
