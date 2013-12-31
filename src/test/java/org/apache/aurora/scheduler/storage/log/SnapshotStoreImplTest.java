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
package com.twitter.aurora.scheduler.storage.log;

import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.Attribute;
import com.twitter.aurora.gen.HostAttributes;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.Lock;
import com.twitter.aurora.gen.LockKey;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.storage.QuotaConfiguration;
import com.twitter.aurora.gen.storage.SchedulerMetadata;
import com.twitter.aurora.gen.storage.Snapshot;
import com.twitter.aurora.gen.storage.StoredJob;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.quota.Quotas;
import com.twitter.aurora.scheduler.storage.SnapshotStore;
import com.twitter.aurora.scheduler.storage.entities.IJobConfiguration;
import com.twitter.aurora.scheduler.storage.entities.ILock;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.aurora.scheduler.storage.testing.StorageTestUtil;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import static com.twitter.aurora.gen.apiConstants.CURRENT_API_VERSION;

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
        ImmutableSet.of(new QuotaConfiguration("steve", Quotas.noQuota().newBuilder()));
    HostAttributes attribute = new HostAttributes("host",
        ImmutableSet.of(new Attribute("attr", ImmutableSet.of("value"))));
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


    storageUtil.expectOperations();
    expect(storageUtil.taskStore.fetchTasks(Query.unscoped())).andReturn(tasks);
    expect(storageUtil.quotaStore.fetchQuotas())
        .andReturn(ImmutableMap.of("steve", Quotas.noQuota()));
    expect(storageUtil.attributeStore.getHostAttributes()).andReturn(ImmutableSet.of(attribute));
    expect(storageUtil.jobStore.fetchManagerIds()).andReturn(ImmutableSet.of("jobManager"));
    expect(storageUtil.jobStore.fetchJobs("jobManager"))
        .andReturn(ImmutableSet.of(IJobConfiguration.build(job.getJobConfiguration())));
    expect(storageUtil.schedulerStore.fetchFrameworkId()).andReturn(frameworkId);
    expect(storageUtil.lockStore.fetchLocks()).andReturn(ImmutableSet.of(lock));

    expectDataWipe();
    storageUtil.taskStore.saveTasks(tasks);
    storageUtil.quotaStore.saveQuota("steve", Quotas.noQuota());
    storageUtil.attributeStore.saveHostAttributes(attribute);
    storageUtil.jobStore.saveAcceptedJob(
        job.getJobManagerId(),
        IJobConfiguration.build(job.getJobConfiguration()));
    storageUtil.schedulerStore.saveFrameworkId(frameworkId);
    storageUtil.lockStore.saveLock(lock);

    control.replay();

    Snapshot expected = new Snapshot()
        .setTimestamp(NOW)
        .setTasks(IScheduledTask.toBuildersSet(tasks))
        .setQuotaConfigurations(quotas)
        .setHostAttributes(ImmutableSet.of(attribute))
        .setJobs(ImmutableSet.of(job))
        .setSchedulerMetadata(metadata)
        .setLocks(ILock.toBuildersSet(ImmutableSet.of(lock)));

    assertEquals(expected, snapshotStore.createSnapshot());

    snapshotStore.applySnapshot(expected);
  }

  private void expectDataWipe() {
    storageUtil.taskStore.deleteAllTasks();
    storageUtil.quotaStore.deleteQuotas();
    storageUtil.attributeStore.deleteHostAttributes();
    storageUtil.jobStore.deleteJobs();
    storageUtil.lockStore.deleteLocks();
  }
}
