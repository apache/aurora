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

import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.util.testing.FakeBuildInfo;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateState;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.storage.QuotaConfiguration;
import org.apache.aurora.gen.storage.SchedulerMetadata;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.StoredCronJob;
import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.db.MigrationManager;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.mem.InMemStoresModule;
import org.junit.Test;

import static org.apache.aurora.common.inject.Bindings.KeyFactory.PLAIN;
import static org.apache.aurora.common.util.testing.FakeBuildInfo.generateBuildInfo;
import static org.apache.aurora.scheduler.resources.ResourceManager.aggregateFromBag;
import static org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import static org.apache.aurora.scheduler.storage.db.DbModule.testModuleWithWorkQueue;
import static org.apache.aurora.scheduler.storage.db.DbUtil.createStorage;
import static org.apache.aurora.scheduler.storage.db.DbUtil.createStorageInjector;
import static org.apache.aurora.scheduler.storage.log.SnapshotStoreImpl.ALL_H2_STORE_FIELDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SnapshotStoreImplIT {

  private static final long NOW = 10335463456L;
  private static final IJobKey JOB_KEY = JobKeys.from("role", "env", "job");

  private Storage storage;
  private SnapshotStore<Snapshot> snapshotStore;

  private void setUpStore(boolean dbTaskStore, Set<String> hydrateFields) {
    storage = dbTaskStore
        ? createStorage()
        : createStorageInjector(
        testModuleWithWorkQueue(PLAIN, Optional.of(new InMemStoresModule(PLAIN))))
        .getInstance(Storage.class);

    FakeClock clock = new FakeClock();
    clock.setNowMillis(NOW);
    snapshotStore = new SnapshotStoreImpl(
        generateBuildInfo(),
        clock,
        storage,
        dbTaskStore,
        hydrateFields,
        createStorageInjector(testModuleWithWorkQueue()).getInstance(MigrationManager.class),
        TaskTestUtil.THRIFT_BACKFILL);
  }

  private static Snapshot makeComparable(Snapshot snapshot) {
    Snapshot copy = snapshot.deepCopy();
    // Ignore DB snapshot. It will be tested by asserting the DB data.
    copy.unsetDbScript();
    copy.setExperimentalTaskStore(false);
    return copy;
  }

  @Test
  public void testNoDBTaskStore() {
    setUpStore(false, ALL_H2_STORE_FIELDS);
    populateStore();

    Snapshot snapshot1 = snapshotStore.createSnapshot();
    assertEquals(expected(), makeComparable(snapshot1));
    assertFalse(snapshot1.isExperimentalTaskStore());

    snapshotStore.applySnapshot(snapshot1);
    Snapshot snapshot2 = snapshotStore.createSnapshot();
    assertEquals(expected(), makeComparable(snapshot2));
    assertEquals(makeComparable(snapshot1), makeComparable(snapshot2));
  }

  @Test
  public void testMigrateToDBTaskStore() {
    setUpStore(false, ALL_H2_STORE_FIELDS);
    populateStore();

    Snapshot snapshot1 = snapshotStore.createSnapshot();
    assertEquals(expected(), makeComparable(snapshot1));
    assertFalse(snapshot1.isExperimentalTaskStore());

    setUpStore(true, ALL_H2_STORE_FIELDS);
    snapshotStore.applySnapshot(snapshot1);
    Snapshot snapshot2 = snapshotStore.createSnapshot();
    assertTrue(snapshot2.isExperimentalTaskStore());
    assertEquals(expected(), makeComparable(snapshot2));
    assertEquals(makeComparable(snapshot1), makeComparable(snapshot2));
  }

  @Test
  public void testMigrateFromDBTaskStore() {
    setUpStore(true, ALL_H2_STORE_FIELDS);
    populateStore();

    Snapshot snapshot1 = snapshotStore.createSnapshot();
    assertEquals(expected(), makeComparable(snapshot1));
    assertTrue(snapshot1.isExperimentalTaskStore());

    setUpStore(false, ALL_H2_STORE_FIELDS);
    snapshotStore.applySnapshot(snapshot1);
    Snapshot snapshot2 = snapshotStore.createSnapshot();
    assertFalse(snapshot2.isExperimentalTaskStore());
    assertEquals(expected(), makeComparable(snapshot2));
    assertEquals(makeComparable(snapshot1), makeComparable(snapshot2));
  }

  @Test
  public void testNonDefaultHydrateOptions() {
    setUpStore(false, ImmutableSet.of());
    populateStore();

    Snapshot snapshot = snapshotStore.createSnapshot();

    assertNull(snapshot.getHostAttributes());
    assertNull(snapshot.getJobUpdateDetails());
    assertNull(snapshot.getLocks());
    assertNull(snapshot.getQuotaConfigurations());
  }

  @Test
  public void testDBTaskStore() {
    setUpStore(true, ALL_H2_STORE_FIELDS);
    populateStore();

    Snapshot snapshot1 = snapshotStore.createSnapshot();
    assertEquals(expected(), makeComparable(snapshot1));
    assertTrue(snapshot1.isExperimentalTaskStore());

    snapshotStore.applySnapshot(snapshot1);
    Snapshot snapshot2 = snapshotStore.createSnapshot();
    assertEquals(expected(), makeComparable(snapshot2));
    assertEquals(makeComparable(snapshot1), makeComparable(snapshot2));
  }

  @Test
  public void testBackfill() {
    setUpStore(false, ALL_H2_STORE_FIELDS);
    snapshotStore.applySnapshot(makeNonBackfilled());

    Snapshot backfilled = snapshotStore.createSnapshot();
    assertEquals(expected(), makeComparable(backfilled));
  }

  private static final IScheduledTask TASK = TaskTestUtil.makeTask("id", JOB_KEY);
  private static final ITaskConfig TASK_CONFIG = TaskTestUtil.makeConfig(JOB_KEY);
  private static final IJobConfiguration CRON_JOB = IJobConfiguration.build(new JobConfiguration()
      .setKey(new JobKey("owner", "env", "name"))
      .setOwner(new Identity("user"))
      .setCronSchedule("* * * * *")
      .setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING)
      .setInstanceCount(1)
      .setTaskConfig(TASK_CONFIG.newBuilder()));
  private static final String ROLE = "role";
  private static final IResourceAggregate QUOTA =
      ThriftBackfill.backfillResourceAggregate(aggregateFromBag(ResourceBag.LARGE).newBuilder());
  private static final IHostAttributes ATTRIBUTES = IHostAttributes.build(
      new HostAttributes("host", ImmutableSet.of(new Attribute("attr", ImmutableSet.of("value"))))
          .setMode(MaintenanceMode.NONE)
          .setSlaveId("slave id"));
  private static final String FRAMEWORK_ID = "framework_id";
  private static final Map<String, String> METADATA = ImmutableMap.of(
          FakeBuildInfo.DATE, FakeBuildInfo.DATE,
          FakeBuildInfo.GIT_REVISION, FakeBuildInfo.GIT_REVISION,
          FakeBuildInfo.GIT_TAG, FakeBuildInfo.GIT_TAG);
  private static final ILock LOCK = ILock.build(new Lock()
      .setKey(LockKey.job(JobKeys.from("testRole", "testEnv", "testJob").newBuilder()))
      .setToken("lockId")
      .setUser("testUser")
      .setTimestampMs(12345L));
  private static final IJobUpdateKey UPDATE_ID =
      IJobUpdateKey.build(new JobUpdateKey(JOB_KEY.newBuilder(), "updateId1"));
  private static final IJobUpdateDetails UPDATE = IJobUpdateDetails.build(new JobUpdateDetails()
      .setUpdate(new JobUpdate()
          .setInstructions(new JobUpdateInstructions()
              .setDesiredState(new InstanceTaskConfig()
                  .setTask(TASK_CONFIG.newBuilder())
                  .setInstances(ImmutableSet.of(new Range(0, 7))))
              .setInitialState(ImmutableSet.of(
                  new InstanceTaskConfig()
                      .setInstances(ImmutableSet.of(new Range(0, 1)))
                      .setTask(TASK_CONFIG.newBuilder())))
              .setSettings(new JobUpdateSettings()
                  .setBlockIfNoPulsesAfterMs(500)
                  .setUpdateGroupSize(1)
                  .setMaxPerInstanceFailures(1)
                  .setMaxFailedInstances(1)
                  .setMinWaitInInstanceRunningMs(200)
                  .setRollbackOnFailure(true)
                  .setWaitForBatchCompletion(true)
                  .setUpdateOnlyTheseInstances(ImmutableSet.of(new Range(0, 0)))))
          .setSummary(new JobUpdateSummary()
              .setState(new JobUpdateState().setStatus(JobUpdateStatus.ERROR))
              .setUser("user")
              .setKey(UPDATE_ID.newBuilder())))
      .setUpdateEvents(ImmutableList.of(new JobUpdateEvent()
          .setUser("user")
          .setMessage("message")
          .setStatus(JobUpdateStatus.ERROR)))
      .setInstanceEvents(ImmutableList.of(new JobInstanceUpdateEvent()
          .setAction(JobUpdateAction.INSTANCE_UPDATED))));

  private Snapshot expected() {
    return new Snapshot()
        .setTimestamp(NOW)
        .setTasks(ImmutableSet.of(TASK.newBuilder()))
        .setQuotaConfigurations(ImmutableSet.of(new QuotaConfiguration(ROLE, QUOTA.newBuilder())))
        .setHostAttributes(ImmutableSet.of(ATTRIBUTES.newBuilder()))
        .setCronJobs(ImmutableSet.of(new StoredCronJob(CRON_JOB.newBuilder())))
        .setSchedulerMetadata(new SchedulerMetadata(FRAMEWORK_ID, METADATA))
        .setLocks(ImmutableSet.of(LOCK.newBuilder()))
        .setJobUpdateDetails(ImmutableSet.of(
            new StoredJobUpdateDetails(UPDATE.newBuilder(), LOCK.getToken())));
  }

  private Snapshot makeNonBackfilled() {
    Snapshot snapshot = expected();
    snapshot.getTasks().forEach(e -> e.getAssignedTask().getTask().unsetResources());
    snapshot.getCronJobs()
        .forEach(e -> e.getJobConfiguration().getTaskConfig().unsetResources());
    snapshot.getJobUpdateDetails()
        .forEach(e -> e.getDetails().getUpdate().getInstructions()
            .getDesiredState().getTask().unsetResources());
    snapshot.getJobUpdateDetails()
        .forEach(e -> e.getDetails().getUpdate().getInstructions()
            .getInitialState().forEach(i -> i.getTask().unsetResources()));

    return snapshot;
  }

  private void populateStore() {
    storage.write((NoResult.Quiet) store -> {
      store.getUnsafeTaskStore().saveTasks(ImmutableSet.of(TASK));
      store.getCronJobStore().saveAcceptedJob(CRON_JOB);
      store.getQuotaStore().saveQuota(ROLE, QUOTA);
      store.getAttributeStore().saveHostAttributes(ATTRIBUTES);
      store.getSchedulerStore().saveFrameworkId(FRAMEWORK_ID);
      store.getLockStore().saveLock(LOCK);
      store.getJobUpdateStore().saveJobUpdate(UPDATE.getUpdate(), Optional.of(LOCK.getToken()));
      store.getJobUpdateStore().saveJobUpdateEvent(
          UPDATE.getUpdate().getSummary().getKey(),
          UPDATE.getUpdateEvents().get(0));
      store.getJobUpdateStore().saveJobInstanceUpdateEvent(
          UPDATE.getUpdate().getSummary().getKey(),
          UPDATE.getInstanceEvents().get(0)
      );
    });
  }
}
