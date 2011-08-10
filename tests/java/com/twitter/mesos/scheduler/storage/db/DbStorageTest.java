package com.twitter.mesos.scheduler.storage.db;

import java.sql.SQLException;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.junit.Test;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.storage.BaseTaskStoreTest;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.UpdateStore.ShardUpdateConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author John Sirois
 */
public class DbStorageTest extends BaseTaskStoreTest<DbStorage> {

  @Override
  protected DbStorage createTaskStore() throws SQLException {
    DbStorage dbStorage = DbStorageTestUtil.setupStorage(this);
    dbStorage.start(new Work.NoResult.Quiet() {
      @Override protected void execute(Storage.StoreProvider storeProvider) {
        // noop
      }
    });
    return dbStorage;
  }

  @Test
  public void testFrameworkStorage() {
    assertNull(store.fetchFrameworkId());

    store.saveFrameworkId("jake");
    assertEquals("jake", store.fetchFrameworkId());

    store.saveFrameworkId("jane");
    assertEquals("jane", store.fetchFrameworkId());

    store.saveFrameworkId("jim");
    store.saveFrameworkId("jeff");
    store.saveFrameworkId("bob");
    assertEquals("bob", store.fetchFrameworkId());
  }

  @Test
  public void testJobConfigurationStorage() {
    JobConfiguration jobConfig1 = createJobConfig("jake", "jake", "fortune");
    store.saveAcceptedJob("CRON", jobConfig1);

    JobConfiguration jobConfig2 = createJobConfig("jane", "jane", "df");
    store.saveAcceptedJob("CRON", jobConfig2);

    JobConfiguration jobConfig3 = createJobConfig("fred", "fred", "uname");
    store.saveAcceptedJob("IMMEDIATE", jobConfig3);

    assertTrue(Iterables.isEmpty(store.fetchJobs("DNE")));
    assertEquals(ImmutableList.of(jobConfig1, jobConfig2), store.fetchJobs("CRON"));
    assertEquals(ImmutableList.of(jobConfig3), store.fetchJobs("IMMEDIATE"));

    store.removeJob(Tasks.jobKey(jobConfig1));
    assertEquals(ImmutableList.of(jobConfig2), store.fetchJobs("CRON"));
    assertEquals(ImmutableList.of(jobConfig3), store.fetchJobs("IMMEDIATE"));

    assertNull(store.fetchJob("IMMEDIATE", Tasks.jobKey(jobConfig2)));

    JobConfiguration actual = store.fetchJob("CRON", Tasks.jobKey(jobConfig2));
    assertEquals(jobConfig2, actual);
  }

  @Test
  public void testGetTaskStoreSize() {
    assertEquals(0, store.getTaskStoreSize());

    store(ImmutableList.of(makeTask("task1")));
    assertEquals(1, store.getTaskStoreSize());

    store(ImmutableList.of(makeTask("task2"), makeTask("task3")));
    assertEquals(3, store.getTaskStoreSize());

    store.removeTasks(Query.GET_ALL);
    assertEquals(0, store.getTaskStoreSize());
  }

  @Test
  public void testCheckpointing() {
    assertNull(store.fetchCheckpoint());

    store.checkpoint(createCheckpoint("bob"));
    store.checkpoint(createCheckpoint("fred"));
    assertEquals("fred", decodeCheckpoint(store.fetchCheckpoint()));

    store.checkpoint(createCheckpoint("bob"));
    assertEquals("bob", decodeCheckpoint(store.fetchCheckpoint()));
  }

  @Test
  public void testSnapshotting() {
    byte[] snapshot1 = store.createSnapshot();

    store.saveFrameworkId("jake");
    store.checkpoint(createCheckpoint("1"));
    byte[] snapshot2 = store.createSnapshot();

    JobConfiguration fortuneCron = createJobConfig("jake", "jake", "fortune");
    store.saveAcceptedJob("CRON", fortuneCron);

    ScheduledTask originalTask = makeTask("42");
    store.saveTasks(ImmutableSet.<ScheduledTask>of(originalTask));

    TwitterTaskInfo originalTaskInfo = originalTask.getAssignedTask().getTask();
    final TwitterTaskInfo newTaskInfo = originalTaskInfo.deepCopy().setNumCpus(42);
    TaskUpdateConfiguration updateConfiguration =
        new TaskUpdateConfiguration(originalTaskInfo, newTaskInfo);
    store.saveShardUpdateConfigs("fortune", "please", ImmutableSet
        .<TaskUpdateConfiguration>of(updateConfiguration));
    store.checkpoint(createCheckpoint("2"));
    byte[] snapshot3 = store.createSnapshot();

    store.applySnapshot(snapshot1);
    assertNull(store.fetchCheckpoint());
    assertNull(store.fetchFrameworkId());
    assertTrue(Iterables.isEmpty(store.fetchJobs("CRON")));
    assertTrue(store.fetchTaskIds(Query.GET_ALL).isEmpty());
    assertTrue(store.fetchShardUpdateConfigs("fortune").isEmpty());

    store.applySnapshot(snapshot3);
    assertEquals("2", decodeCheckpoint(store.fetchCheckpoint()));
    assertEquals("jake", store.fetchFrameworkId());
    assertEquals(ImmutableList.of(fortuneCron), ImmutableList.copyOf(store.fetchJobs("CRON")));
    assertEquals("42", Iterables.getOnlyElement(store.fetchTaskIds(Query.GET_ALL)));
    Set<ShardUpdateConfiguration> updateConfigs = store.fetchShardUpdateConfigs("fortune");
    assertEquals(1, updateConfigs.size());
    ShardUpdateConfiguration config = Iterables.getOnlyElement(updateConfigs);
    assertEquals("please", config.getUpdateToken());
    assertEquals(originalTaskInfo, config.getOldConfig());
    assertEquals(newTaskInfo, config.getNewConfig());

    store.applySnapshot(snapshot2);
    assertEquals("1", decodeCheckpoint(store.fetchCheckpoint()));
    assertEquals("jake", store.fetchFrameworkId());
    assertTrue(Iterables.isEmpty(store.fetchJobs("CRON")));
    assertTrue(store.fetchTaskIds(Query.GET_ALL).isEmpty());
    assertTrue(store.fetchShardUpdateConfigs("fortune").isEmpty());
  }

  private JobConfiguration createJobConfig(String name, String role, String user) {
    return new JobConfiguration().setOwner(new Identity(role, user)).setName(name);
  }

  private byte[] createCheckpoint(String checkpoint) {
    return checkpoint.getBytes(Charsets.UTF_8);
  }

  private String decodeCheckpoint(byte[] data) {
    return new String(data, Charsets.UTF_8);
  }
}
