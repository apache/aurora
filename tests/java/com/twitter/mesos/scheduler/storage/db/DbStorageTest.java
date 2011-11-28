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
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.db.testing.DbStorageTestUtil;
import com.twitter.mesos.scheduler.storage.BaseTaskStoreTest;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.UpdateStore.ShardUpdateConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author John Sirois
 */
public class DbStorageTest extends BaseTaskStoreTest<DbStorage> {

  @Override
  protected DbStorage createTaskStore() throws SQLException {
    DbStorage dbStorage = DbStorageTestUtil.setupStorage(this);
    dbStorage.start(Work.NOOP);
    return dbStorage;
  }

  @Test
  public void testUpdateSchemaUpgrade() {
    assertFalse(store.isOldUpdateStoreSchema());

    store.jdbcTemplate.execute("DROP TABLE IF EXISTS update_store");
    store.jdbcTemplate.execute("DROP INDEX IF EXISTS update_store_job_key_shard_id_idx");

    store.jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS update_store (" +
        "  job_key VARCHAR(511) NOT NULL," +
        "  update_token VARCHAR(36) NOT NULL," +
        "  shard_id INT NOT NULL," +
        "  config BINARY(2000000) NOT NULL)");
    store.jdbcTemplate.execute(
        "CREATE INDEX IF NOT EXISTS update_store_job_key_shard_id_idx" +
            " ON update_store(job_key, shard_id)");

    assertTrue(store.isOldUpdateStoreSchema());
    store.maybeUpgradeUpdateStoreSchema();

    store.createSchema();
    assertFalse(store.isOldUpdateStoreSchema());
  }

  @Test
  public void testUpdateSchemaUpgradeNoop() {
    store.maybeUpgradeUpdateStoreSchema();
    assertFalse(store.isOldUpdateStoreSchema());
  }

  @Test
  public void testSchemeRackUpgrade() {
    store.jdbcTemplate.execute("DROP TABLE IF EXISTS task_state");
    store.jdbcTemplate.execute("DROP INDEX IF EXISTS task_state_rack_name_idx");
    // Add the old version of the task_state(without rack_name).
    store.jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS task_state (" +
        "  task_id VARCHAR(1020) NOT NULL PRIMARY KEY," +
        "  job_role VARCHAR(255) NOT NULL," +
        "  job_user VARCHAR(255) NOT NULL," +
        "  job_name VARCHAR(255) NOT NULL," +
        "  job_key VARCHAR(511) NOT NULL," +
        "  slave_host VARCHAR(255) NULL," +
        "  shard_id INT NOT NULL," +
        "  status TINYINT NOT NULL," +
        "  scheduled_task BINARY(100000) NOT NULL);");
    store.upgradeTaskStorage();
    assertFalse(store.jdbcTemplate.queryForList(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TASK_STATE'"
            + " AND COLUMN_NAME = 'RACK_NAME'", String.class).isEmpty());
    assertFalse(store.jdbcTemplate.queryForList(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME = 'TASK_STATE'"
            + " AND INDEX_NAME = 'TASK_STATE_RACK_NAME_IDX'", String.class).isEmpty());
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
  public void testQuotaStorage() {
    assertNull(store.fetchQuota("jane"));

    Quota quota = new Quota()
        .setNumCpus(5)
        .setRamMb(2)
        .setDiskMb(10);
    store.saveQuota("jane", quota);
    assertEquals(quota, store.fetchQuota("jane"));

    Quota quota2 = new Quota()
        .setNumCpus(1)
        .setRamMb(3)
        .setDiskMb(5);
    store.saveQuota("jane", quota2);
    assertEquals(quota2, store.fetchQuota("jane"));

    store.removeQuota("jane");
    assertNull(store.fetchQuota("jane"));
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
    String frameworkId = "framework";
    String user = "jim";
    String role = "jake";
    String job = "fortune";
    String token = "please";
    byte[] snapshot1 = store.createSnapshot();

    store.saveFrameworkId(frameworkId);
    store.checkpoint(createCheckpoint("1"));
    byte[] snapshot2 = store.createSnapshot();

    JobConfiguration fortuneCron = createJobConfig(job, role, job);
    store.saveAcceptedJob("CRON", fortuneCron);

    ScheduledTask originalTask = makeTask("42");
    store.saveTasks(ImmutableSet.<ScheduledTask>of(originalTask));

    TwitterTaskInfo originalTaskInfo = originalTask.getAssignedTask().getTask();
    final TwitterTaskInfo newTaskInfo = originalTaskInfo.deepCopy().setNumCpus(42);
    TaskUpdateConfiguration updateConfiguration =
        new TaskUpdateConfiguration(originalTaskInfo, newTaskInfo);
    store.saveShardUpdateConfigs(role, job, token,
        ImmutableSet.<TaskUpdateConfiguration>of(updateConfiguration));
    store.checkpoint(createCheckpoint("2"));
    byte[] snapshot3 = store.createSnapshot();

    store.applySnapshot(snapshot1);
    assertNull(store.fetchCheckpoint());
    assertNull(store.fetchFrameworkId());
    assertTrue(Iterables.isEmpty(store.fetchJobs("CRON")));
    assertTrue(store.fetchTaskIds(Query.GET_ALL).isEmpty());
    assertTrue(store.fetchShardUpdateConfigs(role).isEmpty());

    store.applySnapshot(snapshot3);
    assertEquals("2", decodeCheckpoint(store.fetchCheckpoint()));
    assertEquals(frameworkId, store.fetchFrameworkId());
    assertEquals(ImmutableList.of(fortuneCron), ImmutableList.copyOf(store.fetchJobs("CRON")));
    assertEquals("42", Iterables.getOnlyElement(store.fetchTaskIds(Query.GET_ALL)));
    Set<ShardUpdateConfiguration> updateConfigs = store.fetchShardUpdateConfigs(role, job);
    assertEquals(1, updateConfigs.size());
    ShardUpdateConfiguration config = Iterables.getOnlyElement(updateConfigs);
    assertEquals(token, config.getUpdateToken());
    assertEquals(originalTaskInfo, config.getOldConfig());
    assertEquals(newTaskInfo, config.getNewConfig());

    store.applySnapshot(snapshot2);
    assertEquals("1", decodeCheckpoint(store.fetchCheckpoint()));
    assertEquals(frameworkId, store.fetchFrameworkId());
    assertTrue(Iterables.isEmpty(store.fetchJobs("CRON")));
    assertTrue(store.fetchTaskIds(Query.GET_ALL).isEmpty());
    assertTrue(store.fetchShardUpdateConfigs(role).isEmpty());
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
