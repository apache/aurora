package com.twitter.mesos.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.scheduler.DbStorage.Configuration;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author jsirois
 */
public class DbStorageTest extends BaseTaskStoreTest<DbStorage> {

  @Override
  protected DbStorage createTaskStore() throws SQLException {
    return DbStorageTestUtil.setupStorage(this, new Configuration().setMaxFrameworkIdHistory(2));
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

    assertEquals(ImmutableList.of("jeff", "bob"),
        store.jdbcTemplate.queryForList("SELECT framework_id FROM scheduler_state", String.class));
  }

  @Test
  public void testJobConfigurationStorage() {
    JobConfiguration jobConfig1 = new JobConfiguration().setOwner("jake").setName("fortune");
    store.saveAcceptedJob("CRON", jobConfig1);

    JobConfiguration jobConfig2 = new JobConfiguration().setOwner("jane").setName("df");
    store.saveAcceptedJob("CRON", jobConfig2);

    JobConfiguration jobConfig3 = new JobConfiguration().setOwner("fred").setName("uname");
    store.saveAcceptedJob("IMMEDIATE", jobConfig3);

    assertTrue(Iterables.isEmpty(store.fetchJobs("DNE")));
    assertEquals(ImmutableList.of(jobConfig1, jobConfig2), store.fetchJobs("CRON"));
    assertEquals(ImmutableList.of(jobConfig3), store.fetchJobs("IMMEDIATE"));

    store.deleteJob(Tasks.jobKey(jobConfig1));
    assertEquals(ImmutableList.of(jobConfig2), store.fetchJobs("CRON"));
    assertEquals(ImmutableList.of(jobConfig3), store.fetchJobs("IMMEDIATE"));
  }
}
