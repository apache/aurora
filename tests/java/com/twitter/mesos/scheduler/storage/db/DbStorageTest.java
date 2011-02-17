package com.twitter.mesos.scheduler.storage.db;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.storage.BaseTaskStoreTest;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.TaskStore;
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
    DbStorage dbStorage = DbStorageTestUtil.setupStorage(this);
    dbStorage.start(new Work.NoResult.Quiet() {
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) throws RuntimeException {
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

    store.remove(Query.GET_ALL);
    assertEquals(0, store.getTaskStoreSize());
  }
}
