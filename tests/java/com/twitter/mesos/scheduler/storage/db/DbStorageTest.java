package com.twitter.mesos.scheduler.storage.db;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.storage.JobUpdateConfiguration;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.db.testing.DbStorageTestUtil;
import com.twitter.mesos.scheduler.storage.BaseTaskStoreTest;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.Storage.Work.Quiet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DbStorageTest extends BaseTaskStoreTest<DbStorage> {

  @Override
  protected DbStorage createTaskStore() throws SQLException {
    DbStorage dbStorage = DbStorageTestUtil.setupStorage(this);
    dbStorage.start(MutateWork.NOOP);
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
  public void testQuotaStorage() {
    assertFalse(store.fetchQuota("jane").isPresent());

    Quota quota = new Quota()
        .setNumCpus(5)
        .setRamMb(2)
        .setDiskMb(10);
    store.saveQuota("jane", quota);
    assertEquals(Optional.of(quota), store.fetchQuota("jane"));

    Quota quota2 = new Quota()
        .setNumCpus(1)
        .setRamMb(3)
        .setDiskMb(5);
    store.saveQuota("jane", quota2);
    assertEquals(Optional.of(quota2), store.fetchQuota("jane"));

    store.removeQuota("jane");
    assertFalse(store.fetchQuota("jane").isPresent());

    store.saveQuota("foo", quota);
    store.saveQuota("bar", quota2);
    assertEquals(ImmutableSet.of("foo", "bar"), store.fetchQuotaRoles());
    store.deleteQuotas();
    assertEquals(ImmutableSet.<String>of(), store.fetchQuotaRoles());
  }

  @Test
  public void testGetTaskStoreSize() {
    assertEquals(0, store.getTaskStoreSize());

    store(ImmutableList.of(makeTask("task1")));
    assertEquals(1, store.getTaskStoreSize());

    store(ImmutableList.of(makeTask("task2"), makeTask("task3")));
    assertEquals(3, store.getTaskStoreSize());

    store.deleteTasks();
    assertEquals(0, store.getTaskStoreSize());
  }

  @Test
  public void testSnapshotting() {
    String frameworkId = "framework";
    String role = "jake";
    String job = "spin";
    String token = "please";
    byte[] snapshot1 = store.createSnapshot();

    store.saveFrameworkId(frameworkId);
    byte[] snapshot2 = store.createSnapshot();

    JobConfiguration fortuneCron = createJobConfig(job, role, job);
    store.saveAcceptedJob("CRON", fortuneCron);

    ScheduledTask originalTask = makeTask("42");
    store.saveTasks(ImmutableSet.<ScheduledTask>of(originalTask));

    TwitterTaskInfo originalTaskInfo = originalTask.getAssignedTask().getTask();
    final TwitterTaskInfo newTaskInfo = originalTaskInfo.deepCopy().setNumCpus(42);
    TaskUpdateConfiguration updateConfiguration =
        new TaskUpdateConfiguration(originalTaskInfo, newTaskInfo);
    store.saveJobUpdateConfig(
        new JobUpdateConfiguration(role, job, token, ImmutableSet.of(updateConfiguration)));
    byte[] snapshot3 = store.createSnapshot();

    store.applySnapshot(snapshot1);
    assertNull(store.fetchFrameworkId());
    assertTrue(Iterables.isEmpty(store.fetchJobs("CRON")));
    assertTrue(store.fetchTaskIds(Query.GET_ALL).isEmpty());
    assertTrue(store.fetchUpdateConfigs(role).isEmpty());

    store.applySnapshot(snapshot3);
    assertEquals(frameworkId, store.fetchFrameworkId());
    assertEquals(ImmutableList.of(fortuneCron), ImmutableList.copyOf(store.fetchJobs("CRON")));
    assertEquals("42", Iterables.getOnlyElement(store.fetchTaskIds(Query.GET_ALL)));
    JobUpdateConfiguration updateConfig = store.fetchJobUpdateConfig(role, job).get();
    assertEquals(token, updateConfig .getUpdateToken());
    TaskUpdateConfiguration config = Iterables.getOnlyElement(updateConfig.getConfigs());

    assertEquals(originalTaskInfo, config.getOldConfig());
    assertEquals(newTaskInfo, config.getNewConfig());

    store.applySnapshot(snapshot2);
    assertEquals(frameworkId, store.fetchFrameworkId());
    assertTrue(Iterables.isEmpty(store.fetchJobs("CRON")));
    assertTrue(store.fetchTaskIds(Query.GET_ALL).isEmpty());
    assertTrue(store.fetchUpdateConfigs(role).isEmpty());
  }

  private Runnable latchedReader(final CountDownLatch myLatch, final CountDownLatch awaitLatch) {
    return new Runnable() {
      @Override public void run() {
        store.doInTransaction(new Work.Quiet<Void>() {
          @Override public Void apply(StoreProvider storeProvider) {
            myLatch.countDown();
            try {
              awaitLatch.await();
            } catch (InterruptedException e) {
              Thread.interrupted();
              fail(e.getMessage());
            }
            return null;
          }
        });
      }
    };
  }

  @Test
  public void testSimultaneousReaders() throws Exception {
    final CountDownLatch read1Started = new CountDownLatch(1);
    final CountDownLatch read2Started = new CountDownLatch(1);

    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).build();

    // One of these threads will deadlock if the second read transaction is not allowed to execute
    // simultaneously.
    Thread reader1 = threadFactory.newThread(latchedReader(read1Started, read2Started));
    Thread reader2 = threadFactory.newThread(latchedReader(read2Started, read1Started));
    reader1.start();
    reader2.start();
    reader1.join();
    reader2.join();
  }

  static class FakeException extends RuntimeException {
  }

  private Runnable throwingOp(final Closure<Storage> op) {
    return new Runnable() {
      @Override public void run() {
        try {
          op.execute(store);
          fail("Should have thrown.");
        } catch (FakeException e) {
          // Expected.
        }
      }
    };
  }

  @Test
  public void testLocksReleased() throws Exception {
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).build();
    Thread reader = threadFactory.newThread(throwingOp(new Closure<Storage>() {
      @Override public void execute(Storage storage) {
        storage.doInTransaction(new Work.Quiet<Void>() {
          @Override public Void apply(StoreProvider storeProvider) {
            throw new FakeException();
          }
        });
      }
    }));
    Thread writer1 = threadFactory.newThread(throwingOp(new Closure<Storage>() {
      @Override public void execute(Storage storage) {
        storage.doInWriteTransaction(new MutateWork.Quiet<Void>() {
          @Override public Void apply(MutableStoreProvider storeProvider) {
            throw new FakeException();
          }
        });
      }
    }));

    reader.start();
    writer1.start();
    reader.join();
    writer1.join();

    store.doInWriteTransaction(new MutateWork.Quiet<Void>() {
      @Override public Void apply(MutableStoreProvider storeProvider) {
        return null;
      }
    });
  }

  @Test
  public void testNestedTransactions() throws Exception {
    store.doInWriteTransaction(new MutateWork.Quiet<Void>() {
      @Override public Void apply(MutableStoreProvider storeProvider) {
        return store.doInWriteTransaction(new MutateWork.Quiet<Void>() {
          @Override public Void apply(MutableStoreProvider storeProvider) {
            return null;
          }
        });
      }
    });
  }

  @Test
  public void testTransactionDowngrade() throws Exception {
    store.doInWriteTransaction(new MutateWork.Quiet<Void>() {
      @Override public Void apply(MutableStoreProvider storeProvider) {
        return store.doInTransaction(new Work.Quiet<Void>() {
          @Override public Void apply(StoreProvider storeProvider) throws RuntimeException {
            return null;
          }
        });
      }
    });
  }

  @Test(expected = IllegalStateException.class)
  public void testTransactionUpgrade() throws Exception {
    store.doInTransaction(new Work.Quiet<Void>() {
      @Override public Void apply(StoreProvider storeProvider) throws RuntimeException {
        return store.doInWriteTransaction(new MutateWork.Quiet<Void>() {
          @Override public Void apply(MutableStoreProvider storeProvider) {
            return null;
          }
        });
      }
    });
  }

  private JobConfiguration createJobConfig(String name, String role, String user) {
    return new JobConfiguration().setOwner(new Identity(role, user)).setName(name);
  }
}
