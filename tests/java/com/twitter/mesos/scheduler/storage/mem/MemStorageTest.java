package com.twitter.mesos.scheduler.storage.mem;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.testing.TearDown;
import com.google.common.testing.junit4.TearDownTestCase;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * TODO(William Farner): Wire a mechanism to allow verification of synchronized writers.
 */
public class MemStorageTest extends TearDownTestCase {

  private ExecutorService executor;
  private Storage storage;

  @Before
  public void setUp() {
    executor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("SlowRead-%d").setDaemon(true).build());
    addTearDown(new TearDown() {
      @Override public void tearDown() {
        new ExecutorServiceShutdown(executor, Amount.of(1L, Time.SECONDS)).execute();
      }
    });
    storage = MemStorage.newEmptyStorage();
  }

  @Test
  public void testConcurrentReaders() throws Exception {
    // Validate that a slow read does not block another read.

    final CountDownLatch slowReadStarted = new CountDownLatch(1);
    final CountDownLatch slowReadFinished = new CountDownLatch(1);

    Future<String> future = executor.submit(new Callable<String>() {
      @Override public String call() throws Exception {
        return storage.readOp(new Work.Quiet<String>() {
          @Override public String apply(StoreProvider storeProvider) {
            slowReadStarted.countDown();
            try {
              slowReadFinished.await();
            } catch (InterruptedException e) {
              fail(e.getMessage());
            }
            return "slowResult";
          }
        });
      }
    });

    slowReadStarted.await();

    String fastResult = storage.readOp(new Work.Quiet<String>() {
      @Override public String apply(StoreProvider storeProvider) {
        return "fastResult";
      }
    });
    assertEquals("fastResult", fastResult);
    slowReadFinished.countDown();
    assertEquals("slowResult", future.get());
  }

  private ScheduledTask makeTask(String taskId) {
    return new ScheduledTask().setAssignedTask(
        new AssignedTask()
            .setTaskId(taskId)
            .setTask(new TwitterTaskInfo()
                .setOwner(new Identity().setRole("owner-" + taskId))
                .setJobName("job-" + taskId)));
  }

  private class CustomException extends RuntimeException {
  }

  private <T, E extends RuntimeException> void expectWriteFail(MutateWork<T, E> work) {
    try {
      storage.writeOp(work);
      fail("Expected a CustomException.");
    } catch (CustomException e) {
      // Expected.
    }
  }

  private void expectTasks(final String... taskIds) {
    storage.readOp(new Work.Quiet<Void>() {
      @Override public Void apply(StoreProvider storeProvider) {
        assertEquals(
            ImmutableSet.<String>builder().add(taskIds).build(),
            storeProvider.getTaskStore().fetchTaskIds(Query.GET_ALL));
        return null;
      }
    });
  }

  @Test
  public void testOperations() {
    expectWriteFail(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("a"), makeTask("b")));
        throw new CustomException();
      }
    });
    expectTasks("a", "b");

    storage.writeOp(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("a"), makeTask("b")));
      }
    });
    expectTasks("a", "b");

    expectWriteFail(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().deleteAllTasks();
        throw new CustomException();
      }
    });
    expectTasks();

    expectWriteFail(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("a")));
        ScheduledTask a = Iterables.getOnlyElement(
            storeProvider.getUnsafeTaskStore().fetchTasks(Query.byId("a")));
        a.setStatus(ScheduleStatus.RUNNING)
            .setAncestorId("z");
        throw new CustomException();
      }
    });
    expectTasks("a");
    storage.readOp(new Work.Quiet<Void>() {
      @Override public Void apply(StoreProvider storeProvider) {
        assertEquals(
            makeTask("a"),
            Iterables.getOnlyElement(storeProvider.getTaskStore().fetchTasks(Query.byId("a"))));
        return null;
      }
    });

    // Nested transaction where inner transaction fails.
    expectWriteFail(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("c")));
        storage.writeOp(new MutateWork.NoResult.Quiet() {
          @Override protected void execute(MutableStoreProvider storeProvider) {
            storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("d")));
            throw new CustomException();
          }
        });
      }
    });
    expectTasks("a", "c", "d");

    // Nested transaction where outer transaction fails.
    expectWriteFail(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("c")));
        storage.writeOp(new MutateWork.NoResult.Quiet() {
          @Override protected void execute(MutableStoreProvider storeProvider) {
            storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("d")));
          }
        });
        throw new CustomException();
      }
    });
    expectTasks("a", "c", "d");
  }
}
