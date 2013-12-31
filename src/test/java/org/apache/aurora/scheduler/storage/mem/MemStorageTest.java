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
package org.apache.aurora.scheduler.storage.mem;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.testing.TearDown;
import com.google.common.testing.junit4.TearDownTestCase;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import org.junit.Before;
import org.junit.Test;

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
        return storage.consistentRead(new Work.Quiet<String>() {
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

    String fastResult = storage.consistentRead(new Work.Quiet<String>() {
      @Override public String apply(StoreProvider storeProvider) {
        return "fastResult";
      }
    });
    assertEquals("fastResult", fastResult);
    slowReadFinished.countDown();
    assertEquals("slowResult", future.get());
  }

  private IScheduledTask makeTask(String taskId) {
    return IScheduledTask.build(new ScheduledTask().setAssignedTask(
        new AssignedTask()
            .setTaskId(taskId)
            .setTask(new TaskConfig()
                .setOwner(new Identity().setRole("owner-" + taskId))
                .setJobName("job-" + taskId)
                .setEnvironment("env-" + taskId))));
  }

  private class CustomException extends RuntimeException {
  }

  private <T, E extends RuntimeException> void expectWriteFail(MutateWork<T, E> work) {
    try {
      storage.write(work);
      fail("Expected a CustomException.");
    } catch (CustomException e) {
      // Expected.
    }
  }

  private void expectTasks(final String... taskIds) {
    storage.consistentRead(new Work.Quiet<Void>() {
      @Override public Void apply(StoreProvider storeProvider) {
        Query.Builder query = Query.unscoped();
        Set<String> ids = FluentIterable.from(storeProvider.getTaskStore().fetchTasks(query))
            .transform(Tasks.SCHEDULED_TO_ID)
            .toSet();
        assertEquals(ImmutableSet.<String>builder().add(taskIds).build(), ids);
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

    storage.write(new MutateWork.NoResult.Quiet() {
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
        throw new CustomException();
      }
    });
    expectTasks("a");
    storage.consistentRead(new Work.Quiet<Void>() {
      @Override public Void apply(StoreProvider storeProvider) {
        assertEquals(
            makeTask("a"),
            Iterables.getOnlyElement(storeProvider.getTaskStore().fetchTasks(
                Query.taskScoped("a"))));
        return null;
      }
    });

    // Nested transaction where inner transaction fails.
    expectWriteFail(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("c")));
        storage.write(new MutateWork.NoResult.Quiet() {
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
        storage.write(new MutateWork.NoResult.Quiet() {
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
