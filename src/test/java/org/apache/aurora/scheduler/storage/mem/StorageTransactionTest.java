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
package org.apache.aurora.scheduler.storage.mem;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.aurora.common.testing.TearDownTestCase;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.db.DbUtil;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * TODO(William Farner): Wire a mechanism to allow verification of synchronized writers.
 * TODO(wfarner): Merge this with DbStorageTest.
 */
public class StorageTransactionTest extends TearDownTestCase {

  private ExecutorService executor;
  private Storage storage;

  @Before
  public void setUp() {
    executor = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat("SlowRead-%d").setDaemon(true).build());
    addTearDown(() -> MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.SECONDS));
    storage = DbUtil.createStorage();
  }

  @Test
  public void testConcurrentReaders() throws Exception {
    // Validate that a slow read does not block another read.

    CountDownLatch slowReadStarted = new CountDownLatch(1);
    CountDownLatch slowReadFinished = new CountDownLatch(1);

    Future<String> future = executor.submit(() -> storage.read(storeProvider -> {
      slowReadStarted.countDown();
      try {
        slowReadFinished.await();
      } catch (InterruptedException e) {
        fail(e.getMessage());
      }
      return "slowResult";
    }));

    slowReadStarted.await();

    String fastResult = storage.read(storeProvider -> "fastResult");
    assertEquals("fastResult", fastResult);
    slowReadFinished.countDown();
    assertEquals("slowResult", future.get());
  }

  private IScheduledTask makeTask(String taskId) {
    return TaskTestUtil.makeTask(taskId, TaskTestUtil.JOB);
  }

  private static class CustomException extends RuntimeException {
  }

  private <T, E extends RuntimeException> void expectWriteFail(MutateWork<T, E> work) {
    try {
      storage.write(work);
      fail("Expected a CustomException.");
    } catch (CustomException e) {
      // Expected.
    }
  }

  private void expectTasks(String... taskIds) {
    storage.read(storeProvider -> {
      Query.Builder query = Query.unscoped();
      Set<String> ids = FluentIterable.from(storeProvider.getTaskStore().fetchTasks(query))
          .transform(Tasks::id)
          .toSet();
      assertEquals(ImmutableSet.<String>builder().add(taskIds).build(), ids);
      return null;
    });
  }

  @Test
  public void testWritesUnderTransaction() {
    IResourceAggregate quota = IResourceAggregate
        .build(new ResourceAggregate().setDiskMb(100).setNumCpus(2.0).setRamMb(512));

    try {
      storage.write(storeProvider -> {
        storeProvider.getQuotaStore().saveQuota("a", quota);
        throw new CustomException();
      });
      fail("Expected CustomException to be thrown.");
    } catch (CustomException e) {
      // Expected
    }

    storage.read(storeProvider -> {
      // If the previous write was under a transaction then there would be no quota records.
      assertEquals(ImmutableMap.of(),
          storeProvider.getQuotaStore().fetchQuotas());
      return null;
    });
  }

  @Test
  public void testOperations() {
    expectWriteFail(storeProvider -> {
      storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("a"), makeTask("b")));
      throw new CustomException();
    });
    expectTasks();

    storage.write((NoResult.Quiet) storeProvider ->
        storeProvider.getUnsafeTaskStore().saveTasks(
            ImmutableSet.of(makeTask("a"), makeTask("b"))));
    expectTasks("a", "b");

    expectWriteFail(storeProvider -> {
      storeProvider.getUnsafeTaskStore().deleteAllTasks();
      throw new CustomException();
    });
    expectTasks("a", "b");

    storage.write(
        (NoResult.Quiet) storeProvider -> storeProvider.getUnsafeTaskStore().deleteAllTasks());

    expectWriteFail(storeProvider -> {
      storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("a")));
      throw new CustomException();
    });
    expectTasks();

    storage.write((NoResult.Quiet) storeProvider ->
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("a"))));

    // Nested transaction where inner transaction fails.
    expectWriteFail((NoResult.Quiet) storeProvider -> {
      storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("c")));
      storage.write(storeProvider1 -> {
        storeProvider1.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("d")));
        throw new CustomException();
      });
    });
    expectTasks("a");

    // Nested transaction where outer transaction fails.
    expectWriteFail(storeProvider -> {
      storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("c")));
      storage.write((NoResult.Quiet) storeProvider1 ->
          storeProvider1.getUnsafeTaskStore().saveTasks(ImmutableSet.of(makeTask("d"))));
      throw new CustomException();
    });
    expectTasks("a");
  }
}
