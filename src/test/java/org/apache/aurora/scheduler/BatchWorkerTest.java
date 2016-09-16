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
package org.apache.aurora.scheduler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.BackoffStrategy;
import org.apache.aurora.scheduler.BatchWorker.Result;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertTrue;

public class BatchWorkerTest extends EasyMockTest {
  private static final String SERVICE_NAME = "TestWorker";
  private static final String BATCH_STAT = SERVICE_NAME + "_batches_processed";
  private FakeStatsProvider statsProvider;
  private BatchWorker<Boolean> batchWorker;

  @Before
  public void setUp() {
    StorageTestUtil storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    statsProvider = new FakeStatsProvider();
    batchWorker = new BatchWorker<Boolean>(storageUtil.storage, statsProvider, 2) {
      @Override
      protected String serviceName() {
        return SERVICE_NAME;
      }
    };
  }

  @Test
  public void testExecute() throws Exception {
    control.replay();

    CompletableFuture<Boolean> result1 = batchWorker.execute(store -> true);
    CompletableFuture<Boolean> result2 = batchWorker.execute(store -> true);
    CompletableFuture<Boolean> result3 = batchWorker.execute(store -> true);
    batchWorker.startAsync().awaitRunning();

    assertTrue(result1.get());
    assertTrue(result2.get());
    assertTrue(result3.get());
  }

  @Test(expected = ExecutionException.class)
  public void testExecuteThrows() throws Exception {
    control.replay();

    CompletableFuture<Boolean> result =
        batchWorker.execute(store -> { throw new IllegalArgumentException(); });
    batchWorker.startAsync().awaitRunning();

    result.get();
  }

  @Test
  public void testExecuteWithReplay() throws Exception {
    BackoffStrategy backoff = createMock(BackoffStrategy.class);
    final CountDownLatch complete = new CountDownLatch(1);

    expect(backoff.calculateBackoffMs(EasyMock.anyLong())).andReturn(0L).anyTimes();

    control.replay();

    batchWorker.startAsync().awaitRunning();
    batchWorker.executeWithReplay(
        backoff,
        store -> statsProvider.getValue(BATCH_STAT).longValue() > 1L
            ? new Result<>(true, true)
            : new Result<>(false, false))
        .thenAccept(result -> complete.countDown());

    assertTrue(complete.await(10L, TimeUnit.SECONDS));
  }
}
