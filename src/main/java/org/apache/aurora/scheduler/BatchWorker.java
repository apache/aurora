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

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import com.google.common.util.concurrent.AbstractExecutionThreadService;

import org.apache.aurora.common.stats.SlidingStats;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.BackoffStrategy;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkState;

/**
 * Generic helper that allows bundling multiple work items into a single {@link Storage}
 * transaction aiming to reduce the write lock contention.
 *
 * @param <T> Expected result type.
 */
public class BatchWorker<T> extends AbstractExecutionThreadService {
  /**
   * Empty result placeholder.
   */
  public interface NoResult { }

  /**
   * Convenience wrapper for a non-repeatable no value work {@link Result}.
   */
  public static final NoResult NO_RESULT = new NoResult() { };

  private static final Logger LOG = LoggerFactory.getLogger(BatchWorker.class);
  private final Storage storage;
  private final int maxBatchSize;
  private final SlidingStats batchUnlocked;
  private final SlidingStats batchLocked;
  private final BlockingQueue<WorkItem<T>> workQueue = new LinkedBlockingQueue<>();
  private final ScheduledExecutorService scheduledExecutor;
  private final AtomicInteger lastBatchSize = new AtomicInteger(0);
  private final AtomicLong itemsProcessed;
  private final AtomicLong batchesProcessed;

  /**
   * Wraps result returned by the {@link RepeatableWork} item.
   *
   * @param <T> Expected result type.
   */
  public static class Result<T> {
    private final boolean isCompleted;
    private final T value;

    /**
     * Initializes a {@link Result} instance with {@code isCompleted} and {@code value}.
     * <p>
     * The {@code isCompleted} may be set to {@code False} for a {@link RepeatableWork} that has
     * not finished yet. Otherwise, it must be set to {@code True}.
     *
     * @param isCompleted Flag indicating if the {@link RepeatableWork} has completed.
     * @param value result value.
     */
    public Result(boolean isCompleted, T value) {
      this.isCompleted = isCompleted;
      this.value = value;
    }
  }

  /**
   * Encapsulates a potentially repeatable operation.
   */
  public interface RepeatableWork<T> {
    /**
     * Abstracts a unit of repeatable (i.e.: "repeat until completed") work.
     * <p>
     * The work unit may be repeated as instructed by the {@link Result}.
     *
     * @param storeProvider {@link MutableStoreProvider} instance.
     * @return {@link Result}
     */
    Result<T> apply(MutableStoreProvider storeProvider);
  }

  /**
   * Encapsulates a non-repeatable operation.
   */
  public interface Work<T> extends RepeatableWork<T> {
    @Override
    default Result<T> apply(MutableStoreProvider storeProvider) {
      T value = execute(storeProvider);
      return new Result<>(true, value);
    }

    /**
     * Abstracts a unit of non-repeatable (i.e.: "run exactly once") work.
     *
     * @param storeProvider {@link MutableStoreProvider} instance.
     * @return result value.
     */
    T execute(MutableStoreProvider storeProvider);
  }

  @Inject
  protected BatchWorker(
      Storage storage,
      StatsProvider statsProvider,
      int maxBatchSize) {

    this.storage = requireNonNull(storage);
    this.maxBatchSize = maxBatchSize;

    scheduledExecutor = AsyncUtil.singleThreadLoggingScheduledExecutor(serviceName() + "-%d", LOG);
    statsProvider.makeGauge(serviceName() + "_queue_size", () -> workQueue.size());
    statsProvider.makeGauge(
        serviceName() + "_last_processed_batch_size",
        () -> lastBatchSize.intValue());
    batchUnlocked = new SlidingStats(serviceName() + "_batch_unlocked", "nanos");
    batchLocked = new SlidingStats(serviceName() + "_batch_locked", "nanos");
    itemsProcessed = statsProvider.makeCounter(serviceName() + "_items_processed");
    batchesProcessed = statsProvider.makeCounter(serviceName() + "_batches_processed");
  }

  /**
   * Executes a non-repeatable {@link Work} and returns {@link CompletableFuture} to wait on.
   *
   * @param work A non-repeatable {@link Work} to execute.
   * @return {@link CompletableFuture} to wait on.
   */
  public CompletableFuture<T> execute(Work<T> work) {
    CompletableFuture<T> result = new CompletableFuture<>();
    workQueue.add(new WorkItem<>(
        work,
        result,
        Optional.empty(),
        Optional.empty()));

    return result;
  }

  /**
   * Executes a {@link RepeatableWork} until it completes and returns {@link CompletableFuture}
   * to wait on.
   *
   * @param backoffStrategy A {@link BackoffStrategy} instance to backoff subsequent runs.
   * @param work A {@link RepeatableWork} to execute.
   */
  public CompletableFuture<T> executeWithReplay(
      BackoffStrategy backoffStrategy,
      RepeatableWork<T> work) {

    CompletableFuture<T> result = new CompletableFuture<>();
    workQueue.add(new WorkItem<>(
        work,
        result,
        Optional.of(backoffStrategy),
        Optional.of(0L)));

    return result;
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      List<WorkItem<T>> batch = new LinkedList<>();

      // Make the loop responsive to shutdown under light load by using
      // a short non-configurable timeout in poll().
      Optional<WorkItem<T>> head = Optional.ofNullable(workQueue.poll(3, TimeUnit.SECONDS));
      if (head.isPresent()) {
        workQueue.add(head.get());
        workQueue.drainTo(batch, maxBatchSize - batch.size());
        processBatch(batch);
      }
    }
  }

  private void processBatch(List<WorkItem<T>> batch) {
    if (!batch.isEmpty()) {
      long unlockedStart = System.nanoTime();
      storage.write((Storage.MutateWork.NoResult.Quiet) storeProvider -> {
        long lockedStart = System.nanoTime();
        for (WorkItem<T> item : batch) {
          Result<T> itemResult = item.work.apply(storeProvider);
          if (itemResult.isCompleted) {
            item.result.complete(itemResult.value);
          } else {
            // Work not finished yet - re-queue for a followup later.
            long backoffMsec = backoffFor(item);
            scheduledExecutor.schedule(
                () -> workQueue.add(new WorkItem<>(
                    item.work,
                    item.result,
                    item.backoffStrategy,
                    Optional.of(backoffMsec))),
                backoffMsec,
                TimeUnit.MILLISECONDS);
          }
        }
        batchLocked.accumulate(System.nanoTime() - lockedStart);
      });
      batchUnlocked.accumulate(System.nanoTime() - unlockedStart);
      batchesProcessed.incrementAndGet();
      lastBatchSize.set(batch.size());
      itemsProcessed.addAndGet(batch.size());
    }
  }

  private long backoffFor(WorkItem<T> item) {
    checkState(item.backoffStrategy.isPresent());
    checkState(item.lastBackoffMsec.isPresent());
    return item.backoffStrategy.get().calculateBackoffMs(item.lastBackoffMsec.get());
  }

  private class WorkItem<V> {
    private final RepeatableWork<V> work;
    private final CompletableFuture<T> result;
    private final Optional<BackoffStrategy> backoffStrategy;
    private final Optional<Long> lastBackoffMsec;

    WorkItem(
        RepeatableWork<V> work,
        CompletableFuture<T> result,
        Optional<BackoffStrategy> backoffStrategy,
        Optional<Long> lastBackoffMsec) {

      this.work = work;
      this.result = result;
      this.backoffStrategy = backoffStrategy;
      this.lastBackoffMsec = lastBackoffMsec;
    }
  }
}
