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

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import com.twitter.common.stats.Stats;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Conversions;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.mesos.Protos.TaskStatus;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

/**
 * A {@link TaskStatusHandler} implementation.
 */
@VisibleForTesting
public class TaskStatusHandlerImpl extends AbstractExecutionThreadService
    implements TaskStatusHandler {

  private static final Logger LOG = Logger.getLogger(TaskStatusHandlerImpl.class.getName());

  @VisibleForTesting
  static final String MEMORY_LIMIT_DISPLAY = "Task used more memory than requested.";

  private static final String STATUS_STAT_FORMAT = "status_update_%s_%s";

  private final Storage storage;
  private final StateManager stateManager;
  private final Driver driver;
  private final BlockingQueue<TaskStatus> pendingUpdates;
  private final int maxBatchSize;
  private final CachedCounters counters;

  private final AtomicReference<Thread> threadReference = new AtomicReference<>();

  /**
   * Binding annotation for the status update queue.
   */
  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface StatusUpdateQueue { }

  /**
   * Binding annotation maximum size of a status update batch.
   */
  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface MaxBatchSize { }

  @Inject
  TaskStatusHandlerImpl(
      Storage storage,
      StateManager stateManager,
      final Driver driver,
      @StatusUpdateQueue BlockingQueue<TaskStatus> pendingUpdates,
      @MaxBatchSize Integer maxBatchSize,
      CachedCounters counters) {

    this.storage = requireNonNull(storage);
    this.stateManager = requireNonNull(stateManager);
    this.driver = requireNonNull(driver);
    this.pendingUpdates = requireNonNull(pendingUpdates);
    this.maxBatchSize = requireNonNull(maxBatchSize);
    this.counters = requireNonNull(counters);

    Stats.exportSize("status_updates_queue_size", this.pendingUpdates);

    addListener(
        new Listener() {
          @Override
          public void failed(State from, Throwable failure) {
            LOG.log(Level.SEVERE, "TaskStatusHandler failed: ", failure);
            driver.abort();
          }
        },
        MoreExecutors.sameThreadExecutor());
  }

  @Override
  public void statusUpdate(TaskStatus status) {
    pendingUpdates.add(status);
  }

  @Override
  protected void triggerShutdown() {
    Thread thread = threadReference.get();

    if (thread != null) {
      thread.interrupt();
    }
  }

  @Override
  protected void run() {
    threadReference.set(Thread.currentThread());

    while (isRunning()) {
      final Queue<TaskStatus> updates = new ArrayDeque<>();

      try {
        updates.add(pendingUpdates.take());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }

      // Process all other available updates, up to the limit on batch size.
      // TODO(bmahler): Expose histogram metrics of the batch sizes.
      pendingUpdates.drainTo(updates, maxBatchSize - updates.size());

      try {
        storage.write(new Storage.MutateWork.NoResult.Quiet() {
          @Override
          protected void execute(Storage.MutableStoreProvider storeProvider) {
            for (TaskStatus status : updates) {
              ScheduleStatus translatedState = Conversions.convertProtoState(status.getState());

              Optional<String> message = Optional.absent();
              if (status.hasMessage()) {
                message = Optional.of(status.getMessage());
              }

              if (translatedState == ScheduleStatus.FAILED && status.hasReason()
                  && status.getReason() == TaskStatus.Reason.REASON_MEMORY_LIMIT) {
                message = Optional.of(MEMORY_LIMIT_DISPLAY);
              }

              StateChangeResult result = stateManager.changeState(
                  storeProvider,
                  status.getTaskId().getValue(),
                  Optional.absent(),
                  translatedState,
                  message);

              if (status.hasReason()) {
                counters.get(statName(status, result)).incrementAndGet();
              }
            }
          }
        });

        for (TaskStatus status : updates) {
          driver.acknowledgeStatusUpdate(status);
        }
      } catch (RuntimeException e) {
        LOG.log(Level.SEVERE, "Failed to process status update batch " + updates, e);
      }
    }
  }

  @VisibleForTesting
  static String statName(TaskStatus status, StateChangeResult result) {
    return String.format(STATUS_STAT_FORMAT, status.getReason(), result);
  }
}
