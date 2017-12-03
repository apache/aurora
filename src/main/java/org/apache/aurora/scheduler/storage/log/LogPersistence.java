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
package org.apache.aurora.scheduler.storage.log;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.log.Log.Stream.InvalidPositionException;
import org.apache.aurora.scheduler.log.Log.Stream.StreamAccessException;
import org.apache.aurora.scheduler.storage.DistributedSnapshotStore;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.durability.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Persistence layer that uses a replicated log.
 */
class LogPersistence implements Persistence, DistributedSnapshotStore {

  private static final Logger LOG = LoggerFactory.getLogger(LogPersistence.class);

  private final LogManager logManager;
  private final SnapshotStore<Snapshot> snapshotStore;
  private final SchedulingService schedulingService;
  private final Amount<Long, Time> snapshotInterval;
  private StreamManager streamManager;

  @Inject
  LogPersistence(
      Settings settings,
      LogManager logManager,
      SnapshotStore<Snapshot> snapshotStore,
      ShutdownRegistry shutdownRegistry) {

    this(new ScheduledExecutorSchedulingService(
            shutdownRegistry,
            settings.getShutdownGracePeriod()),
        settings.getSnapshotInterval(),
        logManager,
        snapshotStore);
  }

  @VisibleForTesting
  LogPersistence(
      SchedulingService schedulingService,
      Amount<Long, Time> snapshotInterval,
      LogManager logManager,
      SnapshotStore<Snapshot> snapshotStore) {

    this.schedulingService = requireNonNull(schedulingService);
    this.snapshotInterval = requireNonNull(snapshotInterval);
    this.logManager = requireNonNull(logManager);
    this.snapshotStore = requireNonNull(snapshotStore);
  }

  @Override
  public void prepare() {
    // Open the log to make a log replica available to the scheduler group.
    try {
      streamManager = logManager.open();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to open the log, cannot continue", e);
    }
  }

  @Override
  public void persist(Stream<Op> mutations) throws PersistenceException {
    try {
      streamManager.commit(mutations.collect(Collectors.toList()));
    } catch (CodingException e) {
      throw new PersistenceException(e);
    }
  }

  @Override
  public Stream<Op> recover() throws PersistenceException {
    scheduleSnapshots();

    try {
      Iterator<LogEntry> entries = streamManager.readFromBeginning();
      Iterable<LogEntry> iterableEntries = () -> entries;
      Stream<LogEntry> entryStream = StreamSupport.stream(iterableEntries.spliterator(), false);

      return entryStream
          .filter(entry -> entry.getSetField() != LogEntry._Fields.NOOP)
          .filter(entry -> {
            if (entry.getSetField() == LogEntry._Fields.SNAPSHOT) {
              Snapshot snapshot = entry.getSnapshot();
              LOG.info("Applying snapshot taken on " + new Date(snapshot.getTimestamp()));
              snapshotStore.applySnapshot(snapshot);
              return false;
            }
            return true;
          })
          .peek(entry -> {
            if (entry.getSetField() != LogEntry._Fields.TRANSACTION) {
              throw new IllegalStateException("Unknown log entry type: " + entry.getSetField());
            }
          })
          .flatMap(entry -> entry.getTransaction().getOps().stream());
    } catch (CodingException | InvalidPositionException | StreamAccessException e) {
      throw new PersistenceException(e);
    }
  }

  private void scheduleSnapshots() {
    if (snapshotInterval.getValue() > 0) {
      schedulingService.doEvery(snapshotInterval, () -> {
        try {
          snapshot();
        } catch (StorageException e) {
          if (e.getCause() == null) {
            LOG.warn("StorageException when attempting to snapshot.", e);
          } else {
            LOG.warn(e.getMessage(), e.getCause());
          }
        }
      });
    }
  }

  @Override
  public void snapshot() throws StorageException {
    try {
      doSnapshot();
    } catch (CodingException e) {
      throw new StorageException("Failed to encode a snapshot", e);
    } catch (InvalidPositionException e) {
      throw new StorageException("Saved snapshot but failed to truncate entries preceding it", e);
    } catch (StreamAccessException e) {
      throw new StorageException("Failed to create a snapshot", e);
    }
  }

  @Timed("scheduler_log_snapshot_persist")
  @Override
  public void snapshotWith(Snapshot snapshot)
      throws CodingException, InvalidPositionException, StreamAccessException {

    streamManager.snapshot(snapshot);
  }

  /**
   * Forces a snapshot of the storage state.
   *
   * @throws CodingException If there is a problem encoding the snapshot.
   * @throws InvalidPositionException If the log stream cursor is invalid.
   * @throws StreamAccessException If there is a problem writing the snapshot to the log stream.
   */
  @Timed("scheduler_log_snapshot")
  void doSnapshot() throws CodingException, InvalidPositionException, StreamAccessException {
    LOG.info("Creating snapshot.");
    Snapshot snapshot = snapshotStore.createSnapshot();
    snapshotWith(snapshot);
    LOG.info("Snapshot complete."
        + " host attrs: " + snapshot.getHostAttributesSize()
        + ", cron jobs: " + snapshot.getCronJobsSize()
        + ", quota confs: " + snapshot.getQuotaConfigurationsSize()
        + ", tasks: " + snapshot.getTasksSize()
        + ", updates: " + snapshot.getJobUpdateDetailsSize());
  }

  /**
   * A service that can schedule an action to be executed periodically.
   */
  @VisibleForTesting
  interface SchedulingService {

    /**
     * Schedules an action to execute periodically.
     *
     * @param interval The time period to wait until running the {@code action} again.
     * @param action The action to execute periodically.
     */
    void doEvery(Amount<Long, Time> interval, Runnable action);
  }

  private static class ScheduledExecutorSchedulingService implements SchedulingService {
    private final ScheduledExecutorService scheduledExecutor;

    ScheduledExecutorSchedulingService(ShutdownRegistry shutdownRegistry,
                                       Amount<Long, Time> shutdownGracePeriod) {
      scheduledExecutor = AsyncUtil.singleThreadLoggingScheduledExecutor("LogStorage-%d", LOG);
      shutdownRegistry.addAction(() -> MoreExecutors.shutdownAndAwaitTermination(
          scheduledExecutor,
          shutdownGracePeriod.getValue(),
          shutdownGracePeriod.getUnit().getTimeUnit()));
    }

    @Override
    public void doEvery(Amount<Long, Time> interval, Runnable action) {
      requireNonNull(interval);
      requireNonNull(action);

      long delay = interval.getValue();
      TimeUnit timeUnit = interval.getUnit().getTimeUnit();
      scheduledExecutor.scheduleWithFixedDelay(action, delay, delay, timeUnit);
    }
  }

  /**
   * Configuration settings for log persistence.
   */
  public static class Settings {
    private final Amount<Long, Time> shutdownGracePeriod;
    private final Amount<Long, Time> snapshotInterval;

    Settings(Amount<Long, Time> shutdownGracePeriod, Amount<Long, Time> snapshotInterval) {
      this.shutdownGracePeriod = requireNonNull(shutdownGracePeriod);
      this.snapshotInterval = requireNonNull(snapshotInterval);
    }

    public Amount<Long, Time> getShutdownGracePeriod() {
      return shutdownGracePeriod;
    }

    public Amount<Long, Time> getSnapshotInterval() {
      return snapshotInterval;
    }
  }
}
