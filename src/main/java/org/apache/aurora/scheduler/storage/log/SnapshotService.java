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

import javax.inject.Inject;

import com.google.common.util.concurrent.AbstractScheduledService;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.log.Log.Stream.InvalidPositionException;
import org.apache.aurora.scheduler.log.Log.Stream.StreamAccessException;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.Snapshotter;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * A {@link SnapshotStore} that snapshots to the log, and automatically snapshots on
 * a fixed interval.
 */
class SnapshotService extends AbstractScheduledService implements SnapshotStore {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotService.class);

  private final Storage storage;
  private final LogPersistence log;
  private final Snapshotter snapshotter;
  private final Amount<Long, Time> snapshotInterval;

  @Inject
  SnapshotService(Storage storage, LogPersistence log, Snapshotter snapshotter, Settings settings) {
    this.storage = requireNonNull(storage);
    this.log = requireNonNull(log);
    this.snapshotter = requireNonNull(snapshotter);
    this.snapshotInterval = settings.getSnapshotInterval();
  }

  @Override
  protected void runOneIteration() {
    snapshot();
  }

  @Timed("scheduler_log_snapshot")
  @Override
  public void snapshot() throws StorageException {
    try {
      LOG.info("Creating snapshot");

      // It's important to perform snapshot creation in a write lock to ensure all upstream callers
      // are correctly synchronized (e.g. during backup creation).
      storage.write((NoResult.Quiet) stores -> {
        Snapshot snapshot = snapshotter.from(stores);
        LOG.info("Saving snapshot");
        snapshotWith(snapshot);

        LOG.info("Snapshot complete."
            + " host attrs: " + snapshot.getHostAttributesSize()
            + ", cron jobs: " + snapshot.getCronJobsSize()
            + ", quota confs: " + snapshot.getQuotaConfigurationsSize()
            + ", tasks: " + snapshot.getTasksSize()
            + ", updates: " + snapshot.getJobUpdateDetailsSize());
      });
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

    log.persist(snapshot);
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(
        snapshotInterval.getValue(),
        snapshotInterval.getValue(),
        snapshotInterval.getUnit().getTimeUnit());
  }

  /**
   * Configuration settings for log persistence.
   */
  public static class Settings {
    private final Amount<Long, Time> snapshotInterval;

    Settings(Amount<Long, Time> snapshotInterval) {
      this.snapshotInterval = requireNonNull(snapshotInterval);
    }

    public Amount<Long, Time> getSnapshotInterval() {
      return snapshotInterval;
    }
  }
}
