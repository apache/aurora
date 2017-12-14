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
package org.apache.aurora.scheduler.storage.backup;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.io.Files;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.storage.Snapshotter;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

/**
 * A backup routine that layers over a snapshot store and periodically writes snapshots to
 * local disk.
 */
public interface StorageBackup {

  /**
   * Perform a storage backup immediately, blocking until it is complete.
   */
  void backupNow();

  class StorageBackupImpl implements StorageBackup, Snapshotter {
    private static final Logger LOG = LoggerFactory.getLogger(StorageBackupImpl.class);

    private static final String FILE_PREFIX = "scheduler-backup-";
    private final BackupConfig config;

    static class BackupConfig {
      private final File dir;
      private final int maxBackups;
      private final Amount<Long, Time> interval;

      BackupConfig(File dir, int maxBackups, Amount<Long, Time> interval) {
        this.dir = requireNonNull(dir);
        this.maxBackups = maxBackups;
        this.interval = requireNonNull(interval);
      }

      @VisibleForTesting
      File getDir() {
        return dir;
      }
    }

    /**
     * Binding annotation that the underlying {@link Snapshotter} must be bound with.
     */
    @Qualifier
    @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME)
    @interface SnapshotDelegate { }

    private final Storage storage;
    private final Snapshotter delegate;
    private final Clock clock;
    private final long backupIntervalMs;
    private volatile long lastBackupMs;
    private final DateFormat backupDateFormat;
    private final Executor executor;

    private final AtomicLong successes = Stats.exportLong("scheduler_backup_success");
    @VisibleForTesting
    AtomicLong getSuccesses() {
      return successes;
    }

    private final AtomicLong failures = Stats.exportLong("scheduler_backup_failed");
    @VisibleForTesting
    AtomicLong getFailures() {
      return failures;
    }

    @Inject
    StorageBackupImpl(
        Storage storage,
        @SnapshotDelegate Snapshotter delegate,
        Clock clock,
        BackupConfig config,
        Executor executor) {

      this.storage = requireNonNull(storage);
      this.delegate = requireNonNull(delegate);
      this.clock = requireNonNull(clock);
      this.config = requireNonNull(config);
      this.executor = requireNonNull(executor);
      backupDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm", Locale.ENGLISH);
      backupIntervalMs = config.interval.as(Time.MILLISECONDS);
      lastBackupMs = clock.nowMillis();
    }

    @Override
    public Snapshot from(StoreProvider stores) {
      Snapshot snapshot = delegate.from(stores);
      if (clock.nowMillis() >= (lastBackupMs + backupIntervalMs)) {
        executor.execute(() -> save(snapshot));
      }
      return snapshot;
    }

    @Override
    public void backupNow() {
      save(storage.write(delegate::from));
    }

    @VisibleForTesting
    String createBackupName() {
      return FILE_PREFIX + backupDateFormat.format(new Date(clock.nowMillis()));
    }

    private void save(Snapshot snapshot) {
      lastBackupMs = clock.nowMillis();

      String backupName = createBackupName();
      String tempBackupName = "temp_" + backupName;
      File tempFile = new File(config.dir, tempBackupName);
      LOG.info("Saving backup to " + tempFile);
      try (
          OutputStream tempFileStream = new BufferedOutputStream(new FileOutputStream(tempFile))) {

        TTransport transport = new TIOStreamTransport(tempFileStream);
        TProtocol protocol = new TBinaryProtocol(transport);
        snapshot.write(protocol);
        Files.move(tempFile, new File(config.dir, backupName));
        successes.incrementAndGet();
      } catch (IOException e) {
        failures.incrementAndGet();
        LOG.error("Failed to prepare backup " + backupName + ": " + e, e);
      } catch (TException e) {
        LOG.error("Failed to encode backup " + backupName + ": " + e, e);
        failures.incrementAndGet();
      } finally {
        if (tempFile.exists()) {
          LOG.info("Deleting incomplete backup file " + tempFile);
          tryDelete(tempFile);
        }
      }

      File[] backups = config.dir.listFiles(BACKUP_FILTER);
      if (backups == null) {
        LOG.error("Failed to list backup dir " + config.dir);
      } else {
        int backupsToDelete = backups.length - config.maxBackups;
        if (backupsToDelete > 0) {
          List<File> toDelete = Ordering.natural()
              .onResultOf(FILE_NAME)
              .sortedCopy(ImmutableList.copyOf(backups)).subList(0, backupsToDelete);
          LOG.info("Deleting " + backupsToDelete + " outdated backups: " + toDelete);
          for (File outdated : toDelete) {
            tryDelete(outdated);
          }
        }
      }
    }

    private void tryDelete(File fileToDelete) {
      if (!fileToDelete.delete()) {
        LOG.error("Failed to delete file: " + fileToDelete.getName());
      }
    }

    private static final FilenameFilter BACKUP_FILTER = (file, s) -> s.startsWith(FILE_PREFIX);

    @VisibleForTesting
    static final Function<File, String> FILE_NAME = File::getName;

    @Override
    public Stream<Op> asStream(Snapshot snapshot) {
      return delegate.asStream(snapshot);
    }
  }
}
