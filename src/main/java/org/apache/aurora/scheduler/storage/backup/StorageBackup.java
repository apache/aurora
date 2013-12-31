/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.storage.backup;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.io.Files;
import com.google.inject.BindingAnnotation;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.storage.SnapshotStore;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A backup routine that layers over a snapshot store and periodically writes snapshots to
 * local disk.
 *
 * TODO(William Farner): Perform backups asynchronously.  As written, they are performed in a
 * blocking write operation, which is asking for trouble.
 */
public interface StorageBackup {

  /**
   * Perform a storage backup immediately, blocking until it is complete.
   */
  void backupNow();

  class StorageBackupImpl implements StorageBackup, SnapshotStore<Snapshot> {
    private static final Logger LOG = Logger.getLogger(StorageBackup.class.getName());

    private static final String FILE_PREFIX = "scheduler-backup-";
    private final BackupConfig config;

    static class BackupConfig {
      private final File dir;
      private final int maxBackups;
      private final Amount<Long, Time> interval;

      BackupConfig(File dir, int maxBackups, Amount<Long, Time> interval) {
        this.dir = checkNotNull(dir);
        this.maxBackups = maxBackups;
        this.interval = checkNotNull(interval);
      }

      @VisibleForTesting
      File getDir() {
        return dir;
      }
    }

    /**
     * Binding annotation that the underlying {@link SnapshotStore} must be bound with.
     */
    @BindingAnnotation
    @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME)
    @interface SnapshotDelegate { }

    private final SnapshotStore<Snapshot> delegate;
    private final Clock clock;
    private final long backupIntervalMs;
    private volatile long lastBackupMs;
    private final DateFormat backupDateFormat;

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
        @SnapshotDelegate SnapshotStore<Snapshot> delegate,
        Clock clock,
        BackupConfig config) {

      this.delegate = checkNotNull(delegate);
      this.clock = checkNotNull(clock);
      this.config = checkNotNull(config);
      backupDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
      backupIntervalMs = config.interval.as(Time.MILLISECONDS);
      lastBackupMs = clock.nowMillis();
    }

    @Override public Snapshot createSnapshot() {
      Snapshot snapshot = delegate.createSnapshot();
      if (clock.nowMillis() >= (lastBackupMs + backupIntervalMs)) {
        save(snapshot);
      }
      return snapshot;
    }

    @Override public void backupNow() {
      save(delegate.createSnapshot());
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
      try {
        byte[] backup = ThriftBinaryCodec.encodeNonNull(snapshot);
        Files.write(backup, tempFile);
        Files.move(tempFile, new File(config.dir, backupName));
        successes.incrementAndGet();
      } catch (IOException e) {
        failures.incrementAndGet();
        LOG.log(Level.SEVERE, "Failed to prepare backup " + backupName + ": " + e, e);
      } catch (CodingException e) {
        LOG.log(Level.SEVERE, "Failed to encode backup " + backupName + ": " + e, e);
        failures.incrementAndGet();
      } finally {
        if (tempFile.exists()) {
          LOG.info("Deleting incomplete backup file " + tempFile);
          tempFile.delete();
        }
      }

      File[] backups = config.dir.listFiles(BACKUP_FILTER);
      if (backups == null) {
        LOG.severe("Failed to list backup dir " + config.dir);
      } else {
        int backupsToDelete = backups.length - config.maxBackups;
        if (backupsToDelete > 0) {
          List<File> toDelete = Ordering.natural()
              .onResultOf(FILE_NAME)
              .sortedCopy(ImmutableList.copyOf(backups)).subList(0, backupsToDelete);
          LOG.info("Deleting " + backupsToDelete + " outdated backups: " + toDelete);
          for (File outdated : toDelete) {
            outdated.delete();
          }
        }
      }
    }

    private static final FilenameFilter BACKUP_FILTER = new FilenameFilter() {
      @Override public boolean accept(File file, String s) {
        return s.startsWith(FILE_PREFIX);
      }
    };

    @VisibleForTesting
    static final Function<File, String> FILE_NAME = new Function<File, String>() {
      @Override public String apply(File file) {
        return file.getName();
      }
    };

    @Override
    public void applySnapshot(Snapshot snapshot) {
      delegate.applySnapshot(snapshot);
    }
  }
}
