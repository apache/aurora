package com.twitter.mesos.scheduler.storage.backup;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Files;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.scheduler.storage.SnapshotStore;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A backup routine that layers over a snapshot store and periodically writes snapshots to
 * local disk.
 */
class StorageBackup implements SnapshotStore<Snapshot> {

  private static final Logger LOG = Logger.getLogger(StorageBackup.class.getName());

  /**
   * Binding annotation that the underlying {@link SnapshotStore} must be bound with.
   */
  @BindingAnnotation
  @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME)
  @interface SnapshotDelegate { }

  private final SnapshotStore<Snapshot> delegate;
  private final Clock clock;
  private final long backupIntervalMs;
  private volatile long nextSnapshotMs;
  private final File backupDir;
  private final DateFormat backupDateFormat;

  @VisibleForTesting
  final AtomicLong successes = Stats.exportLong("scheduler_backup_success");
  @VisibleForTesting
  final AtomicLong failures = Stats.exportLong("scheduler_backup_failed");

  @Inject
  StorageBackup(
      @SnapshotDelegate SnapshotStore<Snapshot> delegate,
      Clock clock,
      Amount<Long, Time> backupInterval,
      File backupDir) {

    this.delegate = checkNotNull(delegate);
    this.clock = checkNotNull(clock);
    this.backupDir = checkNotNull(backupDir);
    backupDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    backupIntervalMs = backupInterval.as(Time.MILLISECONDS);
    nextSnapshotMs = clock.nowMillis() + backupIntervalMs;
  }

  @Override
  public Snapshot createSnapshot() {
    Snapshot snapshot = delegate.createSnapshot();
    if (clock.nowMillis() >= nextSnapshotMs) {
      nextSnapshotMs += backupIntervalMs;
      save(snapshot);
    }

    return snapshot;
  }

  @VisibleForTesting
  String createBackupName() {
    return backupDateFormat.format(new Date(clock.nowMillis()));
  }

  private void save(Snapshot snapshot) {
    String backupName = createBackupName();
    String tempBackupName = "temp_" + backupName;
    File tempFile = new File(backupDir, tempBackupName);
    try {
      byte[] backup = ThriftBinaryCodec.encodeNonNull(snapshot);
      Files.write(backup, tempFile);
      Files.move(tempFile, new File(backupDir, backupName));
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
  }

  @Override
  public void applySnapshot(Snapshot snapshot) {
    delegate.applySnapshot(snapshot);
  }
}
