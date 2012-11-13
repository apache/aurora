package com.twitter.mesos.scheduler.storage.backup;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Atomics;
import com.google.inject.Inject;

import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.scheduler.storage.SnapshotStore;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A recovery mechanism that works with {@link StorageBackup} to provide a two-step storage
 * recovery process.
 */
class Recovery {
  private final File backupDir;
  private final SnapshotStore<Snapshot> snapshotStore;
  private final Function<Snapshot, TemporaryStorage> tempStorageFactory;
  private final AtomicReference<PendingRecovery> recovery;

  @Inject
  Recovery(
      File backupDir,
      SnapshotStore<Snapshot> snapshotStore,
      Function<Snapshot, TemporaryStorage> tempStorageFactory) {

    this.backupDir = checkNotNull(backupDir);
    this.snapshotStore = checkNotNull(snapshotStore);
    this.tempStorageFactory = checkNotNull(tempStorageFactory);
    this.recovery = Atomics.newReference();
  }

  Set<String> listBackups() {
    return ImmutableSet.<String>builder().add(backupDir.list()).build();
  }

  void loadBackup(String backupName) throws RecoveryException {
    File backupFile = new File(backupDir, backupName);
    if (!backupFile.exists()) {
      throw new RecoveryException("Backup " + backupName + " does not exist.");
    }

    Snapshot snapshot;
    try {
      snapshot = ThriftBinaryCodec.decode(Snapshot.class, Files.toByteArray(backupFile));
    } catch (CodingException e) {
      throw new RecoveryException("Failed to decode backup " + e, e);
    } catch (IOException e) {
      throw new RecoveryException("Failed to read backup " + e, e);
    }
    boolean applied =
        recovery.compareAndSet(null, new PendingRecovery(tempStorageFactory.apply(snapshot)));
    if (!applied) {
      throw new RecoveryException("Another backup is already loaded.");
    }
  }

  PendingRecovery getLoadedRecovery() throws RecoveryException {
    @Nullable PendingRecovery loaded = this.recovery.get();
    if (loaded == null) {
      throw new RecoveryException("No backup loaded.");
    }
    return loaded;
  }

  Set<ScheduledTask> query(TaskQuery query) throws RecoveryException {
    return getLoadedRecovery().query(query);
  }

  void deleteTasks(TaskQuery query) throws RecoveryException {
    getLoadedRecovery().delete(query);
  }

  void unload() {
    recovery.set(null);
  }

  void commit() throws RecoveryException {
    getLoadedRecovery().commit();
  }

  private class PendingRecovery {
    private final TemporaryStorage tempStorage;

    PendingRecovery(TemporaryStorage tempStorage) {
      this.tempStorage = tempStorage;
    }

    void commit() {
      snapshotStore.applySnapshot(tempStorage.toSnapshot());
      unload();
    }

    Set<ScheduledTask> query(final TaskQuery query) {
      return tempStorage.fetchTasks(query);
    }

    void delete(final TaskQuery query) {
      tempStorage.deleteTasks(query);
    }
  }

  /**
   * Thrown when a recovery operation could not be completed due to internal errors or improper
   * invocation order.
   */
  public static class RecoveryException extends Exception {
    RecoveryException(String message) {
      super(message);
    }

    RecoveryException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
