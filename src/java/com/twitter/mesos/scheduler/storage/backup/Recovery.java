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

import com.twitter.common.base.Command;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.scheduler.storage.DistributedSnapshotStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A recovery mechanism that works with {@link StorageBackup} to provide a two-step storage
 * recovery process.
 */
public interface Recovery {

  /**
   * List backups available for recovery.
   *
   * @return Available backup IDs.
   */
  Set<String> listBackups();

  /**
   * Loads a backup in 'staging' so that it may be queried and modified prior to committing.
   *
   * @param backupName Name of the backup to load.
   * @throws RecoveryException If the backup could not be found or loaded.
   */
  void stage(String backupName) throws RecoveryException;

  /**
   * Queries a staged backup.
   *
   * @param query Query to perform.
   * @return Tasks matching the query.
   * @throws RecoveryException If a backup is not staged, or could not be queried.
   */
  Set<ScheduledTask> query(TaskQuery query) throws RecoveryException;

  /**
   * Deletes tasks from a staged backup.
   *
   * @param query Query selector for tasks to delete.
   * @throws RecoveryException If a backup is not staged, or tasks could not be deleted.
   */
  void deleteTasks(TaskQuery query) throws RecoveryException;

  /**
   * Unloads a staged backup.
   */
  void unload();

  /**
   * Commits a staged backup the main storage system.
   *
   * @throws RecoveryException If a backup is not staged, or the commit failed.
   */
  void commit() throws RecoveryException;

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

  class RecoveryImpl implements Recovery {
    private final File backupDir;
    private final Function<Snapshot, TemporaryStorage> tempStorageFactory;
    private final AtomicReference<PendingRecovery> recovery;
    private final Storage primaryStorage;
    private final DistributedSnapshotStore distributedStore;
    private final Command shutDownNow;

    @Inject
    RecoveryImpl(
        File backupDir,
        Function<Snapshot, TemporaryStorage> tempStorageFactory,
        Storage primaryStorage,
        DistributedSnapshotStore distributedStore,
        Command shutDownNow) {

      this.backupDir = checkNotNull(backupDir);
      this.tempStorageFactory = checkNotNull(tempStorageFactory);
      this.recovery = Atomics.newReference();
      this.primaryStorage = checkNotNull(primaryStorage);
      this.distributedStore = checkNotNull(distributedStore);
      this.shutDownNow = checkNotNull(shutDownNow);
    }

    @Override public Set<String> listBackups() {
      return ImmutableSet.<String>builder().add(backupDir.list()).build();
    }

    @Override public void stage(String backupName) throws RecoveryException {
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

    private PendingRecovery getLoadedRecovery() throws RecoveryException {
      @Nullable PendingRecovery loaded = this.recovery.get();
      if (loaded == null) {
        throw new RecoveryException("No backup loaded.");
      }
      return loaded;
    }

    @Override public Set<ScheduledTask> query(TaskQuery query) throws RecoveryException {
      return getLoadedRecovery().query(query);
    }

    @Override public void deleteTasks(TaskQuery query) throws RecoveryException {
      getLoadedRecovery().delete(query);
    }

    @Override public void unload() {
      recovery.set(null);
    }

    @Override public void commit() throws RecoveryException {
      getLoadedRecovery().commit();
    }

    private class PendingRecovery {
      private final TemporaryStorage tempStorage;

      PendingRecovery(TemporaryStorage tempStorage) {
        this.tempStorage = tempStorage;
      }

      void commit() {
        primaryStorage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
          @Override protected void execute(MutableStoreProvider storeProvider) {
            try {
              distributedStore.persist(tempStorage.toSnapshot());
              shutDownNow.execute();
            } catch (CodingException e) {
              throw new IllegalStateException("Failed to encode snapshot.", e);
            }
          }
        });
      }

      Set<ScheduledTask> query(final TaskQuery query) {
        return tempStorage.fetchTasks(query);
      }

      void delete(final TaskQuery query) {
        tempStorage.deleteTasks(query);
      }
    }
  }
}
