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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Atomics;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import static java.util.Objects.requireNonNull;

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
   * @param query Builder of query to perform.
   * @return Tasks matching the query.
   * @throws RecoveryException If a backup is not staged, or could not be queried.
   */
  Iterable<IScheduledTask> query(Query.Builder query) throws RecoveryException;

  /**
   * Deletes tasks from a staged backup.
   *
   * @param query Query builder for tasks to delete.
   * @throws RecoveryException If a backup is not staged, or tasks could not be deleted.
   */
  void deleteTasks(Query.Builder query) throws RecoveryException;

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
  class RecoveryException extends RuntimeException {
    public RecoveryException(String message) {
      super(message);
    }

    public RecoveryException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  class RecoveryImpl implements Recovery {
    private final File backupDir;
    private final Function<Snapshot, TemporaryStorage> tempStorageFactory;
    private final AtomicReference<PendingRecovery> recovery;
    private final Storage primaryStorage;
    private final SnapshotStore snapshotStore;
    private final Command shutDownNow;

    @Inject
    RecoveryImpl(
        File backupDir,
        Function<Snapshot, TemporaryStorage> tempStorageFactory,
        Storage primaryStorage,
        SnapshotStore snapshotStore,
        Command shutDownNow) {

      this.backupDir = requireNonNull(backupDir);
      this.tempStorageFactory = requireNonNull(tempStorageFactory);
      this.recovery = Atomics.newReference();
      this.primaryStorage = requireNonNull(primaryStorage);
      this.snapshotStore = requireNonNull(snapshotStore);
      this.shutDownNow = requireNonNull(shutDownNow);
    }

    @Override
    public Set<String> listBackups() {
      return ImmutableSet.<String>builder().add(backupDir.list()).build();
    }

    @Override
    public void stage(String backupName) throws RecoveryException {
      Snapshot snapshot = load(new File(backupDir, backupName));
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

    @Override
    public Iterable<IScheduledTask> query(Query.Builder query) throws RecoveryException {
      return getLoadedRecovery().query(query);
    }

    @Override
    public void deleteTasks(Query.Builder query) throws RecoveryException {
      getLoadedRecovery().delete(query);
    }

    @Override
    public void unload() {
      recovery.set(null);
    }

    @Override
    public void commit() throws RecoveryException {
      getLoadedRecovery().commit();
    }

    private class PendingRecovery {
      private final TemporaryStorage tempStorage;

      PendingRecovery(TemporaryStorage tempStorage) {
        this.tempStorage = tempStorage;
      }

      void commit() {
        primaryStorage.write((NoResult.Quiet) storeProvider -> {
          try {
            snapshotStore.snapshotWith(tempStorage.toSnapshot());
            shutDownNow.execute();
          } catch (CodingException e) {
            throw new IllegalStateException("Failed to encode snapshot.", e);
          }
        });
      }

      Iterable<IScheduledTask> query(final Query.Builder query) {
        return tempStorage.fetchTasks(query);
      }

      void delete(final Query.Builder query) {
        tempStorage.deleteTasks(query);
      }
    }
  }

  static Snapshot load(File backupFile) throws RecoveryException {
    if (!backupFile.exists()) {
      throw new RecoveryException("Backup " + backupFile + " does not exist.");
    }

    try {
      Snapshot snapshot = new Snapshot();
      TBinaryProtocol prot = new TBinaryProtocol(
          new TIOStreamTransport(new BufferedInputStream(new FileInputStream(backupFile))));
      snapshot.read(prot);
      return snapshot;
    } catch (TException e) {
      throw new RecoveryException("Failed to decode backup " + e, e);
    } catch (IOException e) {
      throw new RecoveryException("Failed to read backup " + e, e);
    }
  }
}
