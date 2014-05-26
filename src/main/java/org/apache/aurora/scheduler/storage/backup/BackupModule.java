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

import java.io.File;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.twitter.common.application.Lifecycle;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.backup.Recovery.RecoveryImpl;
import org.apache.aurora.scheduler.storage.backup.StorageBackup.StorageBackupImpl;
import org.apache.aurora.scheduler.storage.backup.StorageBackup.StorageBackupImpl.BackupConfig;
import org.apache.aurora.scheduler.storage.backup.TemporaryStorage.TemporaryStorageFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A module that will periodically save full storage backups to local disk and makes those backups
 * available for on-line recovery.
 */
public class BackupModule extends PrivateModule {
  private static final Logger LOG = Logger.getLogger(BackupModule.class.getName());

  @CmdLine(name = "backup_interval", help = "Minimum interval on which to write a storage backup.")
  private static final Arg<Amount<Long, Time>> BACKUP_INTERVAL =
      Arg.create(Amount.of(1L, Time.HOURS));

  @CmdLine(name = "max_saved_backups",
      help = "Maximum number of backups to retain before deleting the oldest backups.")
  private static final Arg<Integer> MAX_SAVED_BACKUPS = Arg.create(48);

  @NotNull
  @CmdLine(name = "backup_dir",
      help = "Directory to store backups under. Will be created if it does not exist.")
  private static final Arg<File> BACKUP_DIR = Arg.create();

  private final Class<? extends SnapshotStore<Snapshot>> snapshotStore;
  private final File unvalidatedBackupDir;

  /**
   * Creates a new backup module.
   *
   * @param snapshotStore Snapshot store implementation class.
   */
  public BackupModule(Class<? extends SnapshotStore<Snapshot>> snapshotStore) {
    this(BACKUP_DIR.get(), snapshotStore);
  }

  /**
   * Creates a new backup module using a given backupDir instead of a flagged one.
   *
   * @param backupDir Directory to write backups to.
   * @param snapshotStore Snapshot store implementation class.
   */
  @VisibleForTesting
  public BackupModule(File backupDir, Class<? extends SnapshotStore<Snapshot>> snapshotStore) {
    this.unvalidatedBackupDir = checkNotNull(backupDir);
    this.snapshotStore = checkNotNull(snapshotStore);
  }

  @Override
  protected void configure() {
    TypeLiteral<SnapshotStore<Snapshot>> type = new TypeLiteral<SnapshotStore<Snapshot>>() { };
    bind(type).annotatedWith(StorageBackupImpl.SnapshotDelegate.class).to(snapshotStore);

    bind(type).to(StorageBackupImpl.class);
    bind(StorageBackup.class).to(StorageBackupImpl.class);
    bind(StorageBackupImpl.class).in(Singleton.class);
    expose(type);
    expose(StorageBackup.class);

    bind(new TypeLiteral<Function<Snapshot, TemporaryStorage>>() { })
        .to(TemporaryStorageFactory.class);

    bind(Command.class).to(LifecycleHook.class);
    bind(Recovery.class).to(RecoveryImpl.class);
    bind(RecoveryImpl.class).in(Singleton.class);
    expose(Recovery.class);
  }

  static class LifecycleHook implements Command {
    private final Lifecycle lifecycle;

    @Inject LifecycleHook(Lifecycle lifecycle) {
      this.lifecycle = checkNotNull(lifecycle);
    }

    @Override
    public void execute() {
      lifecycle.shutdown();
    }
  }

  @Provides
  private File provideBackupDir() {
    if (!unvalidatedBackupDir.exists()) {
      if (!unvalidatedBackupDir.mkdirs()) {
        throw new IllegalArgumentException(
            "Unable to create backup dir " + unvalidatedBackupDir.getPath() + ".");
      } else {
        LOG.info("Created backup dir " + unvalidatedBackupDir.getPath() + ".");
      }
    }

    if (!unvalidatedBackupDir.canWrite()) {
      throw new IllegalArgumentException(
          "Backup dir " + unvalidatedBackupDir.getPath() + " is not writable.");
    }

    return unvalidatedBackupDir;
  }

  @Provides
  private BackupConfig provideBackupConfig(File backupDir) {
    return new BackupConfig(backupDir, MAX_SAVED_BACKUPS.get(), BACKUP_INTERVAL.get());
  }
}
