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
import java.util.concurrent.Executor;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Function;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.storage.Snapshotter;
import org.apache.aurora.scheduler.storage.backup.Recovery.RecoveryImpl;
import org.apache.aurora.scheduler.storage.backup.StorageBackup.StorageBackupImpl;
import org.apache.aurora.scheduler.storage.backup.StorageBackup.StorageBackupImpl.BackupConfig;
import org.apache.aurora.scheduler.storage.backup.TemporaryStorage.TemporaryStorageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * A module that will periodically save full storage backups to local disk and makes those backups
 * available for on-line recovery.
 */
public class BackupModule extends PrivateModule {
  private static final Logger LOG = LoggerFactory.getLogger(BackupModule.class);

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-backup_interval",
        description = "Minimum interval on which to write a storage backup.")
    public TimeAmount backupInterval = new TimeAmount(1, Time.HOURS);

    @Parameter(names = "-max_saved_backups",
        description = "Maximum number of backups to retain before deleting the oldest backups.")
    public int maxSavedBackups = 48;

    @Parameter(names = "-backup_dir",
        required = true,
        description = "Directory to store backups under. Will be created if it does not exist.")
    public File backupDir;
  }

  private final Options options;
  private final Class<? extends Snapshotter> snapshotStore;

  public BackupModule(Options options, Class<? extends Snapshotter> snapshotStore) {
    this.options = options;
    this.snapshotStore = snapshotStore;
  }

  @Override
  protected void configure() {
    Executor executor = AsyncUtil.singleThreadLoggingScheduledExecutor("StorageBackup-%d", LOG);
    bind(Executor.class).toInstance(executor);

    bind(Snapshotter.class).annotatedWith(StorageBackupImpl.SnapshotDelegate.class)
        .to(snapshotStore);

    bind(Snapshotter.class).to(StorageBackupImpl.class);
    bind(StorageBackup.class).to(StorageBackupImpl.class);
    bind(StorageBackupImpl.class).in(Singleton.class);
    expose(Snapshotter.class);
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
      this.lifecycle = requireNonNull(lifecycle);
    }

    @Override
    public void execute() {
      lifecycle.shutdown();
    }
  }

  @Provides
  File provideBackupDir() {
    if (!options.backupDir.exists()) {
      if (options.backupDir.mkdirs()) {
        LOG.info("Created backup dir " + options.backupDir.getPath() + ".");
      } else {
        throw new IllegalArgumentException(
            "Unable to create backup dir " + options.backupDir.getPath() + ".");
      }
    }

    if (!options.backupDir.canWrite()) {
      throw new IllegalArgumentException(
          "Backup dir " + options.backupDir.getPath() + " is not writable.");
    }

    return options.backupDir;
  }

  @Provides
  BackupConfig provideBackupConfig(File backupDir) {
    return new BackupConfig(backupDir, options.maxSavedBackups, options.backupInterval);
  }
}
