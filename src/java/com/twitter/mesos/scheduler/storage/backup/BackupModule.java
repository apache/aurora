package com.twitter.mesos.scheduler.storage.backup;

import java.io.File;

import com.google.common.base.Function;
import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.scheduler.storage.SnapshotStore;
import com.twitter.mesos.scheduler.storage.backup.Recovery.RecoveryImpl;
import com.twitter.mesos.scheduler.storage.backup.StorageBackup.StorageBackupImpl;
import com.twitter.mesos.scheduler.storage.backup.StorageBackup.StorageBackupImpl.BackupConfig;
import com.twitter.mesos.scheduler.storage.backup.TemporaryStorage.TemporaryStorageFactory;

/**
 * A module that will periodically save full storage backups to local disk and makes those backups
 * available for on-line recovery.
 */
public class BackupModule extends PrivateModule {

  @CmdLine(name = "backup_interval", help = "Minimum interval on which to write a storage backup.")
  private static final Arg<Amount<Long, Time>> BACKUP_INTERVAL =
      Arg.create(Amount.of(6L, Time.HOURS));

  @CmdLine(name = "max_saved_backups",
      help = "Maximum number of backups to retain before deleting the oldest backups.")
  private static final Arg<Integer> MAX_SAVED_BACKUPS = Arg.create(10);

  private final File backupDir;
  private final Class<? extends SnapshotStore<Snapshot>> snapshotStore;

  /**
   * Creates a new backup module.
   *
   * @param snapshotStore Snapshot store implementation class.
   */
  public BackupModule(File backupDir, Class<? extends SnapshotStore<Snapshot>> snapshotStore) {
    this.backupDir = backupDir;
    this.snapshotStore = snapshotStore;
  }

  @Override
  protected void configure() {
    TypeLiteral<SnapshotStore<Snapshot>> type = new TypeLiteral<SnapshotStore<Snapshot>>() { };
    bind(type).annotatedWith(StorageBackupImpl.SnapshotDelegate.class).to(snapshotStore);

    bind(BackupConfig.class)
        .toInstance(new BackupConfig(backupDir, MAX_SAVED_BACKUPS.get(), BACKUP_INTERVAL.get()));
    bind(File.class).toInstance(backupDir);
    bind(type).to(StorageBackupImpl.class);
    bind(StorageBackup.class).to(StorageBackupImpl.class);
    bind(StorageBackupImpl.class).in(Singleton.class);
    expose(type);
    expose(StorageBackup.class);

    bind(new TypeLiteral<Function<Snapshot, TemporaryStorage>>() { })
        .to(TemporaryStorageFactory.class);

    bind(Recovery.class).to(RecoveryImpl.class);
    bind(RecoveryImpl.class).in(Singleton.class);
    expose(Recovery.class);
  }
}
