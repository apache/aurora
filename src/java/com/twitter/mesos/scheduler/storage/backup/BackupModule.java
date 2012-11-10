package com.twitter.mesos.scheduler.storage.backup;

import java.io.File;

import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.Exists;
import com.twitter.common.args.constraints.IsDirectory;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.scheduler.storage.SnapshotStore;
import com.twitter.mesos.scheduler.storage.backup.StorageBackup.SnapshotDelegate;

/**
 * A module that will periodically save full storage backups to local disk.
 */
public class BackupModule extends PrivateModule {

  @CmdLine(name = "backup_interval", help = "Minimum interval on which to write a storage backup.")
  private static final Arg<Amount<Long, Time>> BACKUP_INTERVAL =
      Arg.create(Amount.of(6L, Time.HOURS));

  // TODO(William Farner): Uncomment once this module is used.
  // @NotNull
  @Exists
  @IsDirectory
  @CmdLine(name = "backup_dir", help = "Directory to store backups under.")
  private static final Arg<File> BACKUP_DIR = Arg.create();

  private final Class<? extends SnapshotStore<Snapshot>> snapshotStore;

  /**
   * Creates a new backup module.
   *
   * @param snapshotStore Snapshot store implementation class.
   */
  public BackupModule(Class<? extends SnapshotStore<Snapshot>> snapshotStore) {
    this.snapshotStore = snapshotStore;
  }

  @Override
  protected void configure() {
    TypeLiteral<SnapshotStore<Snapshot>> type = new TypeLiteral<SnapshotStore<Snapshot>>() { };
    bind(type).annotatedWith(SnapshotDelegate.class).to(snapshotStore);
    bind(new TypeLiteral<Amount<Long, Time>>() { }).toInstance(BACKUP_INTERVAL.get());
    bind(File.class).toInstance(BACKUP_DIR.get());
    bind(type).to(StorageBackup.class);
    bind(StorageBackup.class).in(Singleton.class);
    expose(type);
  }
}
