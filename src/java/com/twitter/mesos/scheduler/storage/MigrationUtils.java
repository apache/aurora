package com.twitter.mesos.scheduler.storage;

import com.google.common.base.Preconditions;
import com.twitter.mesos.gen.StorageMigrationPath;
import com.twitter.mesos.gen.StorageSystemId;

/**
 * Utilities for dealing with storage migration.
 *
 * @author John Sirois
 */
public final class MigrationUtils {

  private MigrationUtils() {
    // utility
  }

  /**
   * Creates a migration path describing migration from {@code from} to {@code to}.
   *
   * @param from The storage system to migrate data from.
   * @param to The storage system to migrate data to.
   * @return A migration path describing migration from {@code from} to {@code to}.
   */
  public static StorageMigrationPath migrationPath(Storage from, Storage to) {
    return new StorageMigrationPath(id(from), id(to));
  }

  /**
   * Creates a migration path describing migration from one storage system to another.
   *
   * @param from The storage system to migrate data from.
   * @param to The storage system to migrate data to.
   * @return A migration path describing migration from one storage system to the other.
   */
  public static StorageMigrationPath migrationPath(StorageSystemId from, StorageSystemId to) {
    Preconditions.checkNotNull(from);
    Preconditions.checkNotNull(to);
    return new StorageMigrationPath(from, to);
  }

  /**
   * Creates an identifier for a storage system at its current version.
   *
   * @param storage The storage system to create an identifier for.
   * @return An identifier encoding the type and version of {@code storage}.
   */
  public static StorageSystemId id(Storage storage) {
    Preconditions.checkNotNull(storage);
    return storage.id();
  }
}
