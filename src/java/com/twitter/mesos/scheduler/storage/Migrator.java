package com.twitter.mesos.scheduler.storage;

import com.twitter.mesos.gen.StorageMigrationPath;
import com.twitter.mesos.gen.StorageMigrationResult;

/**
 * Migrates storage data and provides detailed results.
 *
 * @author John Sirois
 */
public interface Migrator {

  /**
   * Returns the migration path this {@code Migrator} is configured to migrate data along.
   *
   * @return The configured migration path.
   */
  StorageMigrationPath getMigrationPath();

  /**
   * Attempts to migrate data along the configured storage migration path.  Any errors encountered
   * are trapped and recorded in the returned result object.  If migration is successful, subsequent
   * attempts to migrate along the same path are guaranteed to short circuit with a result status of
   * {@link com.twitter.mesos.gen.StorageMigrationStatus#NO_MIGRATION_NEEDED}.
   *
   * @return The result encoding the migration status.
   */
  StorageMigrationResult migrate();
}
