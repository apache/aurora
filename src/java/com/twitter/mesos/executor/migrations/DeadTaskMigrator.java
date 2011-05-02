package com.twitter.mesos.executor.migrations;

import com.twitter.mesos.executor.DeadTask;
import com.twitter.mesos.executor.TaskOnDisk.TaskStorageException;
import com.twitter.mesos.gen.AssignedTask;

/**
 * A strategy for preparing transforming {@link AssignedTask}s persisted as dead tasks by an
 * executor to valid {@code AssignedTask}s in a thrift IDL migration.
 *
 * @author John Sirois
 */
public interface DeadTaskMigrator {

  /**
   * Migrates an old {@code task} to a new form on disk if needed.
   *
   * @param task A dead task recorded in the previous version
   * @return the {@code true} if the task was migrated and {@code false} if it was up to date
   * @throws TaskStorageException if there was a problem migrating the dead task
   */
  boolean migrateDeadTask(DeadTask task) throws TaskStorageException;

  /**
   * A migrator that does nothing.
   */
  DeadTaskMigrator NOOP = new DeadTaskMigrator() {
    @Override public boolean migrateDeadTask(DeadTask task) {
      return false;
    }
  };
}
