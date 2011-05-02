package com.twitter.mesos.executor.migrations.v0_v1;

import java.util.logging.Logger;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.executor.DeadTask;
import com.twitter.mesos.executor.TaskOnDisk.TaskStorageException;
import com.twitter.mesos.executor.migrations.DeadTaskMigrator;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.migrations.v0_v1.OwnerIdentities;

/**
 * Migrates old style thrift struct owner strings to storage v1 style
 * {@link com.twitter.mesos.gen.Identity} owners.
 *
 * @author John Sirois
 */
public class OwnerMigrator implements DeadTaskMigrator {

  private static final Logger LOG = Logger.getLogger(OwnerMigrator.class.getName());

  @Override
  public boolean migrateDeadTask(DeadTask task) throws TaskStorageException {
    TwitterTaskInfo taskInfo = task.getAssignedTask().getTask();
    Identity migratedOwner = migrateIdentity(taskInfo);
    if (migratedOwner.equals(taskInfo.getOwner())) {
      return false;
    }

    taskInfo.setOwner(migratedOwner);
    task.recordTask();
    LOG.info("Migrated task " + task.getId() + " " + Tasks.jobKey(taskInfo));
    return true;
  }

  private Identity migrateIdentity(TwitterTaskInfo taskInfo) throws TaskStorageException {
    try {
      return OwnerIdentities.getOwner(taskInfo);
    } catch (IllegalArgumentException e) {
      throw new TaskStorageException("Failed to migrate owner for " + taskInfo, e);
    }
  }
}
