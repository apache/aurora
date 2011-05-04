package com.twitter.mesos.scheduler.storage.db.migrations.v0_v1;

import java.io.IOException;

import com.google.inject.Inject;

import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.migrations.v0_v1.OwnerIdentities;
import com.twitter.mesos.scheduler.storage.StorageRole;
import com.twitter.mesos.scheduler.storage.StorageRole.Role;
import com.twitter.mesos.scheduler.storage.db.DbStorage;
import com.twitter.mesos.scheduler.storage.db.migrations.SchemaMigrator;

/**
 * Migrates old style thrift struct owner strings to storage v1 style {@link Identity} owners.
 *
 * @author John Sirois
 */
public class OwnerMigrator extends SchemaMigrator {

  @Inject
  public OwnerMigrator(@StorageRole(Role.Legacy) DbStorage from,
      @StorageRole(Role.Primary) DbStorage to) throws IOException {
    super(from, "pre-migrate-legacy.sql", null, to, null, null);
  }

  @Override
  public ScheduledTask migrateTask(ScheduledTask task) {
    ScheduledTask migrated = task.deepCopy();
    migrated.getAssignedTask().getTask().setOwner(getOwner(task));
    return migrated;
  }

  @Override
  public JobConfiguration migrateJobConfig(JobConfiguration jobConfiguration) {
    return OwnerIdentities.repairOwnership(jobConfiguration);
  }

  public static Identity getOwner(ScheduledTask scheduledTask) {
    return OwnerIdentities.getOwner(scheduledTask.getAssignedTask().getTask());
  }
}
