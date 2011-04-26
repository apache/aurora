package com.twitter.mesos.scheduler.storage.db.migrations;

import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.StorageMigrationResult;
import com.twitter.mesos.scheduler.storage.DualStoreMigrator.DataMigrator;
import com.twitter.mesos.scheduler.storage.db.DbStorage;

/**
 * A base class for database migrations that evolve the database schema.  Subclasses need only
 * specify the sql resources to execute.  No mutation of tasks, job configs or the stored framework
 * id will occur unless subclasses override the corresponding {@link DataMigrator} methods.
 *
 * @author John Sirois
 */
public abstract class SchemaMigrator implements DataMigrator {

  private static final Logger LOG = Logger.getLogger(SchemaMigrator.class.getName());

  private final DbStorage from;
  @Nullable private final String fromPrepareSqlResourcePath;
  @Nullable private final String fromFinishSqlResourcePath;
  private final DbStorage to;
  @Nullable private final String toPrepareSqlResourcePath;
  @Nullable private final String toFinishSqlResourcePath;

  /**
   * @param from The legacy database storage
   * @param fromPrepareSqlResourcePath The relative resource path to the sql to run on
   *     {@link #prepare()} against the legacy storage
   * @param fromFinishSqlResourcePath The relative resource path to the sql to run on
   *     {@link #finish(com.twitter.mesos.gen.StorageMigrationResult)} against the legacy storage
   * @param to The new primary database storage
   * @param toPrepareSqlResourcePath The relative resource path to the sql to run on
   *     {@link #prepare()} against the new primary storage
   * @param toFinishSqlResourcePath The relative resource path to the sql to run on
   *     {@link #finish(com.twitter.mesos.gen.StorageMigrationResult)} against the new primary
   *     storage
   */
  protected SchemaMigrator(DbStorage from, @Nullable String fromPrepareSqlResourcePath,
      @Nullable String fromFinishSqlResourcePath, DbStorage to,
      @Nullable String toPrepareSqlResourcePath, @Nullable String toFinishSqlResourcePath) {
    this.from = Preconditions.checkNotNull(from);
    this.fromPrepareSqlResourcePath = fromPrepareSqlResourcePath;
    this.fromFinishSqlResourcePath = fromFinishSqlResourcePath;
    this.to = Preconditions.checkNotNull(to);
    this.toPrepareSqlResourcePath = toPrepareSqlResourcePath;
    this.toFinishSqlResourcePath = toFinishSqlResourcePath;
  }

  @Override
  public final boolean prepare() {
    to.ensureInitialized();
    if (to.hasMigrated(from)) {
      return false;
    }
    if (toPrepareSqlResourcePath != null) {
      LOG.info("Executing prepare phase sql against the new primary store from resource: "
               + toPrepareSqlResourcePath);
      to.executeSql(SchemaMigrator.this.getClass(), toPrepareSqlResourcePath, true);
    }

    if (fromPrepareSqlResourcePath != null) {
      LOG.info("Executing prepare phase sql against the legacy store from resource: "
               + fromPrepareSqlResourcePath);
      from.executeSql(SchemaMigrator.this.getClass(), fromPrepareSqlResourcePath, true);
    }
    from.ensureInitialized();

    return true;
  }

  /**
   * This implementation no-ops; subclasses should override if task structs need to be migrated.
   */
  @Override
  public ScheduledTask migrateTask(ScheduledTask task) {
    return task;
  }

  /**
   * This implementation no-ops; subclasses should override if job config structs need to be
   * migrated.
   */
  @Override
  public JobConfiguration migrateJobConfig(JobConfiguration jobConfiguration) {
    return jobConfiguration;
  }

  /**
   * This implementation no-ops; subclasses should override if the framework id should be migrated.
   */
  @Override
  public String migrateFrameworkId(String frameworkId) {
    return frameworkId;
  }

  @Override
  public final void finish(StorageMigrationResult migrationResult) {
    if (fromFinishSqlResourcePath != null) {
      LOG.info("Executing finish phase sql against the legacy store from resource: "
               + fromFinishSqlResourcePath);
      from.executeSql(SchemaMigrator.this.getClass(), fromFinishSqlResourcePath, true);
    }
    if (toFinishSqlResourcePath != null) {
      LOG.info("Executing finish phase sql against the new primary store from resource: "
               + toFinishSqlResourcePath);
      to.executeSql(SchemaMigrator.this.getClass(), toFinishSqlResourcePath, true);
    }

    to.markMigration(migrationResult);
  }
}
