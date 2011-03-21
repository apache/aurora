package com.twitter.mesos.scheduler.storage;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.twitter.common.base.Function;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.JobManagerMigrationResult;
import com.twitter.mesos.gen.JobMigrationResult;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.SchedulerMigrationResult;
import com.twitter.mesos.gen.StorageMigrationPath;
import com.twitter.mesos.gen.StorageMigrationResult;
import com.twitter.mesos.gen.StorageMigrationStatus;
import com.twitter.mesos.gen.StorageSystemId;
import com.twitter.mesos.gen.TaskMigrationResult;
import com.twitter.mesos.scheduler.JobManager;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.Storage.Work.Quiet;
import com.twitter.mesos.scheduler.storage.StorageRole.Role;
import com.twitter.mesos.scheduler.storage.db.DbStorage;
import com.twitter.mesos.scheduler.storage.stream.MapStorage;

import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * Migrates from one {@link Storage} system to another and provides detailed results.
 *
 * @author John Sirois
 */
public class Migrator {

  private static final Logger LOG = Logger.getLogger(Migrator.class.getName());

  /**
   * {@literal @Named} binding key for the Migrator's job manager ids.
   */
  public static final String JOB_MANAGER_IDS_KEY =
      "com.twitter.mesos.scheduler.storage.Migrator.JOB_MANAGER_IDS_KEY";

  /**
   * Binds the objects required to instantiate a {@code Migrator} via a Guice
   * {@link com.google.inject.Injector}.
   *
   * @param binder The binder to perform the bindings against.
   */
  public static void bind(Binder binder) {
    binder.install(new AbstractModule() {
      @Override protected void configure() {
        requireBinding(Key.get(new TypeLiteral<Set<JobManager>>() {}));
        requireBinding(Key.get(Storage.class, StorageRoles.forRole(Role.Primary)));
        requireBinding(Key.get(Storage.class, StorageRoles.forRole(Role.Legacy)));
      }
      @Provides @Singleton @Named(JOB_MANAGER_IDS_KEY) Set<String> providesJobManagerIds(
          final Set<JobManager> jobManagers) {
        return ImmutableSet.copyOf(Iterables.transform(jobManagers,
            new Function<JobManager, String>() {
              @Override public String apply(JobManager jobManager) {
                return jobManager.getUniqueKey();
              }
            }));
      }
    });
  }

  private Set<String> jobManagerIds;
  private final Storage from;
  private final Storage to;
  private final StorageMigrationPath migrationPath;

  private static final ImmutableSet<StorageMigrationPath> VALID_MIGRATION_PATHS =
      ImmutableSet.of(MigrationUtils.migrationPath(
          new StorageSystemId(MapStorage.STORAGE_SYSTEM_TYPE, 0),
          new StorageSystemId(DbStorage.STORAGE_SYSTEM_TYPE, 0)));

  /**
   * Creates a new {@code Migrator} that can migrate from the given legacy storage system to the new
   * primary storage system.
   *
   * @param jobManagersIds The unique keys for all job managers in the system.
   * @param from The {@code Storage} system to migrate from.
   * @param to The {@code Storage} system to migrate to.
   */
  @Inject
  public Migrator(@Named(JOB_MANAGER_IDS_KEY) Set<String> jobManagersIds,
      @StorageRole(Role.Legacy) Storage from, @StorageRole(Role.Primary) Storage to) {
    this.jobManagerIds = Preconditions.checkNotNull(jobManagersIds);
    this.from = Preconditions.checkNotNull(from);
    this.to = Preconditions.checkNotNull(to);
    migrationPath = MigrationUtils.migrationPath(from, to);
  }

  /**
   * Returns the migration path this {@code Migrator} is configured to migrate data along.
   *
   * @return The configured migration path.
   */
  public StorageMigrationPath getMigrationPath() {
    return migrationPath;
  }

  /**
   * Attempts to migrate data along the configured storage migration path.  Any errors encountered
   * are trapped and recorded in the returned result object.  If migration is successful, subsequent
   * attempts to migrate along the same path are guaranteed to short circuit with a result status of
   * {@link StorageMigrationStatus#NO_MIGRATION_NEEDED}.
   *
   * @return The result encoding the migration status.
   */
  public StorageMigrationResult migrate() {
    // TODO(John Sirois): reconsider the current migration scheme of Migrator having all migration
    // control - its not clear this will always work.  For instance, when the migration is between
    // different versions of the same storage type, it may make sense for the storage system itself
    // to handle the lion's share of the migration (it may need to rely on non-Storage interface
    // methods for efficiency

    if (!isValidMigrationPath()) {
      return new StorageMigrationResult(StorageMigrationStatus.INVALID_MIGRATION_PATH,
          migrationPath);
    }

    if (to.hasMigrated(from)) {
      return new StorageMigrationResult(StorageMigrationStatus.NO_MIGRATION_NEEDED, migrationPath);
    }

    return to.doInTransaction(new Quiet<StorageMigrationResult>() {
      @Override public StorageMigrationResult apply(SchedulerStore toSchedulerStore,
          JobStore toJobStore, TaskStore toTaskStore) {
        return migrateTo(toSchedulerStore, toJobStore, toTaskStore);
      }
    });
  }

  private boolean isValidMigrationPath() {
    return VALID_MIGRATION_PATHS.contains(migrationPath);
  }

  private StorageMigrationResult migrateTo(final SchedulerStore toSchedulerStore,
      final JobStore toJobStore, final TaskStore toTaskStore) {

    return from.doInTransaction(new Work.Quiet<StorageMigrationResult>() {
      @Override public StorageMigrationResult apply(SchedulerStore fromSchedulerStore,
          JobStore fromJobStore, TaskStore fromTaskStore) {

        StorageMigrationResult migrationResult =
            new StorageMigrationResult(StorageMigrationStatus.SUCCESS, migrationPath);

        SchedulerMigrationResult schedulerResult =
            migrateSchedulerStore(fromSchedulerStore, toSchedulerStore);
        if (schedulerResult == null) {
          LOG.info("From store contained no data: " + from.id());
          return migrationResult;
        }

        migrationResult.setTaskResult(migrateTaskStore(fromTaskStore, toTaskStore))
            .setJobManagerResult(migrateJobStore(fromJobStore, toJobStore))
            .setSchedulerResult(schedulerResult);

        to.markMigration(migrationResult);

        return migrationResult;
      }
    });
  }

  private TaskMigrationResult migrateTaskStore(TaskStore fromTaskStore, TaskStore toTaskStore) {
    TaskMigrationResult taskMigrationResult = new TaskMigrationResult();
    try {
      ImmutableSortedSet<ScheduledTask> allTasks = fromTaskStore.fetch(Query.GET_ALL);
      toTaskStore.add(allTasks);
      taskMigrationResult.setMigratedCount(allTasks.size());
    } catch (RuntimeException e) {
      taskMigrationResult.setFailureMessage(Throwables.getStackTraceAsString(e));
    }
    return taskMigrationResult;
  }

  private Map<String, JobManagerMigrationResult> migrateJobStore(JobStore fromJobStore,
      JobStore toJobStore) {

    ImmutableMap.Builder<String, JobManagerMigrationResult> jobManagerMigrationResultBuilder =
        ImmutableMap.builder();

    for (String managerId : jobManagerIds) {
      JobManagerMigrationResult jobManagerMigrationResult = new JobManagerMigrationResult();
      try {
        ImmutableList.Builder<JobMigrationResult> migrationResultBuilder = ImmutableList.builder();
        for (JobConfiguration jobConfiguration : fromJobStore.fetchJobs(managerId)) {
          String jobKey = Tasks.jobKey(jobConfiguration);
          JobMigrationResult result = new JobMigrationResult(jobKey);
          try {
            toJobStore.saveAcceptedJob(managerId, jobConfiguration);
          } catch (RuntimeException e) {
            result.setFailureMessage(Throwables.getStackTraceAsString(e));
          }
          migrationResultBuilder.add(result);
        }
        jobManagerMigrationResult.setJobResult(migrationResultBuilder.build());
      } catch (RuntimeException e) { // fetchJobs failure
        jobManagerMigrationResult.setFailureMessage(Throwables.getStackTraceAsString(e));
      }
      jobManagerMigrationResultBuilder.put(managerId, jobManagerMigrationResult);
    }
    return jobManagerMigrationResultBuilder.build();
  }

  @Nullable
  private SchedulerMigrationResult migrateSchedulerStore(SchedulerStore fromSchedulerStore,
      SchedulerStore toSchedulerStore) {

    SchedulerMigrationResult schedulerMigrationResult = null;
    try {
      String frameworkId = fromSchedulerStore.fetchFrameworkId();
      if (frameworkId == null) {
        return schedulerMigrationResult;
      }
      toSchedulerStore.saveFrameworkId(frameworkId);
      schedulerMigrationResult = new SchedulerMigrationResult();
      schedulerMigrationResult.setMigratedFameworkId(frameworkId);
    } catch (RuntimeException e) {
      schedulerMigrationResult.setFailureMessage(Throwables.getStackTraceAsString(e));
    }
    return schedulerMigrationResult;
  }
}
