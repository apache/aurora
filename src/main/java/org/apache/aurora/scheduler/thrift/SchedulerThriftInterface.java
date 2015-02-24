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
package org.apache.aurora.scheduler.thrift;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Range;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.Positive;

import org.apache.aurora.auth.CapabilityValidator;
import org.apache.aurora.auth.CapabilityValidator.AuditCheck;
import org.apache.aurora.auth.SessionValidator.AuthFailedException;
import org.apache.aurora.gen.AcquireLockResult;
import org.apache.aurora.gen.AddInstancesConfig;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.ConfigRewrite;
import org.apache.aurora.gen.DrainHostsResult;
import org.apache.aurora.gen.EndMaintenanceResult;
import org.apache.aurora.gen.Hosts;
import org.apache.aurora.gen.InstanceConfigRewrite;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfigRewrite;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdatePulseStatus;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.ListBackupsResult;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.LockKey._Fields;
import org.apache.aurora.gen.LockValidation;
import org.apache.aurora.gen.MaintenanceStatusResult;
import org.apache.aurora.gen.PulseJobUpdateResult;
import org.apache.aurora.gen.QueryRecoveryResult;
import org.apache.aurora.gen.ReadOnlyScheduler;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.RewriteConfigsRequest;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.SessionKey;
import org.apache.aurora.gen.StartJobUpdateResult;
import org.apache.aurora.gen.StartMaintenanceResult;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Numbers;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.SanitizedCronJob;
import org.apache.aurora.scheduler.quota.QuotaCheckResult;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.quota.QuotaManager.QuotaException;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.state.LockManager.LockException;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.UUIDGenerator;
import org.apache.aurora.scheduler.storage.JobStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateRequest;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSettings;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.thrift.auth.DecoratedThrift;
import org.apache.aurora.scheduler.thrift.auth.Requires;
import org.apache.aurora.scheduler.updater.JobDiff;
import org.apache.aurora.scheduler.updater.JobUpdateController;
import org.apache.aurora.scheduler.updater.UpdateStateException;
import org.apache.thrift.TException;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

import static com.google.common.base.CharMatcher.WHITESPACE;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

import static org.apache.aurora.auth.CapabilityValidator.Capability.MACHINE_MAINTAINER;
import static org.apache.aurora.auth.CapabilityValidator.Capability.PROVISIONER;
import static org.apache.aurora.auth.CapabilityValidator.Capability.ROOT;
import static org.apache.aurora.auth.CapabilityValidator.Capability.UPDATE_COORDINATOR;
import static org.apache.aurora.auth.SessionValidator.SessionContext;
import static org.apache.aurora.gen.ResponseCode.AUTH_FAILED;
import static org.apache.aurora.gen.ResponseCode.INVALID_REQUEST;
import static org.apache.aurora.gen.ResponseCode.LOCK_ERROR;
import static org.apache.aurora.gen.ResponseCode.OK;
import static org.apache.aurora.gen.ResponseCode.WARNING;
import static org.apache.aurora.scheduler.base.Tasks.ACTIVE_STATES;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.INSUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.thrift.Responses.addMessage;
import static org.apache.aurora.scheduler.thrift.Responses.empty;
import static org.apache.aurora.scheduler.thrift.Responses.error;
import static org.apache.aurora.scheduler.thrift.Responses.invalidRequest;
import static org.apache.aurora.scheduler.thrift.Responses.ok;

/**
 * Aurora scheduler thrift server implementation.
 * <p/>
 * Interfaces between users and the scheduler to access/modify jobs and perform cluster
 * administration tasks.
 */
@DecoratedThrift
class SchedulerThriftInterface implements AuroraAdmin.Iface {
  @Positive
  @CmdLine(name = "max_tasks_per_job", help = "Maximum number of allowed tasks in a single job.")
  public static final Arg<Integer> MAX_TASKS_PER_JOB = Arg.create(4000);

  // This number is derived from the maximum file name length limit on most UNIX systems, less
  // the number of characters we've observed being added by mesos for the executor ID, prefix, and
  // delimiters.
  @VisibleForTesting
  static final int MAX_TASK_ID_LENGTH = 255 - 90;

  private static final Logger LOG = Logger.getLogger(SchedulerThriftInterface.class.getName());

  private static final Function<IScheduledTask, String> GET_ROLE = Functions.compose(
      new Function<ITaskConfig, String>() {
        @Override
        public String apply(ITaskConfig task) {
          return task.getJob().getRole();
        }
      },
      Tasks.SCHEDULED_TO_INFO);

  private final NonVolatileStorage storage;
  private final LockManager lockManager;
  private final CapabilityValidator sessionValidator;
  private final StorageBackup backup;
  private final Recovery recovery;
  private final MaintenanceController maintenance;
  private final CronJobManager cronJobManager;
  private final QuotaManager quotaManager;
  private final StateManager stateManager;
  private final TaskIdGenerator taskIdGenerator;
  private final UUIDGenerator uuidGenerator;
  private final JobUpdateController jobUpdateController;
  private final boolean isUpdaterEnabled;
  private final ReadOnlyScheduler.Iface readOnlyScheduler;

  @Qualifier
  @Target({FIELD, PARAMETER, METHOD})
  @Retention(RUNTIME)
  @interface EnableUpdater { }

  @Inject
  SchedulerThriftInterface(
      NonVolatileStorage storage,
      LockManager lockManager,
      CapabilityValidator sessionValidator,
      StorageBackup backup,
      Recovery recovery,
      CronJobManager cronJobManager,
      MaintenanceController maintenance,
      QuotaManager quotaManager,
      StateManager stateManager,
      TaskIdGenerator taskIdGenerator,
      UUIDGenerator uuidGenerator,
      JobUpdateController jobUpdateController,
      @EnableUpdater boolean isUpdaterEnabled,
      ReadOnlyScheduler.Iface readOnlyScheduler) {

    this.storage = requireNonNull(storage);
    this.lockManager = requireNonNull(lockManager);
    this.sessionValidator = requireNonNull(sessionValidator);
    this.backup = requireNonNull(backup);
    this.recovery = requireNonNull(recovery);
    this.maintenance = requireNonNull(maintenance);
    this.cronJobManager = requireNonNull(cronJobManager);
    this.quotaManager = requireNonNull(quotaManager);
    this.stateManager = requireNonNull(stateManager);
    this.taskIdGenerator = requireNonNull(taskIdGenerator);
    this.uuidGenerator = requireNonNull(uuidGenerator);
    this.jobUpdateController = requireNonNull(jobUpdateController);
    this.isUpdaterEnabled = isUpdaterEnabled;
    this.readOnlyScheduler = requireNonNull(readOnlyScheduler);
  }

  @Override
  public Response createJob(
      JobConfiguration mutableJob,
      @Nullable final Lock mutableLock,
      SessionKey session) {

    requireNonNull(session);

    final SanitizedConfiguration sanitized;
    try {
      sessionValidator.checkAuthenticated(
          session,
          ImmutableSet.of(mutableJob.getKey().getRole()));
      sanitized = SanitizedConfiguration.fromUnsanitized(IJobConfiguration.build(mutableJob));
    } catch (AuthFailedException e) {
      return error(AUTH_FAILED, e);
    } catch (TaskDescriptionException e) {
      return error(INVALID_REQUEST, e);
    }

    if (sanitized.isCron()) {
      return invalidRequest(NO_CRON);
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        final IJobConfiguration job = sanitized.getJobConfig();

        try {
          lockManager.validateIfLocked(
              ILockKey.build(LockKey.job(job.getKey().newBuilder())),
              Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

          checkJobExists(storeProvider, job.getKey());

          ITaskConfig template = sanitized.getJobConfig().getTaskConfig();
          int count = sanitized.getJobConfig().getInstanceCount();

          validateTaskLimits(template, count, quotaManager.checkInstanceAddition(template, count));

          LOG.info("Launching " + count + " tasks.");
          stateManager.insertPendingTasks(
              storeProvider,
              template,
              sanitized.getInstanceIds());

          return ok();
        } catch (LockException e) {
          return error(LOCK_ERROR, e);
        } catch (JobExistsException | TaskValidationException e) {
          return error(INVALID_REQUEST, e);
        }
      }
    });
  }

  private static class JobExistsException extends Exception {
    public JobExistsException(String message) {
      super(message);
    }
  }

  private void checkJobExists(StoreProvider store, IJobKey jobKey) throws JobExistsException {
    if (!store.getTaskStore().fetchTasks(Query.jobScoped(jobKey).active()).isEmpty()
        || getCronJob(store, jobKey).isPresent()) {

      throw new JobExistsException(jobAlreadyExistsMessage(jobKey));
    }
  }

  private Response createOrUpdateCronTemplate(
      JobConfiguration mutableJob,
      @Nullable final Lock mutableLock,
      SessionKey session,
      final boolean updateOnly) {

    IJobConfiguration job = IJobConfiguration.build(mutableJob);
    final IJobKey jobKey = JobKeys.assertValid(job.getKey());
    requireNonNull(session);

    final SanitizedConfiguration sanitized;
    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
      sanitized = SanitizedConfiguration.fromUnsanitized(job);
    } catch (AuthFailedException e) {
      return error(AUTH_FAILED, e);
    } catch (TaskDescriptionException e) {
      return error(INVALID_REQUEST, e);
    }

    if (!sanitized.isCron()) {
      return invalidRequest(noCronScheduleMessage(jobKey));
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        try {
          lockManager.validateIfLocked(
              ILockKey.build(LockKey.job(jobKey.newBuilder())),
              Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

          ITaskConfig template = sanitized.getJobConfig().getTaskConfig();
          int count = sanitized.getJobConfig().getInstanceCount();

          validateTaskLimits(template, count, quotaManager.checkInstanceAddition(template, count));

          // TODO(mchucarroll): Merge CronJobManager.createJob/updateJob
          if (updateOnly || getCronJob(storeProvider, jobKey).isPresent()) {
            // The job already has a schedule: so update it.
            cronJobManager.updateJob(SanitizedCronJob.from(sanitized));
          } else {
            checkJobExists(storeProvider, jobKey);
            cronJobManager.createJob(SanitizedCronJob.from(sanitized));
          }

          return ok();
        } catch (LockException e) {
          return error(LOCK_ERROR, e);
        } catch (JobExistsException | TaskValidationException | CronException e) {
          return error(INVALID_REQUEST, e);
        }
      }
    });
  }

  @Override
  public Response scheduleCronJob(
      JobConfiguration mutableJob,
      @Nullable Lock mutableLock,
      SessionKey session) {

    return createOrUpdateCronTemplate(mutableJob, mutableLock, session, false);
  }

  @Override
  public Response replaceCronTemplate(
      JobConfiguration mutableJob,
      @Nullable Lock mutableLock,
      SessionKey session) {

    return createOrUpdateCronTemplate(mutableJob, mutableLock, session, true);
  }

  @Override
  public Response descheduleCronJob(
      JobKey mutableJobKey,
      @Nullable Lock mutableLock,
      SessionKey session) {

    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(mutableJobKey.getRole()));

      IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));
      lockManager.validateIfLocked(
          ILockKey.build(LockKey.job(jobKey.newBuilder())),
          Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

      if (!cronJobManager.deleteJob(jobKey)) {
        return invalidRequest(notScheduledCronMessage(jobKey));
      }
      return ok();
    } catch (AuthFailedException e) {
      return error(AUTH_FAILED, e);
    } catch (LockException e) {
      return error(LOCK_ERROR, e);
    }
  }

  @Override
  public Response populateJobConfig(JobConfiguration description) throws TException {
    return readOnlyScheduler.populateJobConfig(description);
  }

  @Override
  public Response startCronJob(JobKey mutableJobKey, SessionKey session) {
    requireNonNull(session);
    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));

    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
    } catch (AuthFailedException e) {
      return error(AUTH_FAILED, e);
    }

    try {
      cronJobManager.startJobNow(jobKey);
      return ok();
    } catch (CronException e) {
      return invalidRequest("Failed to start cron job - " + e.getMessage());
    }
  }

  // TODO(William Farner): Provide status information about cron jobs here.
  @Override
  public Response getTasksStatus(TaskQuery query) throws TException {
    return readOnlyScheduler.getTasksStatus(query);
  }

  @Override
  public Response getTasksWithoutConfigs(TaskQuery query) throws TException {
    return readOnlyScheduler.getTasksWithoutConfigs(query);
  }

  @Override
  public Response getPendingReason(TaskQuery query) throws TException {

    // Only PENDING tasks should be considered.

    return readOnlyScheduler.getPendingReason(query);
  }

  @Override
  public Response getConfigSummary(JobKey job) throws TException {

    return readOnlyScheduler.getConfigSummary(job);
  }

  @Override
  public Response getRoleSummary() throws TException {
    return readOnlyScheduler.getRoleSummary();
  }

  @Override
  public Response getJobSummary(@Nullable String maybeNullRole) throws TException {
    return readOnlyScheduler.getJobSummary(maybeNullRole);
  }

  @Override
  public Response getJobs(@Nullable String maybeNullRole) throws TException {
    return readOnlyScheduler.getJobs(maybeNullRole);
  }

  private void validateLockForTasks(Optional<ILock> lock, Iterable<IScheduledTask> tasks)
      throws LockException {

    ImmutableSet<IJobKey> uniqueKeys = FluentIterable.from(tasks)
        .transform(Tasks.SCHEDULED_TO_JOB_KEY)
        .toSet();

    // Validate lock against every unique job key derived from the tasks.
    for (IJobKey key : uniqueKeys) {
      lockManager.validateIfLocked(ILockKey.build(LockKey.job(key.newBuilder())), lock);
    }
  }

  private static Query.Builder implicitKillQuery(TaskQuery mutableQuery) {
    Query.Builder query = Query.arbitrary(mutableQuery);
    // Unless statuses were specifically supplied, only attempt to kill active tasks.
    return query.get().isSetStatuses() ? query : query.byStatus(ACTIVE_STATES);
  }

  private SessionContext authenticateNonAdminKillingTasks(
      Query.Builder taskQuery,
      SessionKey session) throws AuthFailedException {

    // Authenticate the session against any affected roles, always including the role for a
    // role-scoped query.
    ImmutableSet.Builder<String> targetRoles = ImmutableSet.builder();
    Set<IJobKey> keys = JobKeys.from(taskQuery).or(ImmutableSet.<IJobKey>of());
    targetRoles.addAll(FluentIterable.from(keys).transform(JobKeys.TO_ROLE));

    if (taskQuery.get().isSetRole()) {
      targetRoles.add(taskQuery.get().getRole());
    }

    if (taskQuery.get().isSetTaskIds()) {
      // Note: this operation is weakly-consistent with respect to the transaction that performs
      // the kill.  This means the task could exit between authentication and kill.  Since the user
      // asked to kill a task ID (rather than an instance ID, which would outlast a task), this is
      // considered acceptable.
      targetRoles.addAll(
          FluentIterable.from(Storage.Util.fetchTasks(storage, taskQuery)).transform(GET_ROLE));
    }

    Set<String> explicitRoles = targetRoles.build();
    // Disallow non-admin users from killing tasks on behalf of arbitrary roles.  Since query fields
    // are AND-ed together, the presence of a role scope on the query is sufficient for a non-admin
    // user to perform the operation.
    if (explicitRoles.isEmpty()) {
      throw new AuthFailedException("Only an administrator may kill an arbitrary user's tasks.");
    } else {
      return sessionValidator.checkAuthenticated(session, explicitRoles);
    }
  }

  private Optional<SessionContext> isAdmin(SessionKey session) {
    try {
      return Optional.of(sessionValidator.checkAuthorized(session, ROOT, AuditCheck.REQUIRED));
    } catch (AuthFailedException e) {
      return Optional.absent();
    }
  }

  @Override
  public Response killTasks(TaskQuery mutableQuery, final Lock mutableLock, SessionKey session) {
    requireNonNull(mutableQuery);
    requireNonNull(session);

    if (mutableQuery.getJobName() != null && WHITESPACE.matchesAllOf(mutableQuery.getJobName())) {
      return invalidRequest(String.format("Invalid job name: '%s'", mutableQuery.getJobName()));
    }

    final Query.Builder query = implicitKillQuery(mutableQuery);
    Preconditions.checkState(
        !query.get().isSetOwner(),
        "The owner field in a query should have been unset by Query.Builder.");

    Optional<SessionContext> maybeAdminContext = isAdmin(session);
    final SessionContext context;
    if (maybeAdminContext.isPresent()) {
      LOG.info("Granting kill query to admin user: " + query);
      context = maybeAdminContext.get();
    } else {
      try {
        context = authenticateNonAdminKillingTasks(query, session);
      } catch (AuthFailedException e) {
        return error(AUTH_FAILED, e);
      }
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        Iterable<IScheduledTask> tasks = storeProvider.getTaskStore().fetchTasks(query);
        try {
          validateLockForTasks(
              Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER),
              tasks);
        } catch (LockException e) {
          return error(LOCK_ERROR, e);
        }

        LOG.info("Killing tasks matching " + query);

        boolean tasksKilled = false;
        for (String taskId : Tasks.ids(tasks)) {
          tasksKilled |= stateManager.changeState(
              storeProvider,
              taskId,
              Optional.<ScheduleStatus>absent(),
              ScheduleStatus.KILLING,
              killedByMessage(context.getIdentity()));
        }

        return tasksKilled
            ? ok()
            : addMessage(empty(), OK, NO_TASKS_TO_KILL_MESSAGE);
      }
    });
  }

  @Override
  public Response restartShards(
      JobKey mutableJobKey,
      final Set<Integer> shardIds,
      @Nullable final Lock mutableLock,
      SessionKey session) {

    final IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));
    checkNotBlank(shardIds);
    requireNonNull(session);

    final SessionContext context;
    try {
      context = sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
    } catch (AuthFailedException e) {
      return error(AUTH_FAILED, e);
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        try {
          lockManager.validateIfLocked(
              ILockKey.build(LockKey.job(jobKey.newBuilder())),
              Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));
        } catch (LockException e) {
          return error(LOCK_ERROR, e);
        }

        Query.Builder query = Query.instanceScoped(jobKey, shardIds).active();
        final Set<IScheduledTask> matchingTasks = storeProvider.getTaskStore().fetchTasks(query);
        if (matchingTasks.size() != shardIds.size()) {
          return invalidRequest("Not all requested shards are active.");
        }

        LOG.info("Restarting shards matching " + query);
        storage.write(new MutateWork.NoResult.Quiet() {
          @Override
          protected void execute(MutableStoreProvider storeProvider) {
            for (String taskId : Tasks.ids(matchingTasks)) {
              stateManager.changeState(
                  storeProvider,
                  taskId,
                  Optional.<ScheduleStatus>absent(),
                  ScheduleStatus.RESTARTING,
                  restartedByMessage(context.getIdentity()));
            }
          }
        });
        return ok();
      }
    });
  }

  @Override
  public Response getQuota(String ownerRole) throws TException {
    return readOnlyScheduler.getQuota(ownerRole);
  }

  @Requires(whitelist = PROVISIONER)
  @Override
  public Response setQuota(
      final String ownerRole,
      final ResourceAggregate resourceAggregate,
      SessionKey session) {

    checkNotBlank(ownerRole);
    requireNonNull(resourceAggregate);
    requireNonNull(session);

    try {
      quotaManager.saveQuota(ownerRole, IResourceAggregate.build(resourceAggregate));
      return ok();
    } catch (QuotaException e) {
      return error(INVALID_REQUEST, e);
    }
  }

  @Requires(whitelist = MACHINE_MAINTAINER)
  @Override
  public Response startMaintenance(Hosts hosts, SessionKey session) {
    return ok(Result.startMaintenanceResult(
        new StartMaintenanceResult()
            .setStatuses(maintenance.startMaintenance(hosts.getHostNames()))));
  }

  @Requires(whitelist = MACHINE_MAINTAINER)
  @Override
  public Response drainHosts(Hosts hosts, SessionKey session) {
    return ok(Result.drainHostsResult(
        new DrainHostsResult().setStatuses(maintenance.drain(hosts.getHostNames()))));
  }

  @Requires(whitelist = MACHINE_MAINTAINER)
  @Override
  public Response maintenanceStatus(Hosts hosts, SessionKey session) {
    return ok(Result.maintenanceStatusResult(
        new MaintenanceStatusResult().setStatuses(maintenance.getStatus(hosts.getHostNames()))));
  }

  @Requires(whitelist = MACHINE_MAINTAINER)
  @Override
  public Response endMaintenance(Hosts hosts, SessionKey session) {
    return ok(Result.endMaintenanceResult(
        new EndMaintenanceResult()
            .setStatuses(maintenance.endMaintenance(hosts.getHostNames()))));
  }

  @Override
  public Response forceTaskState(
      final String taskId,
      final ScheduleStatus status,
      SessionKey session) {

    checkNotBlank(taskId);
    requireNonNull(status);
    requireNonNull(session);

    final SessionContext context;
    try {
      // TODO(maxim): Remove this after AOP-style session validation passes in a SessionContext.
      context = sessionValidator.checkAuthorized(session, ROOT, AuditCheck.REQUIRED);
    } catch (AuthFailedException e) {
      return error(AUTH_FAILED, e);
    }

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider storeProvider) {
        stateManager.changeState(
            storeProvider,
            taskId,
            Optional.<ScheduleStatus>absent(),
            status,
            transitionMessage(context.getIdentity()));
      }
    });

    return ok();
  }

  @Override
  public Response performBackup(SessionKey session) {
    backup.backupNow();
    return ok();
  }

  @Override
  public Response listBackups(SessionKey session) {
    return ok(Result.listBackupsResult(new ListBackupsResult()
        .setBackups(recovery.listBackups())));
  }

  @Override
  public Response stageRecovery(String backupId, SessionKey session) {
    recovery.stage(backupId);
    return ok();
  }

  @Override
  public Response queryRecovery(TaskQuery query, SessionKey session) {
    return ok(Result.queryRecoveryResult(new QueryRecoveryResult()
        .setTasks(IScheduledTask.toBuildersSet(recovery.query(Query.arbitrary(query))))));
  }

  @Override
  public Response deleteRecoveryTasks(TaskQuery query, SessionKey session) {
    recovery.deleteTasks(Query.arbitrary(query));
    return ok();
  }

  @Override
  public Response commitRecovery(SessionKey session) {
    recovery.commit();
    return ok();
  }

  @Override
  public Response unloadRecovery(SessionKey session) {
    recovery.unload();
    return ok();
  }

  @Override
  public Response snapshot(SessionKey session) {
    storage.snapshot();
    return ok();
  }

  @Override
  public Response rewriteConfigs(final RewriteConfigsRequest request, SessionKey session) {
    if (request.getRewriteCommandsSize() == 0) {
      return addMessage(Responses.empty(), INVALID_REQUEST, "No rewrite commands provided.");
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        List<String> errors = Lists.newArrayList();

        for (ConfigRewrite command : request.getRewriteCommands()) {
          Optional<String> error = rewriteConfig(command, storeProvider);
          if (error.isPresent()) {
            errors.add(error.get());
          }
        }

        Response resp = empty();
        if (errors.isEmpty()) {
          resp.setResponseCode(OK);
        } else {
          for (String error : errors) {
            addMessage(resp, WARNING, error);
          }
        }
        return resp;
      }
    });
  }

  private Optional<String> rewriteJob(JobConfigRewrite jobRewrite, JobStore.Mutable jobStore) {
    IJobConfiguration existingJob = IJobConfiguration.build(jobRewrite.getOldJob());
    IJobConfiguration rewrittenJob;
    Optional<String> error = Optional.absent();
    try {
      rewrittenJob = ConfigurationManager.validateAndPopulate(
          IJobConfiguration.build(jobRewrite.getRewrittenJob()));
    } catch (TaskDescriptionException e) {
      // We could add an error here, but this is probably a hint of something wrong in
      // the client that's causing a bad configuration to be applied.
      throw new RuntimeException(e);
    }

    if (existingJob.getKey().equals(rewrittenJob.getKey())) {
      Optional<IJobConfiguration> job = jobStore.fetchJob(existingJob.getKey());
      if (job.isPresent()) {
        IJobConfiguration storedJob = job.get();
        if (storedJob.equals(existingJob)) {
          jobStore.saveAcceptedJob(rewrittenJob);
        } else {
          error = Optional.of(
              "CAS compare failed for " + JobKeys.canonicalString(storedJob.getKey()));
        }
      } else {
        error = Optional.of(
            "No jobs found for key " + JobKeys.canonicalString(existingJob.getKey()));
      }
    } else {
      error = Optional.of("Disallowing rewrite attempting to change job key.");
    }

    return error;
  }

  private Optional<String> rewriteInstance(
      InstanceConfigRewrite instanceRewrite,
      MutableStoreProvider storeProvider) {

    InstanceKey instanceKey = instanceRewrite.getInstanceKey();
    Optional<String> error = Optional.absent();
    Iterable<IScheduledTask> tasks = storeProvider.getTaskStore().fetchTasks(
        Query.instanceScoped(IJobKey.build(instanceKey.getJobKey()),
            instanceKey.getInstanceId())
            .active());
    Optional<IAssignedTask> task =
        Optional.fromNullable(Iterables.getOnlyElement(tasks, null))
            .transform(Tasks.SCHEDULED_TO_ASSIGNED);

    if (task.isPresent()) {
      if (task.get().getTask().newBuilder().equals(instanceRewrite.getOldTask())) {
        ITaskConfig newConfiguration = ITaskConfig.build(
            ConfigurationManager.applyDefaultsIfUnset(instanceRewrite.getRewrittenTask()));
        boolean changed = storeProvider.getUnsafeTaskStore().unsafeModifyInPlace(
            task.get().getTaskId(), newConfiguration);
        if (!changed) {
          error = Optional.of("Did not change " + task.get().getTaskId());
        }
      } else {
        error = Optional.of("CAS compare failed for " + instanceKey);
      }
    } else {
      error = Optional.of("No active task found for " + instanceKey);
    }

    return error;
  }

  private Optional<String> rewriteConfig(
      ConfigRewrite command,
      MutableStoreProvider storeProvider) {

    Optional<String> error;
    switch (command.getSetField()) {
      case JOB_REWRITE:
        error = rewriteJob(command.getJobRewrite(), storeProvider.getJobStore());
        break;

      case INSTANCE_REWRITE:
        error = rewriteInstance(command.getInstanceRewrite(), storeProvider);
        break;

      default:
        throw new IllegalArgumentException("Unhandled command type " + command.getSetField());
    }

    return error;
  }

  @Override
  public Response addInstances(
      final AddInstancesConfig config,
      @Nullable final Lock mutableLock,
      final SessionKey session) {

    requireNonNull(config);
    requireNonNull(session);
    checkNotBlank(config.getInstanceIds());
    final IJobKey jobKey = JobKeys.assertValid(IJobKey.build(config.getKey()));

    final ITaskConfig task;
    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
      task = ConfigurationManager.validateAndPopulate(
          ITaskConfig.build(config.getTaskConfig()));
    } catch (AuthFailedException e) {
      return error(AUTH_FAILED, e);
    } catch (TaskDescriptionException e) {
      return error(INVALID_REQUEST, e);
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        try {
          if (getCronJob(storeProvider, jobKey).isPresent()) {
            return invalidRequest("Instances may not be added to cron jobs.");
          }

          lockManager.validateIfLocked(
              ILockKey.build(LockKey.job(jobKey.newBuilder())),
              Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

          ImmutableSet<IScheduledTask> currentTasks = storeProvider.getTaskStore().fetchTasks(
              Query.jobScoped(task.getJob()).active());

          validateTaskLimits(
              task,
              currentTasks.size() + config.getInstanceIdsSize(),
              quotaManager.checkInstanceAddition(task, config.getInstanceIdsSize()));

          storage.write(new NoResult.Quiet() {
            @Override
            protected void execute(MutableStoreProvider storeProvider) {
              stateManager.insertPendingTasks(
                  storeProvider,
                  task,
                  ImmutableSet.copyOf(config.getInstanceIds()));
            }
          });

          return ok();
        } catch (LockException e) {
          return error(LOCK_ERROR, e);
        } catch (TaskValidationException | IllegalArgumentException e) {
          return error(INVALID_REQUEST, e);
        }
      }
    });
  }

  public Optional<IJobConfiguration> getCronJob(StoreProvider storeProvider, final IJobKey jobKey) {
    requireNonNull(jobKey);
    return storeProvider.getJobStore().fetchJob(jobKey);
  }

  private String getRoleFromLockKey(ILockKey lockKey) {
    if (lockKey.getSetField() == _Fields.JOB) {
      JobKeys.assertValid(lockKey.getJob());
      return lockKey.getJob().getRole();
    } else {
      throw new IllegalArgumentException("Unhandled LockKey: " + lockKey.getSetField());
    }
  }

  @Override
  public Response acquireLock(LockKey mutableLockKey, SessionKey session) {
    requireNonNull(mutableLockKey);
    requireNonNull(session);

    ILockKey lockKey = ILockKey.build(mutableLockKey);

    try {
      SessionContext context = sessionValidator.checkAuthenticated(
          session,
          ImmutableSet.of(getRoleFromLockKey(lockKey)));

      ILock lock = lockManager.acquireLock(lockKey, context.getIdentity());
      return ok(Result.acquireLockResult(
          new AcquireLockResult().setLock(lock.newBuilder())));
    } catch (AuthFailedException e) {
      return error(AUTH_FAILED, e);
    } catch (LockException e) {
      return error(LOCK_ERROR, e);
    }
  }

  @Override
  public Response releaseLock(Lock mutableLock, LockValidation validation, SessionKey session) {
    requireNonNull(mutableLock);
    requireNonNull(validation);
    requireNonNull(session);

    ILock lock = ILock.build(mutableLock);

    try {
      sessionValidator.checkAuthenticated(
          session,
          ImmutableSet.of(getRoleFromLockKey(lock.getKey())));

      if (validation == LockValidation.CHECKED) {
        lockManager.validateIfLocked(lock.getKey(), Optional.of(lock));
      }
      lockManager.releaseLock(lock);
      return ok();
    } catch (AuthFailedException e) {
      return error(AUTH_FAILED, e);
    } catch (LockException e) {
      return error(LOCK_ERROR, e);
    }
  }

  @Override
  public Response getLocks() throws TException {
    return readOnlyScheduler.getLocks();
  }

  private static class TaskValidationException extends Exception {
    public TaskValidationException(String message) {
      super(message);
    }
  }

  private void validateTaskLimits(
      ITaskConfig task,
      int totalInstances,
      QuotaCheckResult quotaCheck) throws TaskValidationException {

    if (totalInstances <= 0 || totalInstances > MAX_TASKS_PER_JOB.get()) {
      throw new TaskValidationException(String.format(
          "Instance count must be between 1 and %d inclusive.",
          MAX_TASKS_PER_JOB.get()));
    }

    // TODO(maximk): This is a short-term hack to stop the bleeding from
    //               https://issues.apache.org/jira/browse/MESOS-691
    if (taskIdGenerator.generate(task, totalInstances).length() > MAX_TASK_ID_LENGTH) {
      throw new TaskValidationException(
          "Task ID is too long, please shorten your role or job name.");
    }

    if (quotaCheck.getResult() == INSUFFICIENT_QUOTA) {
      throw new TaskValidationException("Insufficient resource quota: "
          + quotaCheck.getDetails().or(""));
    }
  }

  private static final Function<Collection<Integer>, Set<Range<Integer>>> TO_RANGES =
      new Function<Collection<Integer>, Set<Range<Integer>>>() {
        @Override
        public Set<Range<Integer>> apply(Collection<Integer> numbers) {
          return Numbers.toRanges(numbers);
        }
      };

  private static final Function<Range<Integer>, org.apache.aurora.gen.Range> TO_THRIFT_RANGE =
      new Function<Range<Integer>, org.apache.aurora.gen.Range>() {
        @Override
        public org.apache.aurora.gen.Range apply(Range<Integer> range) {
          return new org.apache.aurora.gen.Range(range.lowerEndpoint(), range.upperEndpoint());
        }
      };

  private static Set<org.apache.aurora.gen.Range> convertRanges(Set<Range<Integer>> ranges) {
    return FluentIterable.from(ranges)
        .transform(TO_THRIFT_RANGE)
        .toSet();
  }

  private static Set<InstanceTaskConfig> buildInitialState(Map<Integer, ITaskConfig> tasks) {
    // Translate tasks into instance IDs.
    Multimap<ITaskConfig, Integer> instancesByConfig = HashMultimap.create();
    Multimaps.invertFrom(Multimaps.forMap(tasks), instancesByConfig);

    // Reduce instance IDs into contiguous ranges.
    Map<ITaskConfig, Set<Range<Integer>>> rangesByConfig =
        Maps.transformValues(instancesByConfig.asMap(), TO_RANGES);

    ImmutableSet.Builder<InstanceTaskConfig> builder = ImmutableSet.builder();
    for (Map.Entry<ITaskConfig, Set<Range<Integer>>> entry : rangesByConfig.entrySet()) {
      builder.add(new InstanceTaskConfig()
          .setTask(entry.getKey().newBuilder())
          .setInstances(convertRanges(entry.getValue())));
    }

    return builder.build();
  }

  @Override
  public Response startJobUpdate(JobUpdateRequest mutableRequest, SessionKey session) {
    if (!isUpdaterEnabled) {
      return invalidRequest("Server-side updates are disabled on this cluster.");
    }

    requireNonNull(mutableRequest);
    requireNonNull(session);

    // TODO(maxim): Switch to key field instead when AURORA-749 is fixed.
    final IJobKey job = JobKeys.assertValid(IJobKey.build(new JobKey()
        .setRole(mutableRequest.getTaskConfig().getOwner().getRole())
        .setEnvironment(mutableRequest.getTaskConfig().getEnvironment())
        .setName(mutableRequest.getTaskConfig().getJobName())));

    if (!mutableRequest.getTaskConfig().isIsService()) {
      return invalidRequest(NON_SERVICE_TASK);
    }

    JobUpdateSettings settings = requireNonNull(mutableRequest.getSettings());
    if (settings.getUpdateGroupSize() <= 0) {
      return invalidRequest(INVALID_GROUP_SIZE);
    }

    if (settings.getMaxPerInstanceFailures() < 0) {
      return invalidRequest(INVALID_MAX_INSTANCE_FAILURES);
    }

    if (settings.getMaxFailedInstances() < 0) {
      return invalidRequest(INVALID_MAX_FAILED_INSTANCES);
    }

    if (settings.getMaxWaitToInstanceRunningMs() < 0) {
      return invalidRequest(INVALID_MAX_WAIT_TO_RUNNING);
    }

    if (settings.getMinWaitInInstanceRunningMs() < 0) {
      return invalidRequest(INVALID_MIN_WAIT_TO_RUNNING);
    }

    if (settings.isSetBlockIfNoPulsesAfterMs() && settings.getBlockIfNoPulsesAfterMs() <= 0) {
      return invalidRequest(INVALID_PULSE_TIMEOUT);
    }

    final SessionContext context;
    final IJobUpdateRequest request;
    try {
      context = sessionValidator.checkAuthenticated(session, ImmutableSet.of(job.getRole()));
      request = IJobUpdateRequest.build(new JobUpdateRequest(mutableRequest).setTaskConfig(
          ConfigurationManager.validateAndPopulate(
              ITaskConfig.build(mutableRequest.getTaskConfig())).newBuilder()));

    } catch (AuthFailedException e) {
      return error(AUTH_FAILED, e);
    } catch (TaskDescriptionException e) {
      return error(INVALID_REQUEST, e);
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        if (getCronJob(storeProvider, job).isPresent()) {
          return invalidRequest(NO_CRON);
        }

        String updateId = uuidGenerator.createNew().toString();
        IJobUpdateSettings settings = request.getSettings();

        JobDiff diff = JobDiff.compute(
            storeProvider.getTaskStore(),
            job,
            JobDiff.asMap(request.getTaskConfig(), request.getInstanceCount()),
            settings.getUpdateOnlyTheseInstances());

        Set<Integer> invalidScope = diff.getOutOfScopeInstances(
            Numbers.rangesToInstanceIds(settings.getUpdateOnlyTheseInstances()));
        if (!invalidScope.isEmpty()) {
          return invalidRequest(
              "updateOnlyTheseInstances contains instances irrelevant to the update: "
                  + invalidScope);
        }

        if (diff.isNoop()) {
          return addMessage(empty(), OK, NOOP_JOB_UPDATE_MESSAGE);
        }

        JobUpdateInstructions instructions = new JobUpdateInstructions()
            .setSettings(settings.newBuilder())
            .setInitialState(buildInitialState(diff.getReplacedInstances()));
        if (!diff.getReplacementInstances().isEmpty()) {
          instructions.setDesiredState(
              new InstanceTaskConfig()
                  .setTask(request.getTaskConfig().newBuilder())
                  .setInstances(convertRanges(Numbers.toRanges(diff.getReplacementInstances()))));
        }

        IJobUpdate update = IJobUpdate.build(new JobUpdate()
            .setSummary(new JobUpdateSummary()
                .setJobKey(job.newBuilder())
                .setUpdateId(updateId)
                .setUser(context.getIdentity()))
            .setInstructions(instructions));
        try {
          validateTaskLimits(
              request.getTaskConfig(),
              request.getInstanceCount(),
              quotaManager.checkJobUpdate(update));

          jobUpdateController.start(update, context.getIdentity());
          return ok(Result.startJobUpdateResult(new StartJobUpdateResult(updateId)));
        } catch (UpdateStateException | TaskValidationException e) {
          return error(INVALID_REQUEST, e);
        }
      }
    });
  }

  private Response changeJobUpdateState(
      JobKey mutableJobKey,
      SessionKey session,
      final JobUpdateStateChange change) {

    final IJobKey jobKey = JobKeys.assertValid(IJobKey.build(requireNonNull(mutableJobKey)));
    final SessionContext context;
    try {
      // TODO(maxim): pass a JobUpdateKey here instead of generating a fake one. AURORA-1093.
      IJobUpdateKey updateKey = IJobUpdateKey.build(new JobUpdateKey().setJob(mutableJobKey));
      context = authorizeJobUpdateAction(updateKey, session);
    } catch (AuthFailedException e) {
      return error(AUTH_FAILED, e);
    }
    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        try {
          change.modifyUpdate(jobUpdateController, jobKey, context.getIdentity());
          return ok();
        } catch (UpdateStateException e) {
          return error(INVALID_REQUEST, e);
        }
      }
    });
  }

  private interface JobUpdateStateChange {
    void modifyUpdate(JobUpdateController controller, IJobKey job, String invokingUser)
        throws UpdateStateException;
  }

  private static final JobUpdateStateChange PAUSE = new JobUpdateStateChange() {
    @Override
    public void modifyUpdate(JobUpdateController controller, IJobKey job, String invokingUser)
        throws UpdateStateException {

      controller.pause(job, invokingUser);
    }
  };

  private static final JobUpdateStateChange RESUME = new JobUpdateStateChange() {
    @Override
    public void modifyUpdate(JobUpdateController controller, IJobKey job, String invokingUser)
        throws UpdateStateException {

      controller.resume(job, invokingUser);
    }
  };

  private static final JobUpdateStateChange ABORT = new JobUpdateStateChange() {
    @Override
    public void modifyUpdate(JobUpdateController controller, IJobKey job, String invokingUser)
        throws UpdateStateException {

      controller.abort(job, invokingUser);
    }
  };

  @Override
  public Response pauseJobUpdate(JobKey mutableJobKey, SessionKey session) {
    return changeJobUpdateState(mutableJobKey, session, PAUSE);
  }

  @Override
  public Response resumeJobUpdate(final JobKey mutableJobKey, final SessionKey session) {
    return changeJobUpdateState(mutableJobKey, session, RESUME);
  }

  @Override
  public Response abortJobUpdate(final JobKey mutableJobKey, final SessionKey session) {
    return changeJobUpdateState(mutableJobKey, session, ABORT);
  }

  @Override
  public Response pulseJobUpdate(JobUpdateKey mutableUpdateKey, final SessionKey session) {
    IJobUpdateKey updateKey = validateJobUpdateKey(mutableUpdateKey);
    try {
      authorizeJobUpdateAction(updateKey, session);
      JobUpdatePulseStatus result = jobUpdateController.pulse(updateKey);
      return ok(Result.pulseJobUpdateResult(new PulseJobUpdateResult(result)));
    } catch (AuthFailedException e) {
      return error(AUTH_FAILED, e);
    } catch (UpdateStateException e) {
      return error(INVALID_REQUEST, e);
    }
  }

  @Override
  public Response getJobUpdateSummaries(JobUpdateQuery mutableQuery) throws TException {
    return readOnlyScheduler.getJobUpdateSummaries(mutableQuery);
  }

  @Override
  public Response getJobUpdateDetails(String updateId) throws TException {
    return readOnlyScheduler.getJobUpdateDetails(updateId);
  }

  private Optional<SessionContext> isUpdateCoordinator(SessionKey session) {
    try {
      return Optional.of(
          sessionValidator.checkAuthorized(session, UPDATE_COORDINATOR, AuditCheck.NONE));
    } catch (AuthFailedException e) {
      return Optional.absent();
    }
  }

  private SessionContext authorizeJobUpdateAction(IJobUpdateKey key, SessionKey session)
      throws AuthFailedException {

    Optional<SessionContext> maybeCoordinatorContext = isUpdateCoordinator(session);
    SessionContext context;
    if (maybeCoordinatorContext.isPresent()) {
      context = maybeCoordinatorContext.get();
    } else {
      context = sessionValidator.checkAuthenticated(
          session,
          ImmutableSet.of(key.getJob().getRole()));
    }

    return context;
  }

  private static IJobUpdateKey validateJobUpdateKey(JobUpdateKey mutableKey) {
    IJobUpdateKey key = IJobUpdateKey.build(mutableKey);
    JobKeys.assertValid(key.getJob());
    checkNotBlank(key.getId());
    return key;
  }

  @VisibleForTesting
  static Optional<String> transitionMessage(String user) {
    return Optional.of("Transition forced by " + user);
  }

  @VisibleForTesting
  static Optional<String> killedByMessage(String user) {
    return Optional.of("Killed by " + user);
  }

  @VisibleForTesting
  static Optional<String> restartedByMessage(String user) {
    return Optional.of("Restarted by " + user);
  }

  @VisibleForTesting
  static String noCronScheduleMessage(IJobKey jobKey) {
    return String.format("Job %s has no cron schedule", JobKeys.canonicalString(jobKey));
  }

  @VisibleForTesting
  static String notScheduledCronMessage(IJobKey jobKey) {
    return String.format("Job %s is not scheduled with cron", JobKeys.canonicalString(jobKey));
  }

  @VisibleForTesting
  static String jobAlreadyExistsMessage(IJobKey jobKey) {
    return String.format("Job %s already exists", JobKeys.canonicalString(jobKey));
  }

  @VisibleForTesting
  static final String NO_TASKS_TO_KILL_MESSAGE = "No tasks to kill.";

  @VisibleForTesting
  static final String NOOP_JOB_UPDATE_MESSAGE = "Job is unchanged by proposed update.";

  @VisibleForTesting
  static final String NO_CRON = "Cron jobs may only be created/updated by calling scheduleCronJob.";

  @VisibleForTesting
  static final String NON_SERVICE_TASK = "Updates are not supported for non-service tasks.";

  @VisibleForTesting
  static final String INVALID_GROUP_SIZE = "updateGroupSize must be positive.";

  @VisibleForTesting
  static final String INVALID_MAX_FAILED_INSTANCES = "maxFailedInstances must be non-negative.";

  @VisibleForTesting
  static final String INVALID_MAX_INSTANCE_FAILURES =
      "maxPerInstanceFailures must be non-negative.";

  @VisibleForTesting
  static final String INVALID_MAX_WAIT_TO_RUNNING =
      "maxWaitToInstanceRunningMs must be non-negative.";

  @VisibleForTesting
  static final String INVALID_MIN_WAIT_TO_RUNNING =
      "minWaitInInstanceRunningMs must be non-negative.";

  @VisibleForTesting
  static final String INVALID_PULSE_TIMEOUT = "blockIfNoPulsesAfterMs must be positive.";
}
