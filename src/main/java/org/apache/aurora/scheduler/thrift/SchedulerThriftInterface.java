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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
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

import org.apache.aurora.gen.AcquireLockResult;
import org.apache.aurora.gen.AddInstancesConfig;
import org.apache.aurora.gen.ConfigRewrite;
import org.apache.aurora.gen.DrainHostsResult;
import org.apache.aurora.gen.EndMaintenanceResult;
import org.apache.aurora.gen.Hosts;
import org.apache.aurora.gen.InstanceTaskConfig;
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
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.UUIDGenerator;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IConfigRewrite;
import org.apache.aurora.scheduler.storage.entities.IInstanceConfigRewrite;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IJobConfigRewrite;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateRequest;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSettings;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.entities.IRange;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.thrift.aop.AnnotatedAuroraAdmin;
import org.apache.aurora.scheduler.thrift.auth.DecoratedThrift;
import org.apache.aurora.scheduler.updater.JobDiff;
import org.apache.aurora.scheduler.updater.JobUpdateController;
import org.apache.aurora.scheduler.updater.JobUpdateController.AuditData;
import org.apache.aurora.scheduler.updater.UpdateStateException;
import org.apache.thrift.TException;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.CharMatcher.WHITESPACE;

import static org.apache.aurora.common.base.MorePreconditions.checkNotBlank;
import static org.apache.aurora.gen.ResponseCode.INVALID_REQUEST;
import static org.apache.aurora.gen.ResponseCode.LOCK_ERROR;
import static org.apache.aurora.gen.ResponseCode.OK;
import static org.apache.aurora.gen.ResponseCode.WARNING;
import static org.apache.aurora.scheduler.base.Numbers.convertRanges;
import static org.apache.aurora.scheduler.base.Numbers.toRanges;
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
class SchedulerThriftInterface implements AnnotatedAuroraAdmin {

  // This number is derived from the maximum file name length limit on most UNIX systems, less
  // the number of characters we've observed being added by mesos for the executor ID, prefix, and
  // delimiters.
  @VisibleForTesting
  static final int MAX_TASK_ID_LENGTH = 255 - 90;

  private static final Logger LOG = Logger.getLogger(SchedulerThriftInterface.class.getName());

  private final ConfigurationManager configurationManager;
  private final Thresholds thresholds;
  private final NonVolatileStorage storage;
  private final LockManager lockManager;
  private final StorageBackup backup;
  private final Recovery recovery;
  private final MaintenanceController maintenance;
  private final CronJobManager cronJobManager;
  private final QuotaManager quotaManager;
  private final StateManager stateManager;
  private final TaskIdGenerator taskIdGenerator;
  private final UUIDGenerator uuidGenerator;
  private final JobUpdateController jobUpdateController;
  private final ReadOnlyScheduler.Iface readOnlyScheduler;
  private final AuditMessages auditMessages;

  @Inject
  SchedulerThriftInterface(
      ConfigurationManager configurationManager,
      Thresholds thresholds,
      NonVolatileStorage storage,
      LockManager lockManager,
      StorageBackup backup,
      Recovery recovery,
      CronJobManager cronJobManager,
      MaintenanceController maintenance,
      QuotaManager quotaManager,
      StateManager stateManager,
      TaskIdGenerator taskIdGenerator,
      UUIDGenerator uuidGenerator,
      JobUpdateController jobUpdateController,
      ReadOnlyScheduler.Iface readOnlyScheduler,
      AuditMessages auditMessages) {

    this.configurationManager = requireNonNull(configurationManager);
    this.thresholds = requireNonNull(thresholds);
    this.storage = requireNonNull(storage);
    this.lockManager = requireNonNull(lockManager);
    this.backup = requireNonNull(backup);
    this.recovery = requireNonNull(recovery);
    this.maintenance = requireNonNull(maintenance);
    this.cronJobManager = requireNonNull(cronJobManager);
    this.quotaManager = requireNonNull(quotaManager);
    this.stateManager = requireNonNull(stateManager);
    this.taskIdGenerator = requireNonNull(taskIdGenerator);
    this.uuidGenerator = requireNonNull(uuidGenerator);
    this.jobUpdateController = requireNonNull(jobUpdateController);
    this.readOnlyScheduler = requireNonNull(readOnlyScheduler);
    this.auditMessages = requireNonNull(auditMessages);
  }

  @Override
  public Response createJob(JobConfiguration mutableJob, @Nullable Lock mutableLock) {
    SanitizedConfiguration sanitized;
    try {
      sanitized = SanitizedConfiguration.fromUnsanitized(
          configurationManager,
          IJobConfiguration.build(mutableJob));
    } catch (TaskDescriptionException e) {
      return error(INVALID_REQUEST, e);
    }

    if (sanitized.isCron()) {
      return invalidRequest(NO_CRON);
    }

    return storage.write(storeProvider -> {
      IJobConfiguration job = sanitized.getJobConfig();

      try {
        lockManager.validateIfLocked(
            ILockKey.build(LockKey.job(job.getKey().newBuilder())),
            java.util.Optional.ofNullable(mutableLock).map(ILock::build));

        checkJobExists(storeProvider, job.getKey());

        ITaskConfig template = sanitized.getJobConfig().getTaskConfig();
        int count = sanitized.getJobConfig().getInstanceCount();

        validateTaskLimits(
            template,
            count,
            quotaManager.checkInstanceAddition(template, count, storeProvider));

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
    });
  }

  private static class JobExistsException extends Exception {
    JobExistsException(String message) {
      super(message);
    }
  }

  private void checkJobExists(StoreProvider store, IJobKey jobKey) throws JobExistsException {
    if (!Iterables.isEmpty(store.getTaskStore().fetchTasks(Query.jobScoped(jobKey).active()))
        || getCronJob(store, jobKey).isPresent()) {

      throw new JobExistsException(jobAlreadyExistsMessage(jobKey));
    }
  }

  private Response createOrUpdateCronTemplate(
      JobConfiguration mutableJob,
      @Nullable Lock mutableLock,
      boolean updateOnly) {

    IJobConfiguration job = IJobConfiguration.build(mutableJob);
    IJobKey jobKey = JobKeys.assertValid(job.getKey());

    SanitizedConfiguration sanitized;
    try {
      sanitized = SanitizedConfiguration.fromUnsanitized(configurationManager, job);
    } catch (TaskDescriptionException e) {
      return error(INVALID_REQUEST, e);
    }

    if (!sanitized.isCron()) {
      return invalidRequest(noCronScheduleMessage(jobKey));
    }

    return storage.write(storeProvider -> {
      try {
        lockManager.validateIfLocked(
            ILockKey.build(LockKey.job(jobKey.newBuilder())),
            java.util.Optional.ofNullable(mutableLock).map(ILock::build));

        ITaskConfig template = sanitized.getJobConfig().getTaskConfig();
        int count = sanitized.getJobConfig().getInstanceCount();

        validateTaskLimits(
            template,
            count,
            quotaManager.checkCronUpdate(sanitized.getJobConfig(), storeProvider));

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
    });
  }

  @Override
  public Response scheduleCronJob(JobConfiguration mutableJob, @Nullable Lock mutableLock) {
    return createOrUpdateCronTemplate(mutableJob, mutableLock, false);
  }

  @Override
  public Response replaceCronTemplate(JobConfiguration mutableJob, @Nullable Lock mutableLock) {
    return createOrUpdateCronTemplate(mutableJob, mutableLock, true);
  }

  @Override
  public Response descheduleCronJob(JobKey mutableJobKey, @Nullable Lock mutableLock) {
    try {
      IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));
      lockManager.validateIfLocked(
          ILockKey.build(LockKey.job(jobKey.newBuilder())),
          java.util.Optional.ofNullable(mutableLock).map(ILock::build));

      if (!cronJobManager.deleteJob(jobKey)) {
        return invalidRequest(notScheduledCronMessage(jobKey));
      }
      return ok();
    } catch (LockException e) {
      return error(LOCK_ERROR, e);
    }
  }

  @Override
  public Response populateJobConfig(JobConfiguration description) throws TException {
    return readOnlyScheduler.populateJobConfig(description);
  }

  @Override
  public Response startCronJob(JobKey mutableJobKey) {
    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));

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

  @Override
  public Response getJobUpdateDiff(JobUpdateRequest request) throws TException {
    return readOnlyScheduler.getJobUpdateDiff(request);
  }

  private void validateLockForTasks(java.util.Optional<ILock> lock, Iterable<IScheduledTask> tasks)
      throws LockException {

    ImmutableSet<IJobKey> uniqueKeys = FluentIterable.from(tasks)
        .transform(Tasks::getJob)
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

  @Override
  public Response killTasks(TaskQuery mutableQuery, Lock mutableLock) {
    requireNonNull(mutableQuery);

    if (mutableQuery.getJobName() != null && WHITESPACE.matchesAllOf(mutableQuery.getJobName())) {
      return invalidRequest(String.format("Invalid job name: '%s'", mutableQuery.getJobName()));
    }

    Query.Builder query = implicitKillQuery(mutableQuery);
    Preconditions.checkState(
        !query.get().isSetOwner(),
        "The owner field in a query should have been unset by Query.Builder.");

    return storage.write(storeProvider -> {
      Iterable<IScheduledTask> tasks = storeProvider.getTaskStore().fetchTasks(query);
      try {
        validateLockForTasks(
            java.util.Optional.ofNullable(mutableLock).map(ILock::build),
            tasks);
      } catch (LockException e) {
        return error(LOCK_ERROR, e);
      }

      LOG.info("Killing tasks matching " + query);

      boolean tasksKilled = false;
      for (String taskId : Tasks.ids(tasks)) {
        tasksKilled |= StateChangeResult.SUCCESS == stateManager.changeState(
            storeProvider,
            taskId,
            Optional.absent(),
            ScheduleStatus.KILLING,
            auditMessages.killedByRemoteUser());
      }

      return tasksKilled
          ? ok()
          : addMessage(empty(), OK, NO_TASKS_TO_KILL_MESSAGE);
    });
  }

  @Override
  public Response restartShards(
      JobKey mutableJobKey,
      Set<Integer> shardIds,
      @Nullable Lock mutableLock) {

    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));
    checkNotBlank(shardIds);

    return storage.write(storeProvider -> {
      try {
        lockManager.validateIfLocked(
            ILockKey.build(LockKey.job(jobKey.newBuilder())),
            java.util.Optional.ofNullable(mutableLock).map(ILock::build));
      } catch (LockException e) {
        return error(LOCK_ERROR, e);
      }

      Query.Builder query = Query.instanceScoped(jobKey, shardIds).active();
      Iterable<IScheduledTask> matchingTasks = storeProvider.getTaskStore().fetchTasks(query);
      if (Iterables.size(matchingTasks) != shardIds.size()) {
        return invalidRequest("Not all requested shards are active.");
      }

      LOG.info("Restarting shards matching " + query);
      for (String taskId : Tasks.ids(matchingTasks)) {
        stateManager.changeState(
            storeProvider,
            taskId,
            Optional.absent(),
            ScheduleStatus.RESTARTING,
            auditMessages.restartedByRemoteUser());
      }
      return ok();
    });
  }

  @Override
  public Response getQuota(String ownerRole) throws TException {
    return readOnlyScheduler.getQuota(ownerRole);
  }

  @Override
  public Response setQuota(String ownerRole, ResourceAggregate resourceAggregate) {
    checkNotBlank(ownerRole);
    requireNonNull(resourceAggregate);

    try {
      storage.write((NoResult<QuotaException>) store -> quotaManager.saveQuota(
          ownerRole,
          IResourceAggregate.build(resourceAggregate),
          store));
      return ok();
    } catch (QuotaException e) {
      return error(INVALID_REQUEST, e);
    }
  }

  @Override
  public Response startMaintenance(Hosts hosts) {
    return ok(Result.startMaintenanceResult(
        new StartMaintenanceResult()
            .setStatuses(maintenance.startMaintenance(hosts.getHostNames()))));
  }

  @Override
  public Response drainHosts(Hosts hosts) {
    return ok(Result.drainHostsResult(
        new DrainHostsResult().setStatuses(maintenance.drain(hosts.getHostNames()))));
  }

  @Override
  public Response maintenanceStatus(Hosts hosts) {
    return ok(Result.maintenanceStatusResult(
        new MaintenanceStatusResult().setStatuses(maintenance.getStatus(hosts.getHostNames()))));
  }

  @Override
  public Response endMaintenance(Hosts hosts) {
    return ok(Result.endMaintenanceResult(
        new EndMaintenanceResult()
            .setStatuses(maintenance.endMaintenance(hosts.getHostNames()))));
  }

  @Override
  public Response forceTaskState(String taskId, ScheduleStatus status) {
    checkNotBlank(taskId);
    requireNonNull(status);

    storage.write(storeProvider -> stateManager.changeState(
        storeProvider,
        taskId,
        Optional.absent(),
        status,
        auditMessages.transitionedBy()));

    return ok();
  }

  @Override
  public Response performBackup() {
    backup.backupNow();
    return ok();
  }

  @Override
  public Response listBackups() {
    return ok(Result.listBackupsResult(new ListBackupsResult()
        .setBackups(recovery.listBackups())));
  }

  @Override
  public Response stageRecovery(String backupId) {
    recovery.stage(backupId);
    return ok();
  }

  @Override
  public Response queryRecovery(TaskQuery query) {
    return ok(Result.queryRecoveryResult(new QueryRecoveryResult()
        .setTasks(IScheduledTask.toBuildersSet(recovery.query(Query.arbitrary(query))))));
  }

  @Override
  public Response deleteRecoveryTasks(TaskQuery query) {
    recovery.deleteTasks(Query.arbitrary(query));
    return ok();
  }

  @Override
  public Response commitRecovery() {
    recovery.commit();
    return ok();
  }

  @Override
  public Response unloadRecovery() {
    recovery.unload();
    return ok();
  }

  @Override
  public Response snapshot() {
    storage.snapshot();
    return ok();
  }

  @Override
  public Response rewriteConfigs(RewriteConfigsRequest request) {
    if (request.getRewriteCommandsSize() == 0) {
      return addMessage(empty(), INVALID_REQUEST, "No rewrite commands provided.");
    }

    return storage.write(storeProvider -> {
      List<String> errors = Lists.newArrayList();

      for (ConfigRewrite command : request.getRewriteCommands()) {
        Optional<String> error = rewriteConfig(IConfigRewrite.build(command), storeProvider);
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
    });
  }

  private Optional<String> rewriteJob(IJobConfigRewrite jobRewrite, CronJobStore.Mutable jobStore) {
    IJobConfiguration existingJob = jobRewrite.getOldJob();
    IJobConfiguration rewrittenJob;
    Optional<String> error = Optional.absent();
    try {
      rewrittenJob = configurationManager.validateAndPopulate(jobRewrite.getRewrittenJob());
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
      IInstanceConfigRewrite instanceRewrite,
      MutableStoreProvider storeProvider) {

    IInstanceKey instanceKey = instanceRewrite.getInstanceKey();
    Optional<String> error = Optional.absent();
    Iterable<IScheduledTask> tasks = storeProvider.getTaskStore().fetchTasks(
        Query.instanceScoped(instanceKey.getJobKey(),
            instanceKey.getInstanceId())
            .active());
    Optional<IAssignedTask> task =
        Optional.fromNullable(Iterables.getOnlyElement(tasks, null))
            .transform(IScheduledTask::getAssignedTask);

    if (task.isPresent()) {
      if (task.get().getTask().equals(instanceRewrite.getOldTask())) {
        ITaskConfig newConfiguration = instanceRewrite.getRewrittenTask();
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
      IConfigRewrite command,
      MutableStoreProvider storeProvider) {

    Optional<String> error;
    switch (command.getSetField()) {
      case JOB_REWRITE:
        error = rewriteJob(command.getJobRewrite(), storeProvider.getCronJobStore());
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
  public Response addInstances(AddInstancesConfig config, @Nullable Lock mutableLock) {
    requireNonNull(config);
    checkNotBlank(config.getInstanceIds());
    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(config.getKey()));

    ITaskConfig task;
    try {
      task = configurationManager.validateAndPopulate(ITaskConfig.build(config.getTaskConfig()));
    } catch (TaskDescriptionException e) {
      return error(INVALID_REQUEST, e);
    }

    return storage.write(storeProvider -> {
      try {
        if (getCronJob(storeProvider, jobKey).isPresent()) {
          return invalidRequest("Instances may not be added to cron jobs.");
        }

        lockManager.validateIfLocked(
            ILockKey.build(LockKey.job(jobKey.newBuilder())),
            java.util.Optional.ofNullable(mutableLock).map(ILock::build));

        Iterable<IScheduledTask> currentTasks = storeProvider.getTaskStore().fetchTasks(
            Query.jobScoped(task.getJob()).active());

        validateTaskLimits(
            task,
            Iterables.size(currentTasks) + config.getInstanceIdsSize(),
            quotaManager.checkInstanceAddition(task, config.getInstanceIdsSize(), storeProvider));

        stateManager.insertPendingTasks(
            storeProvider,
            task,
            ImmutableSet.copyOf(config.getInstanceIds()));

        return ok();
      } catch (LockException e) {
        return error(LOCK_ERROR, e);
      } catch (TaskValidationException | IllegalArgumentException e) {
        return error(INVALID_REQUEST, e);
      }
    });
  }

  public Optional<IJobConfiguration> getCronJob(StoreProvider storeProvider, IJobKey jobKey) {
    requireNonNull(jobKey);
    return storeProvider.getCronJobStore().fetchJob(jobKey);
  }

  @Override
  public Response acquireLock(LockKey mutableLockKey) {
    requireNonNull(mutableLockKey);

    ILockKey lockKey = ILockKey.build(mutableLockKey);

    try {
      ILock lock = lockManager.acquireLock(lockKey, auditMessages.getRemoteUserName());
      return ok(Result.acquireLockResult(
          new AcquireLockResult().setLock(lock.newBuilder())));
    } catch (LockException e) {
      return error(LOCK_ERROR, e);
    }
  }

  @Override
  public Response releaseLock(Lock mutableLock, LockValidation validation) {
    requireNonNull(mutableLock);
    requireNonNull(validation);

    ILock lock = ILock.build(mutableLock);

    try {
      if (validation == LockValidation.CHECKED) {
        lockManager.validateIfLocked(lock.getKey(), java.util.Optional.of(lock));
      }
      lockManager.releaseLock(lock);
      return ok();
    } catch (LockException e) {
      return error(LOCK_ERROR, e);
    }
  }

  @Override
  public Response getLocks() throws TException {
    return readOnlyScheduler.getLocks();
  }

  private static class TaskValidationException extends Exception {
    TaskValidationException(String message) {
      super(message);
    }
  }

  private void validateTaskLimits(
      ITaskConfig task,
      int totalInstances,
      QuotaCheckResult quotaCheck) throws TaskValidationException {

    if (totalInstances <= 0 || totalInstances > thresholds.getMaxTasksPerJob()) {
      throw new TaskValidationException(String.format(
          "Instance count must be between 1 and %d inclusive.", thresholds.getMaxTasksPerJob()));
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

  private static Set<InstanceTaskConfig> buildInitialState(Map<Integer, ITaskConfig> tasks) {
    // Translate tasks into instance IDs.
    Multimap<ITaskConfig, Integer> instancesByConfig = HashMultimap.create();
    Multimaps.invertFrom(Multimaps.forMap(tasks), instancesByConfig);

    // Reduce instance IDs into contiguous ranges.
    Map<ITaskConfig, Set<Range<Integer>>> rangesByConfig =
        Maps.transformValues(instancesByConfig.asMap(), Numbers::toRanges);

    ImmutableSet.Builder<InstanceTaskConfig> builder = ImmutableSet.builder();
    for (Map.Entry<ITaskConfig, Set<Range<Integer>>> entry : rangesByConfig.entrySet()) {
      builder.add(new InstanceTaskConfig()
          .setTask(entry.getKey().newBuilder())
          .setInstances(IRange.toBuildersSet(convertRanges(entry.getValue()))));
    }

    return builder.build();
  }

  @Override
  public Response startJobUpdate(JobUpdateRequest mutableRequest, @Nullable String message) {
    requireNonNull(mutableRequest);

    // TODO(maxim): Switch to key field instead when AURORA-749 is fixed.
    IJobKey job = JobKeys.assertValid(IJobKey.build(new JobKey()
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

    if (settings.getMaxPerInstanceFailures() * mutableRequest.getInstanceCount()
            > thresholds.getMaxUpdateInstanceFailures()) {
      return invalidRequest(TOO_MANY_POTENTIAL_FAILED_INSTANCES);
    }

    if (settings.getMinWaitInInstanceRunningMs() < 0) {
      return invalidRequest(INVALID_MIN_WAIT_TO_RUNNING);
    }

    if (settings.getBlockIfNoPulsesAfterMs() < 0) {
      return invalidRequest(INVALID_PULSE_TIMEOUT);
    }

    IJobUpdateRequest request;
    try {
      request = IJobUpdateRequest.build(new JobUpdateRequest(mutableRequest).setTaskConfig(
          configurationManager.validateAndPopulate(
              ITaskConfig.build(mutableRequest.getTaskConfig())).newBuilder()));
    } catch (TaskDescriptionException e) {
      return error(INVALID_REQUEST, e);
    }

    return storage.write(storeProvider -> {
      if (getCronJob(storeProvider, job).isPresent()) {
        return invalidRequest(NO_CRON);
      }

      String updateId = uuidGenerator.createNew().toString();
      IJobUpdateSettings settings1 = request.getSettings();

      JobDiff diff = JobDiff.compute(
          storeProvider.getTaskStore(),
          job,
          JobDiff.asMap(request.getTaskConfig(), request.getInstanceCount()),
          settings1.getUpdateOnlyTheseInstances());

      Set<Integer> invalidScope = diff.getOutOfScopeInstances(
          Numbers.rangesToInstanceIds(settings1.getUpdateOnlyTheseInstances()));
      if (!invalidScope.isEmpty()) {
        return invalidRequest(
            "The update request attempted to update specific instances,"
                + " but some are irrelevant to the update and current job state: "
                + invalidScope);
      }

      if (diff.isNoop()) {
        return addMessage(empty(), OK, NOOP_JOB_UPDATE_MESSAGE);
      }

      JobUpdateInstructions instructions = new JobUpdateInstructions()
          .setSettings(settings1.newBuilder())
          .setInitialState(buildInitialState(diff.getReplacedInstances()));

      Set<Integer> replacements = diff.getReplacementInstances();
      if (!replacements.isEmpty()) {
        instructions.setDesiredState(
            new InstanceTaskConfig()
                .setTask(request.getTaskConfig().newBuilder())
                .setInstances(IRange.toBuildersSet(convertRanges(toRanges(replacements)))));
      }

      String remoteUserName = auditMessages.getRemoteUserName();
      IJobUpdate update = IJobUpdate.build(new JobUpdate()
          .setSummary(new JobUpdateSummary()
              .setKey(new JobUpdateKey(job.newBuilder(), updateId))
              .setUser(remoteUserName))
          .setInstructions(instructions));

      Response response = empty();
      if (update.getInstructions().getSettings().getMaxWaitToInstanceRunningMs() > 0) {
        addMessage(
            response,
            "The maxWaitToInstanceRunningMs (restart_threshold) field is deprecated.");
      }

      try {
        validateTaskLimits(
            request.getTaskConfig(),
            request.getInstanceCount(),
            quotaManager.checkJobUpdate(update, storeProvider));

        jobUpdateController.start(
            update,
            new AuditData(remoteUserName, Optional.fromNullable(message)));
        return response.setResponseCode(OK)
            .setResult(Result.startJobUpdateResult(
                new StartJobUpdateResult(update.getSummary().getKey().newBuilder())));
      } catch (UpdateStateException | TaskValidationException e) {
        return error(INVALID_REQUEST, e);
      }
    });
  }

  private Response changeJobUpdateState(
      JobUpdateKey mutableKey,
      JobUpdateStateChange change,
      Optional<String> message) {

    IJobUpdateKey key = IJobUpdateKey.build(mutableKey);
    JobKeys.assertValid(key.getJob());
    return storage.write(storeProvider -> {
      try {
        change.modifyUpdate(
            jobUpdateController,
            key,
            new AuditData(auditMessages.getRemoteUserName(), message));
        return ok();
      } catch (UpdateStateException e) {
        return error(INVALID_REQUEST, e);
      }
    });
  }

  private interface JobUpdateStateChange {
    void modifyUpdate(JobUpdateController controller, IJobUpdateKey key, AuditData auditData)
        throws UpdateStateException;
  }

  @Override
  public Response pauseJobUpdate(JobUpdateKey mutableKey, @Nullable String message) {
    return changeJobUpdateState(
        mutableKey,
        JobUpdateController::pause,
        Optional.fromNullable(message));
  }

  @Override
  public Response resumeJobUpdate(JobUpdateKey mutableKey, @Nullable String message) {
    return changeJobUpdateState(
        mutableKey,
        JobUpdateController::resume,
        Optional.fromNullable(message));
  }

  @Override
  public Response abortJobUpdate(JobUpdateKey mutableKey, @Nullable String message) {
    return changeJobUpdateState(
        mutableKey,
        JobUpdateController::abort,
        Optional.fromNullable(message));
  }

  @Override
  public Response pulseJobUpdate(JobUpdateKey mutableUpdateKey) {
    IJobUpdateKey updateKey = validateJobUpdateKey(mutableUpdateKey);
    try {
      JobUpdatePulseStatus result = jobUpdateController.pulse(updateKey);
      return ok(Result.pulseJobUpdateResult(new PulseJobUpdateResult(result)));
    } catch (UpdateStateException e) {
      return error(INVALID_REQUEST, e);
    }
  }

  @Override
  public Response getJobUpdateSummaries(JobUpdateQuery mutableQuery) throws TException {
    return readOnlyScheduler.getJobUpdateSummaries(mutableQuery);
  }

  @Override
  public Response getJobUpdateDetails(JobUpdateKey key) throws TException {
    return readOnlyScheduler.getJobUpdateDetails(key);
  }

  private static IJobUpdateKey validateJobUpdateKey(JobUpdateKey mutableKey) {
    IJobUpdateKey key = IJobUpdateKey.build(mutableKey);
    JobKeys.assertValid(key.getJob());
    checkNotBlank(key.getId());
    return key;
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
  static final String TOO_MANY_POTENTIAL_FAILED_INSTANCES = "Your update allows too many failures "
      + "to occur, consider decreasing the per-instance failures or maxFailedInstances.";

  @VisibleForTesting
  static final String INVALID_MAX_INSTANCE_FAILURES
      = "maxPerInstanceFailures must be non-negative.";

  @VisibleForTesting
  static final String INVALID_MIN_WAIT_TO_RUNNING =
      "minWaitInInstanceRunningMs must be non-negative.";

  @VisibleForTesting
  static final String INVALID_PULSE_TIMEOUT = "blockIfNoPulsesAfterMs must be positive.";
}
