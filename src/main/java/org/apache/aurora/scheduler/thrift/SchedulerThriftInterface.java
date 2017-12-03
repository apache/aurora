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

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.gen.DrainHostsResult;
import org.apache.aurora.gen.EndMaintenanceResult;
import org.apache.aurora.gen.ExplicitReconciliationSettings;
import org.apache.aurora.gen.Hosts;
import org.apache.aurora.gen.InstanceKey;
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
import org.apache.aurora.gen.MaintenanceStatusResult;
import org.apache.aurora.gen.PulseJobUpdateResult;
import org.apache.aurora.gen.QueryRecoveryResult;
import org.apache.aurora.gen.ReadOnlyScheduler;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.StartJobUpdateResult;
import org.apache.aurora.gen.StartMaintenanceResult;
import org.apache.aurora.gen.TaskQuery;
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
import org.apache.aurora.scheduler.reconciliation.TaskReconciler;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.UUIDGenerator;
import org.apache.aurora.scheduler.storage.DistributedSnapshotStore;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.storage.durability.ThriftBackfill;
import org.apache.aurora.scheduler.storage.entities.IHostStatus;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateRequest;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSettings;
import org.apache.aurora.scheduler.storage.entities.IMetadata;
import org.apache.aurora.scheduler.storage.entities.IRange;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.thrift.aop.AnnotatedAuroraAdmin;
import org.apache.aurora.scheduler.thrift.aop.ThriftWorkload;
import org.apache.aurora.scheduler.thrift.auth.DecoratedThrift;
import org.apache.aurora.scheduler.updater.JobDiff;
import org.apache.aurora.scheduler.updater.JobUpdateController;
import org.apache.aurora.scheduler.updater.JobUpdateController.AuditData;
import org.apache.aurora.scheduler.updater.JobUpdateController.JobUpdatingException;
import org.apache.aurora.scheduler.updater.UpdateInProgressException;
import org.apache.aurora.scheduler.updater.UpdateStateException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.common.base.MorePreconditions.checkNotBlank;
import static org.apache.aurora.gen.ResponseCode.INVALID_REQUEST;
import static org.apache.aurora.gen.ResponseCode.JOB_UPDATING_ERROR;
import static org.apache.aurora.gen.ResponseCode.OK;
import static org.apache.aurora.scheduler.base.Numbers.convertRanges;
import static org.apache.aurora.scheduler.base.Numbers.toRanges;
import static org.apache.aurora.scheduler.base.Tasks.ACTIVE_STATES;
import static org.apache.aurora.scheduler.base.Tasks.TERMINAL_STATES;
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

  @VisibleForTesting
  static final String STAT_PREFIX = "thrift_workload_";
  @VisibleForTesting
  static final String CREATE_JOB = STAT_PREFIX + "createJob";
  @VisibleForTesting
  static final String CREATE_OR_UPDATE_CRON = STAT_PREFIX + "createOrUpdateCronTemplate";
  @VisibleForTesting
  static final String KILL_TASKS = STAT_PREFIX + "killTasks";
  @VisibleForTesting
  static final String RESTART_SHARDS = STAT_PREFIX + "restartShards";
  @VisibleForTesting
  static final String START_MAINTENANCE = STAT_PREFIX + "startMaintenance";
  @VisibleForTesting
  static final String DRAIN_HOSTS = STAT_PREFIX + "drainHosts";
  @VisibleForTesting
  static final String MAINTENANCE_STATUS = STAT_PREFIX + "maintenanceStatus";
  @VisibleForTesting
  static final String END_MAINTENANCE = STAT_PREFIX + "endMaintenance";
  @VisibleForTesting
  static final String ADD_INSTANCES = STAT_PREFIX + "addInstances";
  @VisibleForTesting
  static final String START_JOB_UPDATE = STAT_PREFIX + "startJobUpdate";

  private static final Logger LOG = LoggerFactory.getLogger(SchedulerThriftInterface.class);

  private final ConfigurationManager configurationManager;
  private final Thresholds thresholds;
  private final NonVolatileStorage storage;
  private final DistributedSnapshotStore snapshotStore;
  private final StorageBackup backup;
  private final Recovery recovery;
  private final MaintenanceController maintenance;
  private final CronJobManager cronJobManager;
  private final QuotaManager quotaManager;
  private final StateManager stateManager;
  private final UUIDGenerator uuidGenerator;
  private final JobUpdateController jobUpdateController;
  private final ReadOnlyScheduler.Iface readOnlyScheduler;
  private final AuditMessages auditMessages;
  private final TaskReconciler taskReconciler;

  private final AtomicLong createJobCounter;
  private final AtomicLong createOrUpdateCronCounter;
  private final AtomicLong killTasksCounter;
  private final AtomicLong restartShardsCounter;
  private final AtomicLong startMaintenanceCounter;
  private final AtomicLong drainHostsCounter;
  private final AtomicLong maintenanceStatusCounter;
  private final AtomicLong endMaintenanceCounter;
  private final AtomicLong addInstancesCounter;
  private final AtomicLong startJobUpdateCounter;

  @Inject
  SchedulerThriftInterface(
      ConfigurationManager configurationManager,
      Thresholds thresholds,
      NonVolatileStorage storage,
      DistributedSnapshotStore snapshotStore,
      StorageBackup backup,
      Recovery recovery,
      CronJobManager cronJobManager,
      MaintenanceController maintenance,
      QuotaManager quotaManager,
      StateManager stateManager,
      UUIDGenerator uuidGenerator,
      JobUpdateController jobUpdateController,
      ReadOnlyScheduler.Iface readOnlyScheduler,
      AuditMessages auditMessages,
      TaskReconciler taskReconciler,
      StatsProvider statsProvider) {

    this.configurationManager = requireNonNull(configurationManager);
    this.thresholds = requireNonNull(thresholds);
    this.storage = requireNonNull(storage);
    this.snapshotStore = requireNonNull(snapshotStore);
    this.backup = requireNonNull(backup);
    this.recovery = requireNonNull(recovery);
    this.maintenance = requireNonNull(maintenance);
    this.cronJobManager = requireNonNull(cronJobManager);
    this.quotaManager = requireNonNull(quotaManager);
    this.stateManager = requireNonNull(stateManager);
    this.uuidGenerator = requireNonNull(uuidGenerator);
    this.jobUpdateController = requireNonNull(jobUpdateController);
    this.readOnlyScheduler = requireNonNull(readOnlyScheduler);
    this.auditMessages = requireNonNull(auditMessages);
    this.taskReconciler = requireNonNull(taskReconciler);

    this.createJobCounter = statsProvider.makeCounter(CREATE_JOB);
    this.createOrUpdateCronCounter = statsProvider.makeCounter(CREATE_OR_UPDATE_CRON);
    this.killTasksCounter = statsProvider.makeCounter(KILL_TASKS);
    this.restartShardsCounter = statsProvider.makeCounter(RESTART_SHARDS);
    this.startMaintenanceCounter = statsProvider.makeCounter(START_MAINTENANCE);
    this.drainHostsCounter = statsProvider.makeCounter(DRAIN_HOSTS);
    this.maintenanceStatusCounter = statsProvider.makeCounter(MAINTENANCE_STATUS);
    this.endMaintenanceCounter = statsProvider.makeCounter(END_MAINTENANCE);
    this.addInstancesCounter = statsProvider.makeCounter(ADD_INSTANCES);
    this.startJobUpdateCounter = statsProvider.makeCounter(START_JOB_UPDATE);
  }

  @Override
  public Response createJob(JobConfiguration mutableJob) {
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
        jobUpdateController.assertNotUpdating(job.getKey());

        checkJobExists(storeProvider, job.getKey());

        ITaskConfig template = sanitized.getJobConfig().getTaskConfig();
        int count = sanitized.getJobConfig().getInstanceCount();

        validateTaskLimits(
            count,
            quotaManager.checkInstanceAddition(template, count, storeProvider));

        LOG.info("Launching " + count + " tasks.");
        stateManager.insertPendingTasks(
            storeProvider,
            template,
            sanitized.getInstanceIds());
        createJobCounter.addAndGet(sanitized.getInstanceIds().size());

        return ok();
      } catch (JobUpdatingException e) {
        return error(JOB_UPDATING_ERROR, e);
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
        jobUpdateController.assertNotUpdating(jobKey);

        int count = sanitized.getJobConfig().getInstanceCount();

        validateTaskLimits(
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
        createOrUpdateCronCounter.addAndGet(count);

        return ok();
      } catch (JobUpdatingException e) {
        return error(JOB_UPDATING_ERROR, e);
      } catch (JobExistsException | TaskValidationException | CronException e) {
        return error(INVALID_REQUEST, e);
      }
    });
  }

  @Override
  public Response scheduleCronJob(JobConfiguration mutableJob) {
    return createOrUpdateCronTemplate(mutableJob, false);
  }

  @Override
  public Response replaceCronTemplate(JobConfiguration mutableJob) {
    return createOrUpdateCronTemplate(mutableJob, true);
  }

  @Override
  public Response descheduleCronJob(JobKey mutableJobKey) {
    try {
      IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));
      jobUpdateController.assertNotUpdating(jobKey);

      if (cronJobManager.deleteJob(jobKey)) {
        return ok();
      } else {
        return addMessage(empty(), OK, notScheduledCronMessage(jobKey));
      }
    } catch (JobUpdatingException e) {
      return error(JOB_UPDATING_ERROR, e);
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
  @ThriftWorkload
  @Override
  public Response getTasksStatus(TaskQuery query) throws TException {
    return readOnlyScheduler.getTasksStatus(query);
  }

  @ThriftWorkload
  @Override
  public Response getTasksWithoutConfigs(TaskQuery query) throws TException {
    return readOnlyScheduler.getTasksWithoutConfigs(query);
  }

  @ThriftWorkload
  @Override
  public Response getPendingReason(TaskQuery query) throws TException {
    return readOnlyScheduler.getPendingReason(query);
  }

  @ThriftWorkload
  @Override
  public Response getConfigSummary(JobKey job) throws TException {
    return readOnlyScheduler.getConfigSummary(job);
  }

  @ThriftWorkload
  @Override
  public Response getRoleSummary() throws TException {
    return readOnlyScheduler.getRoleSummary();
  }

  @ThriftWorkload
  @Override
  public Response getJobSummary(@Nullable String maybeNullRole) throws TException {
    return readOnlyScheduler.getJobSummary(maybeNullRole);
  }

  @ThriftWorkload
  @Override
  public Response getJobs(@Nullable String maybeNullRole) throws TException {
    return readOnlyScheduler.getJobs(maybeNullRole);
  }

  @Override
  public Response getJobUpdateDiff(JobUpdateRequest request) throws TException {
    return readOnlyScheduler.getJobUpdateDiff(request);
  }

  @Override
  public Response getTierConfigs() throws TException {
    return readOnlyScheduler.getTierConfigs();
  }

  private static Query.Builder implicitKillQuery(Query.Builder query) {
    // Unless statuses were specifically supplied, only attempt to kill active tasks.
    return query.get().getStatuses().isEmpty() ? query.byStatus(ACTIVE_STATES) : query;
  }

  @Override
  public Response killTasks(
      @Nullable JobKey mutableJob,
      @Nullable Set<Integer> instances,
      @Nullable String message) {

    Response response = empty();
    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJob));
    Query.Builder query;
    if (instances == null || Iterables.isEmpty(instances)) {
      query = implicitKillQuery(Query.jobScoped(jobKey));
    } else {
      query = implicitKillQuery(Query.instanceScoped(jobKey, instances));
    }

    return storage.write(storeProvider -> {
      try {
        jobUpdateController.assertNotUpdating(jobKey);
      } catch (JobUpdatingException e) {
        return error(JOB_UPDATING_ERROR, e);
      }

      Iterable<IScheduledTask> tasks = storeProvider.getTaskStore().fetchTasks(query);

      LOG.info("Killing tasks matching " + query);

      int tasksKilled = 0;
      for (String taskId : Tasks.ids(tasks)) {
        if (StateChangeResult.SUCCESS == stateManager.changeState(
            storeProvider,
            taskId,
            Optional.absent(),
            ScheduleStatus.KILLING,
            auditMessages.killedByRemoteUser(Optional.fromNullable(message)))) {
          ++tasksKilled;
        }
      }
      killTasksCounter.addAndGet(tasksKilled);

      return tasksKilled > 0
          ? response.setResponseCode(OK)
          : addMessage(response, OK, NO_TASKS_TO_KILL_MESSAGE);
    });
  }

  @Override
  public Response restartShards(JobKey mutableJobKey, Set<Integer> shardIds) {
    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));
    checkNotBlank(shardIds);

    return storage.write(storeProvider -> {
      try {
        jobUpdateController.assertNotUpdating(jobKey);
      } catch (JobUpdatingException e) {
        return error(JOB_UPDATING_ERROR, e);
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
      restartShardsCounter.addAndGet(shardIds.size());

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
          ThriftBackfill.backfillResourceAggregate(resourceAggregate),
          store));
      return ok();
    } catch (QuotaException e) {
      return error(INVALID_REQUEST, e);
    }
  }

  @Override
  public Response startMaintenance(Hosts hosts) {
    startMaintenanceCounter.addAndGet(hosts.getHostNamesSize());
    return ok(Result.startMaintenanceResult(
        new StartMaintenanceResult()
            .setStatuses(IHostStatus.toBuildersSet(
                maintenance.startMaintenance(hosts.getHostNames())))));
  }

  @Override
  public Response drainHosts(Hosts hosts) {
    drainHostsCounter.addAndGet(hosts.getHostNamesSize());
    return ok(Result.drainHostsResult(
        new DrainHostsResult().setStatuses(IHostStatus.toBuildersSet(
            maintenance.drain(hosts.getHostNames())))));
  }

  @Override
  public Response maintenanceStatus(Hosts hosts) {
    maintenanceStatusCounter.addAndGet(hosts.getHostNamesSize());
    return ok(Result.maintenanceStatusResult(
        new MaintenanceStatusResult().setStatuses(IHostStatus.toBuildersSet(
            maintenance.getStatus(hosts.getHostNames())))));
  }

  @Override
  public Response endMaintenance(Hosts hosts) {
    endMaintenanceCounter.addAndGet(hosts.getHostNamesSize());
    return ok(Result.endMaintenanceResult(
        new EndMaintenanceResult()
            .setStatuses(IHostStatus.toBuildersSet(
                maintenance.endMaintenance(hosts.getHostNames())))));
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
    snapshotStore.snapshot();
    return ok();
  }

  @Override
  public Response triggerExplicitTaskReconciliation(ExplicitReconciliationSettings settings)
      throws TException {
    try {
      requireNonNull(settings);
      Preconditions.checkArgument(!settings.isSetBatchSize() || settings.getBatchSize() > 0,
          "Batch size must be greater than zero.");

      Optional<Integer> batchSize = settings.isSetBatchSize()
          ? Optional.of(settings.getBatchSize())
          : Optional.absent();

      taskReconciler.triggerExplicitReconciliation(batchSize);
      return ok();
    } catch (IllegalArgumentException e) {
      return error(INVALID_REQUEST, e);
    }
  }

  @Override
  public Response triggerImplicitTaskReconciliation() throws TException {
    taskReconciler.triggerImplicitReconciliation();
    return ok();
  }

  @Override
  public Response addInstances(InstanceKey key, int count) {
    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(key.getJobKey()));

    if (count <= 0) {
      return invalidRequest(INVALID_INSTANCE_COUNT);
    }

    Response response = empty();
    return storage.write(storeProvider -> {
      try {
        if (getCronJob(storeProvider, jobKey).isPresent()) {
          return invalidRequest("Instances may not be added to cron jobs.");
        }

        jobUpdateController.assertNotUpdating(jobKey);

        FluentIterable<IScheduledTask> currentTasks = FluentIterable.from(
            storeProvider.getTaskStore().fetchTasks(Query.jobScoped(jobKey).active()));

        Optional<IScheduledTask> templateTask = Iterables.tryFind(
            currentTasks,
            e -> e.getAssignedTask().getInstanceId() == key.getInstanceId());
        if (!templateTask.isPresent()) {
          return invalidRequest(INVALID_INSTANCE_ID);
        }

        int lastId = currentTasks
            .transform(e -> e.getAssignedTask().getInstanceId())
            .toList()
            .stream()
            .max(Comparator.naturalOrder()).get();

        Set<Integer> instanceIds = ContiguousSet.create(
            Range.openClosed(lastId, lastId + count),
            DiscreteDomain.integers());

        ITaskConfig task = templateTask.get().getAssignedTask().getTask();
        validateTaskLimits(
            Iterables.size(currentTasks) + instanceIds.size(),
            quotaManager.checkInstanceAddition(task, instanceIds.size(), storeProvider));

        stateManager.insertPendingTasks(storeProvider, task, instanceIds);
        addInstancesCounter.addAndGet(instanceIds.size());

        return response.setResponseCode(OK);
      } catch (JobUpdatingException e) {
        return error(JOB_UPDATING_ERROR, e);
      } catch (TaskValidationException | IllegalArgumentException e) {
        return error(INVALID_REQUEST, e);
      }
    });
  }

  private Optional<IJobConfiguration> getCronJob(StoreProvider storeProvider, IJobKey jobKey) {
    requireNonNull(jobKey);
    return storeProvider.getCronJobStore().fetchJob(jobKey);
  }

  private static class TaskValidationException extends Exception {
    TaskValidationException(String message) {
      super(message);
    }
  }

  private void validateTaskLimits(
      int totalInstances,
      QuotaCheckResult quotaCheck) throws TaskValidationException {

    if (totalInstances <= 0 || totalInstances > thresholds.getMaxTasksPerJob()) {
      throw new TaskValidationException(String.format(
          "Instance count must be between 1 and %d inclusive.", thresholds.getMaxTasksPerJob()));
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
      IJobKey job = request.getTaskConfig().getJob();
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
              .setUser(remoteUserName)
              .setMetadata(IMetadata.toBuildersSet(request.getMetadata())))
          .setInstructions(instructions));

      Response response = empty();
      try {
        validateTaskLimits(
            request.getInstanceCount(),
            quotaManager.checkJobUpdate(update, storeProvider));

        jobUpdateController.start(
            update,
            new AuditData(remoteUserName, Optional.fromNullable(message)));
        startJobUpdateCounter.addAndGet(request.getInstanceCount());
        return response.setResponseCode(OK)
            .setResult(Result.startJobUpdateResult(
                new StartJobUpdateResult(update.getSummary().getKey().newBuilder())
                    .setUpdateSummary(update.getSummary().newBuilder())));
      } catch (UpdateInProgressException e) {
        return error(INVALID_REQUEST, e)
            .setResult(Result.startJobUpdateResult(
                new StartJobUpdateResult(e.getInProgressUpdateSummary().getKey().newBuilder())
                    .setUpdateSummary(e.getInProgressUpdateSummary().newBuilder())));
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
  public Response rollbackJobUpdate(JobUpdateKey mutableKey, @Nullable String message) {
    return changeJobUpdateState(
        mutableKey,
        JobUpdateController::rollback,
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
  public Response pruneTasks(TaskQuery query) throws TException {
    if (query.isSetStatuses() && query.getStatuses().stream().anyMatch(ACTIVE_STATES::contains)) {
      return error("Tasks in non-terminal state cannot be pruned.");
    } else if (!query.isSetStatuses()) {
      query.setStatuses(TERMINAL_STATES);
    }

    Iterable<IScheduledTask> tasks = storage.read(storeProvider ->
        storeProvider.getTaskStore().fetchTasks(Query.arbitrary(query)));
    // For some reason fetchTasks ignores the offset/limit options of a TaskQuery. So we have to
    // manually apply the limit here. To be fixed in AURORA-1892.
    if (query.isSetLimit()) {
      tasks = Iterables.limit(tasks, query.getLimit());
    }

    Iterable<String> taskIds = Iterables.transform(
        tasks,
        task -> task.getAssignedTask().getTaskId());

    return storage.write(storeProvider -> {
      stateManager.deleteTasks(storeProvider, Sets.newHashSet(taskIds));
      return ok();
    });
  }

  @ThriftWorkload
  @Override
  public Response getJobUpdateSummaries(JobUpdateQuery mutableQuery) throws TException {
    return readOnlyScheduler.getJobUpdateSummaries(mutableQuery);
  }

  @ThriftWorkload
  @Override
  public Response getJobUpdateDetails(JobUpdateKey key, JobUpdateQuery query) throws TException {
    return readOnlyScheduler.getJobUpdateDetails(key, query);
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

  @VisibleForTesting
  static final String INVALID_INSTANCE_ID = "No active task found for a given instance ID.";

  @VisibleForTesting
  static final String INVALID_INSTANCE_COUNT = "Instance count must be positive.";
}
