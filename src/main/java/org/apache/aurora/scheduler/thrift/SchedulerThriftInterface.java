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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import org.apache.aurora.auth.CapabilityValidator;
import org.apache.aurora.auth.CapabilityValidator.AuditCheck;
import org.apache.aurora.auth.CapabilityValidator.Capability;
import org.apache.aurora.auth.SessionValidator.AuthFailedException;
import org.apache.aurora.gen.AcquireLockResult;
import org.apache.aurora.gen.AddInstancesConfig;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.ConfigGroup;
import org.apache.aurora.gen.ConfigRewrite;
import org.apache.aurora.gen.ConfigSummary;
import org.apache.aurora.gen.ConfigSummaryResult;
import org.apache.aurora.gen.DrainHostsResult;
import org.apache.aurora.gen.EndMaintenanceResult;
import org.apache.aurora.gen.GetJobsResult;
import org.apache.aurora.gen.GetLocksResult;
import org.apache.aurora.gen.GetPendingReasonResult;
import org.apache.aurora.gen.GetQuotaResult;
import org.apache.aurora.gen.Hosts;
import org.apache.aurora.gen.InstanceConfigRewrite;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfigRewrite;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobSummary;
import org.apache.aurora.gen.JobSummaryResult;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateConfiguration;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.ListBackupsResult;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.LockKey._Fields;
import org.apache.aurora.gen.LockValidation;
import org.apache.aurora.gen.MaintenanceStatusResult;
import org.apache.aurora.gen.PendingReason;
import org.apache.aurora.gen.PopulateJobResult;
import org.apache.aurora.gen.QueryRecoveryResult;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.RewriteConfigsRequest;
import org.apache.aurora.gen.RoleSummary;
import org.apache.aurora.gen.RoleSummaryResult;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduleStatusResult;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.SessionKey;
import org.apache.aurora.gen.StartJobUpdateResult;
import org.apache.aurora.gen.StartMaintenanceResult;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Jobs;
import org.apache.aurora.scheduler.base.Numbers;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.ScheduleException;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CrontabEntry;
import org.apache.aurora.scheduler.cron.SanitizedCronJob;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.metadata.NearestFit;
import org.apache.aurora.scheduler.quota.QuotaInfo;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.quota.QuotaManager.QuotaException;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.state.LockManager.LockException;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.aurora.scheduler.state.SchedulerCore;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.UUIDGenerator;
import org.apache.aurora.scheduler.storage.JobStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.Recovery.RecoveryException;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateRequest;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.thrift.auth.DecoratedThrift;
import org.apache.aurora.scheduler.thrift.auth.Requires;
import org.apache.aurora.scheduler.updater.JobUpdateController;
import org.apache.aurora.scheduler.updater.UpdateStateException;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;

import static java.util.Objects.requireNonNull;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;

import static org.apache.aurora.auth.SessionValidator.SessionContext;
import static org.apache.aurora.gen.ResponseCode.AUTH_FAILED;
import static org.apache.aurora.gen.ResponseCode.ERROR;
import static org.apache.aurora.gen.ResponseCode.INVALID_REQUEST;
import static org.apache.aurora.gen.ResponseCode.LOCK_ERROR;
import static org.apache.aurora.gen.ResponseCode.OK;
import static org.apache.aurora.gen.ResponseCode.WARNING;
import static org.apache.aurora.gen.apiConstants.CURRENT_API_VERSION;
import static org.apache.aurora.scheduler.base.Tasks.ACTIVE_STATES;
import static org.apache.aurora.scheduler.thrift.Util.addMessage;
import static org.apache.aurora.scheduler.thrift.Util.emptyResponse;

/**
 * Aurora scheduler thrift server implementation.
 * <p/>
 * Interfaces between users and the scheduler to access/modify jobs and perform cluster
 * administration tasks.
 */
@DecoratedThrift
class SchedulerThriftInterface implements AuroraAdmin.Iface {
  private static final Logger LOG = Logger.getLogger(SchedulerThriftInterface.class.getName());

  private static final Function<IScheduledTask, String> GET_ROLE = Functions.compose(
      new Function<ITaskConfig, String>() {
        @Override
        public String apply(ITaskConfig task) {
          return task.getOwner().getRole();
        }
      },
      Tasks.SCHEDULED_TO_INFO);

  private final NonVolatileStorage storage;
  private final SchedulerCore schedulerCore;
  private final LockManager lockManager;
  private final CapabilityValidator sessionValidator;
  private final StorageBackup backup;
  private final Recovery recovery;
  private final MaintenanceController maintenance;
  private final CronJobManager cronJobManager;
  private final CronPredictor cronPredictor;
  private final QuotaManager quotaManager;
  private final NearestFit nearestFit;
  private final StateManager stateManager;
  private final UUIDGenerator uuidGenerator;
  private final JobUpdateController jobUpdateController;

  @Inject
  SchedulerThriftInterface(
      NonVolatileStorage storage,
      SchedulerCore schedulerCore,
      LockManager lockManager,
      CapabilityValidator sessionValidator,
      StorageBackup backup,
      Recovery recovery,
      CronJobManager cronJobManager,
      CronPredictor cronPredictor,
      MaintenanceController maintenance,
      QuotaManager quotaManager,
      NearestFit nearestFit,
      StateManager stateManager,
      UUIDGenerator uuidGenerator,
      JobUpdateController jobUpdateController) {

    this(storage,
        schedulerCore,
        lockManager,
        sessionValidator,
        backup,
        recovery,
        maintenance,
        cronJobManager,
        cronPredictor,
        quotaManager,
        nearestFit,
        stateManager,
        uuidGenerator,
        jobUpdateController);
  }

  @VisibleForTesting
  SchedulerThriftInterface(
      NonVolatileStorage storage,
      SchedulerCore schedulerCore,
      LockManager lockManager,
      CapabilityValidator sessionValidator,
      StorageBackup backup,
      Recovery recovery,
      MaintenanceController maintenance,
      CronJobManager cronJobManager,
      CronPredictor cronPredictor,
      QuotaManager quotaManager,
      NearestFit nearestFit,
      StateManager stateManager,
      UUIDGenerator uuidGenerator,
      JobUpdateController jobUpdateController) {

    this.storage = requireNonNull(storage);
    this.schedulerCore = requireNonNull(schedulerCore);
    this.lockManager = requireNonNull(lockManager);
    this.sessionValidator = requireNonNull(sessionValidator);
    this.backup = requireNonNull(backup);
    this.recovery = requireNonNull(recovery);
    this.maintenance = requireNonNull(maintenance);
    this.cronJobManager = requireNonNull(cronJobManager);
    this.cronPredictor = requireNonNull(cronPredictor);
    this.quotaManager = requireNonNull(quotaManager);
    this.nearestFit = requireNonNull(nearestFit);
    this.stateManager = requireNonNull(stateManager);
    this.uuidGenerator = requireNonNull(uuidGenerator);
    this.jobUpdateController = requireNonNull(jobUpdateController);
  }

  @Override
  public Response createJob(
      JobConfiguration mutableJob,
      @Nullable Lock mutableLock,
      SessionKey session) {

    IJobConfiguration job = IJobConfiguration.build(mutableJob);
    IJobKey jobKey = JobKeys.assertValid(job.getKey());
    requireNonNull(session);

    Response response = Util.emptyResponse();

    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(job.getOwner().getRole()));
    } catch (AuthFailedException e) {
      return addMessage(response, AUTH_FAILED, e);
    }

    try {
      SanitizedConfiguration sanitized = SanitizedConfiguration.fromUnsanitized(job);

      lockManager.validateIfLocked(
          ILockKey.build(LockKey.job(jobKey.newBuilder())),
          Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

      schedulerCore.createJob(sanitized);
      response.setResponseCode(OK);
    } catch (LockException e) {
      addMessage(response, LOCK_ERROR, e);
    } catch (TaskDescriptionException | ScheduleException e) {
      addMessage(response, INVALID_REQUEST, e);
    }
    return response;
  }

  @Override
  public Response scheduleCronJob(
      JobConfiguration mutableJob,
      @Nullable Lock mutableLock,
      SessionKey session) {

    IJobConfiguration job = IJobConfiguration.build(mutableJob);
    IJobKey jobKey = JobKeys.assertValid(job.getKey());
    requireNonNull(session);

    Response response = Util.emptyResponse();

    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(job.getOwner().getRole()));
    } catch (AuthFailedException e) {
      return addMessage(response, AUTH_FAILED, e);
    }

    try {
      SanitizedConfiguration sanitized = SanitizedConfiguration.fromUnsanitized(job);

      lockManager.validateIfLocked(
          ILockKey.build(LockKey.job(jobKey.newBuilder())),
          Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

      if (!sanitized.isCron()) {
        LOG.info("Invalid attempt to schedule non-cron job "
            + sanitized.getJobConfig().getKey()
            + " with cron.");
        response.setResponseCode(INVALID_REQUEST);
        return addMessage(
            response,
            "Job " + sanitized.getJobConfig().getKey() + " has no cron schedule");
      }
      try {
        // TODO(mchucarroll): Merge CronJobManager.createJob/updateJob
        if (cronJobManager.hasJob(sanitized.getJobConfig().getKey())) {
          // The job already has a schedule: so update it.
          cronJobManager.updateJob(SanitizedCronJob.from(sanitized));
        } else {
          cronJobManager.createJob(SanitizedCronJob.from(sanitized));
        }
      } catch (CronException e) {
        addMessage(response, INVALID_REQUEST, e);
        return response;
      }
      response.setResponseCode(OK);
    } catch (LockException e) {
      addMessage(response, LOCK_ERROR, e);
    } catch (TaskDescriptionException e) {
      addMessage(response, INVALID_REQUEST, e);
    }
    return response;
  }

  @Override
  public Response descheduleCronJob(
      JobKey mutableJobKey,
      @Nullable Lock mutableLock,
      SessionKey session) {

    Response response = Util.emptyResponse();
    try {
      IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));
      lockManager.validateIfLocked(
          ILockKey.build(LockKey.job(jobKey.newBuilder())),
          Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

      if (!cronJobManager.deleteJob(jobKey)) {
        return addMessage(
            response,
            INVALID_REQUEST,
            "Job " + jobKey + " is not scheduled with cron");
      }
      response.setResponseCode(OK);
    } catch (LockException e) {
      addMessage(response, LOCK_ERROR, e);
    }
    return response;
  }

  @Override
  public Response replaceCronTemplate(
      JobConfiguration mutableConfig,
      @Nullable Lock mutableLock,
      SessionKey session) {

    requireNonNull(mutableConfig);
    IJobConfiguration job = IJobConfiguration.build(mutableConfig);
    IJobKey jobKey = JobKeys.assertValid(job.getKey());
    requireNonNull(session);

    Response response = Util.emptyResponse();
    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(job.getOwner().getRole()));
    } catch (AuthFailedException e) {
      return addMessage(response, AUTH_FAILED, e);
    }

    try {
      lockManager.validateIfLocked(
          ILockKey.build(LockKey.job(jobKey.newBuilder())),
          Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

      cronJobManager.updateJob(SanitizedCronJob.fromUnsanitized(job));
      return response.setResponseCode(OK);
    } catch (LockException e) {
      return addMessage(response, LOCK_ERROR, e);
    } catch (CronException | TaskDescriptionException e) {
      return addMessage(response, INVALID_REQUEST, e);
    }
  }

  @Override
  public Response populateJobConfig(JobConfiguration description) {
    requireNonNull(description);

    Response response = Util.emptyResponse();
    try {
      SanitizedConfiguration sanitized =
          SanitizedConfiguration.fromUnsanitized(IJobConfiguration.build(description));

      PopulateJobResult result = new PopulateJobResult()
          .setPopulated(ITaskConfig.toBuildersSet(sanitized.getTaskConfigs().values()));

      response.setResult(Result.populateJobResult(result));
      response.setResponseCode(OK);
    } catch (TaskDescriptionException e) {
      addMessage(response, INVALID_REQUEST, "Invalid configuration: " + e.getMessage());
    }
    return response;
  }

  @Override
  public Response startCronJob(JobKey mutableJobKey, SessionKey session) {
    requireNonNull(session);
    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));

    Response response = Util.emptyResponse();
    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
    } catch (AuthFailedException e) {
      return addMessage(response, AUTH_FAILED, e);
    }

    try {
      cronJobManager.startJobNow(jobKey);
      return response.setResponseCode(OK);
    } catch (CronException e) {
      return addMessage(response, INVALID_REQUEST, "Failed to start cron job - " + e.getMessage());
    }
  }

  // TODO(William Farner): Provide status information about cron jobs here.
  @Override
  public Response getTasksStatus(TaskQuery query) {
    return okResponse(Result.scheduleStatusResult(
        new ScheduleStatusResult().setTasks(getTasks(query))));
  }

  @Override
  public Response getTasksWithoutConfigs(TaskQuery query) {
    List<ScheduledTask> tasks = Lists.transform(
        getTasks(query),
        new Function<ScheduledTask, ScheduledTask>() {
          @Override
          public ScheduledTask apply(ScheduledTask task) {
            task.assignedTask.task.executorConfig = null;
            return task;
          }
        });

    return okResponse(Result.scheduleStatusResult(new ScheduleStatusResult().setTasks(tasks)));
  }

  private List<ScheduledTask> getTasks(TaskQuery query) {
    requireNonNull(query);

    Iterable<IScheduledTask> tasks =
        Storage.Util.weaklyConsistentFetchTasks(storage, Query.arbitrary(query));

    if (query.isSetOffset()) {
      tasks = Iterables.skip(tasks, query.getOffset());
    }
    if (query.isSetLimit()) {
      tasks = Iterables.limit(tasks, query.getLimit());
    }

    return IScheduledTask.toBuildersList(tasks);
  }

  private static final Function<Entry<ITaskConfig, Collection<Integer>>, ConfigGroup>
      CONFIG_TO_GROUP = new Function<Entry<ITaskConfig, Collection<Integer>>, ConfigGroup>() {

    @Override
    public ConfigGroup apply(Entry<ITaskConfig, Collection<Integer>> input) {
      return new ConfigGroup(input.getKey().newBuilder(), ImmutableSet.copyOf(input.getValue()));
    }
  };

  @Override
  public Response getPendingReason(TaskQuery query) throws TException {
    requireNonNull(query);

    Response response = Util.emptyResponse();
    if (query.isSetSlaveHosts() || query.isSetStatuses()) {
      return addMessage(
          response,
          INVALID_REQUEST,
          "Statuses or slaveHosts are not supported in " + query.toString());
    }

    // Only PENDING tasks should be considered.
    query.setStatuses(ImmutableSet.of(ScheduleStatus.PENDING));

    Set<PendingReason> reasons = FluentIterable.from(getTasks(query))
        .transform(new Function<ScheduledTask, PendingReason>() {
          @Override
          public PendingReason apply(ScheduledTask scheduledTask) {
            String taskId = scheduledTask.getAssignedTask().getTaskId();
            String reason = Joiner.on(',').join(Iterables.transform(
                nearestFit.getNearestFit(taskId),
                new Function<Veto, String>() {
                  @Override
                  public String apply(Veto veto) {
                    return veto.getReason();
                  }
                }));

            return new PendingReason()
                .setTaskId(taskId)
                .setReason(reason);
          }
        }).toSet();

    return response
        .setResponseCode(OK)
        .setResult(Result.getPendingReasonResult(new GetPendingReasonResult(reasons)));
  }

  @Override
  public Response getConfigSummary(JobKey job) throws TException {
    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(job));

    Set<IScheduledTask> activeTasks =
        Storage.Util.weaklyConsistentFetchTasks(storage, Query.jobScoped(jobKey).active());

    Iterable<IAssignedTask> assignedTasks =
        Iterables.transform(activeTasks, Tasks.SCHEDULED_TO_ASSIGNED);
    Map<Integer, ITaskConfig> tasksByInstance = Maps.transformValues(
        Maps.uniqueIndex(assignedTasks, Tasks.ASSIGNED_TO_INSTANCE_ID),
        Tasks.ASSIGNED_TO_INFO);
    Multimap<ITaskConfig, Integer> instancesByDetails = Multimaps.invertFrom(
        Multimaps.forMap(tasksByInstance),
        HashMultimap.<ITaskConfig, Integer>create());
    Iterable<ConfigGroup> groups = Iterables.transform(
        instancesByDetails.asMap().entrySet(), CONFIG_TO_GROUP);

    ConfigSummary summary = new ConfigSummary(job, ImmutableSet.copyOf(groups));
    return okResponse(Result.configSummaryResult(new ConfigSummaryResult().setSummary(summary)));
  }

  @Override
  public Response getRoleSummary() {
    Multimap<String, IJobKey> jobsByRole = mapByRole(
        Storage.Util.weaklyConsistentFetchTasks(storage, Query.unscoped()),
        Tasks.SCHEDULED_TO_JOB_KEY);

    Multimap<String, IJobKey> cronJobsByRole = mapByRole(
        cronJobManager.getJobs(),
        JobKeys.FROM_CONFIG);

    Set<RoleSummary> summaries = Sets.newHashSet();
    for (String role : Sets.union(jobsByRole.keySet(), cronJobsByRole.keySet())) {
      RoleSummary summary = new RoleSummary();
      summary.setRole(role);
      summary.setJobCount(jobsByRole.get(role).size());
      summary.setCronJobCount(cronJobsByRole.get(role).size());
      summaries.add(summary);
    }

    return okResponse(Result.roleSummaryResult(new RoleSummaryResult(summaries)));
  }

  @Override
  public Response getJobSummary(@Nullable String maybeNullRole) {
    Optional<String> ownerRole = Optional.fromNullable(maybeNullRole);

    final Multimap<IJobKey, IScheduledTask> tasks = getTasks(maybeRoleScoped(ownerRole));
    final Map<IJobKey, IJobConfiguration> jobs = getJobs(ownerRole, tasks);

    Function<IJobKey, JobSummary> makeJobSummary = new Function<IJobKey, JobSummary>() {
      @Override
      public JobSummary apply(IJobKey jobKey) {
        IJobConfiguration job = jobs.get(jobKey);
        JobSummary summary = new JobSummary()
            .setJob(job.newBuilder())
            .setStats(Jobs.getJobStats(tasks.get(jobKey)).newBuilder());

        return Strings.isNullOrEmpty(job.getCronSchedule())
            ? summary
            : summary.setNextCronRunMs(
                cronPredictor.predictNextRun(CrontabEntry.parse(job.getCronSchedule())).getTime());
      }
    };

    ImmutableSet<JobSummary> jobSummaries =
        FluentIterable.from(jobs.keySet()).transform(makeJobSummary).toSet();

    return okResponse(Result.jobSummaryResult(new JobSummaryResult().setSummaries(jobSummaries)));
  }

  private Query.Builder maybeRoleScoped(Optional<String> ownerRole) {
    return ownerRole.isPresent()
        ? Query.roleScoped(ownerRole.get())
        : Query.unscoped();
  }

  private Map<IJobKey, IJobConfiguration> getJobs(
      Optional<String> ownerRole,
      Multimap<IJobKey, IScheduledTask> tasks) {

    // We need to synthesize the JobConfiguration from the the current tasks because the
    // ImmediateJobManager doesn't store jobs directly and ImmediateJobManager#getJobs always
    // returns an empty Collection.
    Map<IJobKey, IJobConfiguration> jobs = Maps.newHashMap();

    jobs.putAll(Maps.transformEntries(tasks.asMap(),
        new Maps.EntryTransformer<IJobKey, Collection<IScheduledTask>, IJobConfiguration>() {
          @Override
          public IJobConfiguration transformEntry(
              IJobKey jobKey,
              Collection<IScheduledTask> tasks) {

            // Pick the latest transitioned task for each immediate job since the job can be in the
            // middle of an update or some shards have been selectively created.
            TaskConfig mostRecentTaskConfig =
                Tasks.getLatestActiveTask(tasks).getAssignedTask().getTask().newBuilder();

            return IJobConfiguration.build(new JobConfiguration()
                .setKey(jobKey.newBuilder())
                .setOwner(mostRecentTaskConfig.getOwner())
                .setTaskConfig(mostRecentTaskConfig)
                .setInstanceCount(tasks.size()));
          }
        }));

    // Get cron jobs directly from the manager. Do this after querying the task store so the real
    // template JobConfiguration for a cron job will overwrite the synthesized one that could have
    // been created above.
    Predicate<IJobConfiguration> configFilter = ownerRole.isPresent()
        ? Predicates.compose(Predicates.equalTo(ownerRole.get()), JobKeys.CONFIG_TO_ROLE)
        : Predicates.<IJobConfiguration>alwaysTrue();
    jobs.putAll(Maps.uniqueIndex(
        FluentIterable.from(cronJobManager.getJobs()).filter(configFilter),
        JobKeys.FROM_CONFIG));

    return jobs;
  }

  private Multimap<IJobKey, IScheduledTask> getTasks(Query.Builder query) {
    return Tasks.byJobKey(Storage.Util.weaklyConsistentFetchTasks(storage, query));
  }

  private static <T> Multimap<String, IJobKey> mapByRole(
      Iterable<T> tasks,
      Function<T, IJobKey> keyExtractor) {

    return HashMultimap.create(
        Multimaps.index(Iterables.transform(tasks, keyExtractor), JobKeys.TO_ROLE));
  }

  @Override
  public Response getJobs(@Nullable String maybeNullRole) {
    Optional<String> ownerRole = Optional.fromNullable(maybeNullRole);

    return okResponse(Result.getJobsResult(
        new GetJobsResult()
            .setConfigs(IJobConfiguration.toBuildersSet(
                getJobs(ownerRole, getTasks(maybeRoleScoped(ownerRole).active())).values()))));
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

  private SessionContext validateSessionKeyForTasks(
      SessionKey session,
      Query.Builder taskQuery,
      Iterable<IScheduledTask> tasks) throws AuthFailedException {

    // Authenticate the session against any affected roles, always including the role for a
    // role-scoped query.  This papers over the implementation detail that dormant cron jobs are
    // authenticated this way.
    ImmutableSet.Builder<String> targetRoles = ImmutableSet.<String>builder()
        .addAll(FluentIterable.from(tasks).transform(GET_ROLE));
    if (taskQuery.get().isSetOwner()) {
      targetRoles.add(taskQuery.get().getOwner().getRole());
    }
    return sessionValidator.checkAuthenticated(session, targetRoles.build());
  }

  private Optional<SessionContext> isAdmin(SessionKey session) {
    try {
      return Optional.of(
          sessionValidator.checkAuthorized(session, Capability.ROOT, AuditCheck.REQUIRED));
    } catch (AuthFailedException e) {
      return Optional.absent();
    }
  }

  @Override
  public Response killTasks(
      final TaskQuery mutableQuery,
      final Lock mutableLock,
      final SessionKey session) {

    requireNonNull(mutableQuery);
    requireNonNull(session);

    final Response response = emptyResponse();

    if (mutableQuery.getJobName() != null && StringUtils.isBlank(mutableQuery.getJobName())) {
      return addMessage(
          response,
          INVALID_REQUEST,
          String.format("Invalid job name: '%s'", mutableQuery.getJobName()));
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        Query.Builder query = Query.arbitrary(mutableQuery);

        // Check single job scoping before adding statuses.
        boolean isSingleJobScoped = Query.isSingleJobScoped(query);

        // Unless statuses were specifically supplied, only attempt to kill active tasks.
        query = query.get().isSetStatuses() ? query : query.byStatus(ACTIVE_STATES);

        final Set<IScheduledTask> tasks = storeProvider.getTaskStore().fetchTasks(query);

        Optional<SessionContext> context = isAdmin(session);
        if (context.isPresent()) {
          LOG.info("Granting kill query to admin user: " + query);
        } else {
          try {
            context = Optional.of(validateSessionKeyForTasks(session, query, tasks));
          } catch (AuthFailedException e) {
            return addMessage(response, AUTH_FAILED, e);
          }
        }

        try {
          validateLockForTasks(
              Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER),
              tasks);
        } catch (LockException e) {
          return addMessage(response, LOCK_ERROR, e);
        }

        LOG.info("Killing tasks matching " + query);

        boolean tasksKilled = false;
        if (isSingleJobScoped) {
          // If this looks like a query for all tasks in a job, instruct the cron
          // scheduler to delete it.
          // TODO(mchucarroll): deprecate cron as a part of create/kill job.  (AURORA-454)
          IJobKey jobKey = Iterables.getOnlyElement(JobKeys.from(query).get());
          LOG.warning("Deprecated behavior: descheduling job " + jobKey
              + " with cron via killTasks. (See AURORA-454)");
          tasksKilled = cronJobManager.deleteJob(jobKey);
        }

        for (String taskId : Tasks.ids(tasks)) {
          tasksKilled |= stateManager.changeState(
              taskId,
              Optional.<ScheduleStatus>absent(),
              ScheduleStatus.KILLING,
              killedByMessage(context.get().getIdentity()));
        }

        return tasksKilled ? okEmptyResponse() : addMessage(response, OK, NO_TASKS_TO_KILL_MESSAGE);
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
    final Response response = emptyResponse();

    final SessionContext context;
    try {
      context = sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
    } catch (AuthFailedException e) {
      return addMessage(response, AUTH_FAILED, e);
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        try {
          lockManager.validateIfLocked(
              ILockKey.build(LockKey.job(jobKey.newBuilder())),
              Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));
        } catch (LockException e) {
          return addMessage(response, LOCK_ERROR, e);
        }

        Query.Builder query = Query.instanceScoped(jobKey, shardIds).active();
        Set<IScheduledTask> matchingTasks = storeProvider.getTaskStore().fetchTasks(query);
        if (matchingTasks.size() != shardIds.size()) {
          return addMessage(response, INVALID_REQUEST, "Not all requested shards are active.");
        }

        LOG.info("Restarting shards matching " + query);
        for (String taskId : Tasks.ids(matchingTasks)) {
          stateManager.changeState(
              taskId,
              Optional.<ScheduleStatus>absent(),
              ScheduleStatus.RESTARTING,
              restartedByMessage(context.getIdentity()));
        }
        return response.setResponseCode(OK);
      }
    });
  }

  @Override
  public Response getQuota(final String ownerRole) {
    checkNotBlank(ownerRole);

    QuotaInfo quotaInfo = quotaManager.getQuotaInfo(ownerRole);
    GetQuotaResult result = new GetQuotaResult(quotaInfo.guota().newBuilder())
        .setProdConsumption(quotaInfo.getProdConsumption().newBuilder())
        .setNonProdConsumption(quotaInfo.getNonProdConsumption().newBuilder());

    return okResponse(Result.getQuotaResult(result));
  }

  @Requires(whitelist = Capability.PROVISIONER)
  @Override
  public Response setQuota(
      final String ownerRole,
      final ResourceAggregate resourceAggregate,
      SessionKey session) {

    checkNotBlank(ownerRole);
    requireNonNull(resourceAggregate);
    requireNonNull(session);

    Response response = Util.emptyResponse();
    try {
      quotaManager.saveQuota(ownerRole, IResourceAggregate.build(resourceAggregate));
      return response.setResponseCode(OK);
    } catch (QuotaException e) {
      return addMessage(response, INVALID_REQUEST, e);
    }
  }

  @Requires(whitelist = Capability.MACHINE_MAINTAINER)
  @Override
  public Response startMaintenance(Hosts hosts, SessionKey session) {
    return okResponse(Result.startMaintenanceResult(
        new StartMaintenanceResult()
            .setStatuses(maintenance.startMaintenance(hosts.getHostNames()))));
  }

  @Requires(whitelist = Capability.MACHINE_MAINTAINER)
  @Override
  public Response drainHosts(Hosts hosts, SessionKey session) {
    return okResponse(Result.drainHostsResult(
        new DrainHostsResult().setStatuses(maintenance.drain(hosts.getHostNames()))));
  }

  @Requires(whitelist = Capability.MACHINE_MAINTAINER)
  @Override
  public Response maintenanceStatus(Hosts hosts, SessionKey session) {
    return okResponse(Result.maintenanceStatusResult(
        new MaintenanceStatusResult().setStatuses(maintenance.getStatus(hosts.getHostNames()))));
  }

  @Requires(whitelist = Capability.MACHINE_MAINTAINER)
  @Override
  public Response endMaintenance(Hosts hosts, SessionKey session) {
    return okResponse(Result.endMaintenanceResult(
        new EndMaintenanceResult()
            .setStatuses(maintenance.endMaintenance(hosts.getHostNames()))));
  }

  @Override
  public Response forceTaskState(String taskId, ScheduleStatus status, SessionKey session) {
    checkNotBlank(taskId);
    requireNonNull(status);
    requireNonNull(session);

    SessionContext context;
    try {
      // TODO(Sathya): Remove this after AOP-style session validation passes in a SessionContext.
      context = sessionValidator.checkAuthorized(session, Capability.ROOT, AuditCheck.REQUIRED);
    } catch (AuthFailedException e) {
      return addMessage(emptyResponse(), AUTH_FAILED, e);
    }

    stateManager.changeState(
        taskId,
        Optional.<ScheduleStatus>absent(),
        status,
        transitionMessage(context.getIdentity()));

    return okEmptyResponse();
  }

  @Override
  public Response performBackup(SessionKey session) {
    backup.backupNow();
    return okEmptyResponse();
  }

  @Override
  public Response listBackups(SessionKey session) {
    return okResponse(Result.listBackupsResult(new ListBackupsResult()
        .setBackups(recovery.listBackups())));
  }

  @Override
  public Response stageRecovery(String backupId, SessionKey session) {
    Response response = okEmptyResponse();
    try {
      recovery.stage(backupId);
    } catch (RecoveryException e) {
      addMessage(response, ERROR, e);
      LOG.log(Level.WARNING, "Failed to stage recovery: " + e, e);
    }

    return response;
  }

  @Override
  public Response queryRecovery(TaskQuery query, SessionKey session) {
    Response response = Util.emptyResponse();
    try {
      response.setResponseCode(OK)
          .setResult(Result.queryRecoveryResult(new QueryRecoveryResult()
              .setTasks(IScheduledTask.toBuildersSet(recovery.query(Query.arbitrary(query))))));
    } catch (RecoveryException e) {
      addMessage(response, ERROR, e);
      LOG.log(Level.WARNING, "Failed to query recovery: " + e, e);
    }

    return response;
  }

  @Override
  public Response deleteRecoveryTasks(TaskQuery query, SessionKey session) {
    Response response = okEmptyResponse();
    try {
      recovery.deleteTasks(Query.arbitrary(query));
    } catch (RecoveryException e) {
      addMessage(response, ERROR, e);
      LOG.log(Level.WARNING, "Failed to delete recovery tasks: " + e, e);
    }

    return response;
  }

  @Override
  public Response commitRecovery(SessionKey session) {
    Response response = okEmptyResponse();
    try {
      recovery.commit();
    } catch (RecoveryException e) {
      addMessage(response, ERROR, e);
    }

    return response;
  }

  @Override
  public Response unloadRecovery(SessionKey session) {
    recovery.unload();
    return okEmptyResponse();
  }

  @Override
  public Response snapshot(SessionKey session) {
    Response response = Util.emptyResponse();
    try {
      storage.snapshot();
      return response.setResponseCode(OK);
    } catch (Storage.StorageException e) {
      LOG.log(Level.WARNING, "Requested snapshot failed.", e);
      return addMessage(response, ERROR, e);
    }
  }

  private static Multimap<String, IJobConfiguration> jobsByKey(JobStore jobStore, IJobKey jobKey) {
    ImmutableMultimap.Builder<String, IJobConfiguration> matches = ImmutableMultimap.builder();
    for (String managerId : jobStore.fetchManagerIds()) {
      for (IJobConfiguration job : jobStore.fetchJobs(managerId)) {
        if (job.getKey().equals(jobKey)) {
          matches.put(managerId, job);
        }
      }
    }
    return matches.build();
  }

  @Override
  public Response rewriteConfigs(
      final RewriteConfigsRequest request,
      SessionKey session) {

    if (request.getRewriteCommandsSize() == 0) {
      return addMessage(Util.emptyResponse(), ERROR, "No rewrite commands provided.");
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

        Response resp = emptyResponse();
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
      throw Throwables.propagate(e);
    }

    if (existingJob.getKey().equals(rewrittenJob.getKey())) {
      if (existingJob.getOwner().equals(rewrittenJob.getOwner())) {
        Multimap<String, IJobConfiguration> matches = jobsByKey(jobStore, existingJob.getKey());
        switch (matches.size()) {
          case 0:
            error = Optional.of(
                "No jobs found for key " + JobKeys.canonicalString(existingJob.getKey()));
            break;

          case 1:
            Map.Entry<String, IJobConfiguration> match =
                Iterables.getOnlyElement(matches.entries());
            IJobConfiguration storedJob = match.getValue();
            if (storedJob.equals(existingJob)) {
              jobStore.saveAcceptedJob(match.getKey(), rewrittenJob);
            } else {
              error = Optional.of(
                  "CAS compare failed for " + JobKeys.canonicalString(storedJob.getKey()));
            }
            break;

          default:
            error = Optional.of("Multiple jobs found for key "
                + JobKeys.canonicalString(existingJob.getKey()));
        }
      } else {
        error = Optional.of("Disallowing rewrite attempting to change job owner.");
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
  public Response getVersion() {
    return okResponse(Result.getVersionResult(CURRENT_API_VERSION));
  }

  @Override
  public Response addInstances(
      AddInstancesConfig config,
      @Nullable Lock mutableLock,
      SessionKey session) {

    requireNonNull(config);
    requireNonNull(session);
    checkNotBlank(config.getInstanceIds());
    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(config.getKey()));

    Response resp = Util.emptyResponse();
    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
      ITaskConfig task = ConfigurationManager.validateAndPopulate(
          ITaskConfig.build(config.getTaskConfig()));

      if (cronJobManager.hasJob(jobKey)) {
        return addMessage(resp, INVALID_REQUEST, "Instances may not be added to cron jobs.");
      }

      lockManager.validateIfLocked(
          ILockKey.build(LockKey.job(jobKey.newBuilder())),
          Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

      schedulerCore.addInstances(jobKey, ImmutableSet.copyOf(config.getInstanceIds()), task);
      return resp.setResponseCode(OK);
    } catch (AuthFailedException e) {
      return addMessage(resp, AUTH_FAILED, e);
    } catch (LockException e) {
      return addMessage(resp, LOCK_ERROR, e);
    } catch (TaskDescriptionException | ScheduleException e) {
      return addMessage(resp, INVALID_REQUEST, e);
    }
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
    Response response = Util.emptyResponse();

    try {
      SessionContext context = sessionValidator.checkAuthenticated(
          session,
          ImmutableSet.of(getRoleFromLockKey(lockKey)));

      ILock lock = lockManager.acquireLock(lockKey, context.getIdentity());
      response.setResult(Result.acquireLockResult(
          new AcquireLockResult().setLock(lock.newBuilder())));

      return response.setResponseCode(OK);
    } catch (AuthFailedException e) {
      return addMessage(response, AUTH_FAILED, e);
    } catch (LockException e) {
      return addMessage(response, LOCK_ERROR, e);
    }
  }

  @Override
  public Response releaseLock(Lock mutableLock, LockValidation validation, SessionKey session) {
    requireNonNull(mutableLock);
    requireNonNull(validation);
    requireNonNull(session);

    Response response = Util.emptyResponse();
    ILock lock = ILock.build(mutableLock);

    try {
      sessionValidator.checkAuthenticated(
          session,
          ImmutableSet.of(getRoleFromLockKey(lock.getKey())));

      if (validation == LockValidation.CHECKED) {
        lockManager.validateIfLocked(lock.getKey(), Optional.of(lock));
      }
      lockManager.releaseLock(lock);
      return response.setResponseCode(OK);
    } catch (AuthFailedException e) {
      return addMessage(response, AUTH_FAILED, e);
    } catch (LockException e) {
      return addMessage(response, LOCK_ERROR, e);
    }
  }

  @Override
  public Response getLocks() {
    return okResponse(Result.getLocksResult(
        new GetLocksResult().setLocks(ILock.toBuildersSet(lockManager.getLocks()))));
  }

  private static final Function<Collection<Integer>, Set<Range<Integer>>> TO_RANGES  =
      new Function<Collection<Integer>, Set<Range<Integer>>>() {
        @Override
        public Set<Range<Integer>> apply(Collection<Integer> numbers) {
          return Numbers.toRanges(numbers);
        }
      };

  private static Set<InstanceTaskConfig> buildOldTaskConfigs(
      IJobKey jobKey,
      Storage.StoreProvider storeProvider) {

    Set<IScheduledTask> tasks =
        storeProvider.getTaskStore().fetchTasks(Query.jobScoped(jobKey).active());

    // Group tasks by their configurations.
    Multimap<ITaskConfig, IScheduledTask> tasksByConfig =
        Multimaps.index(tasks, Tasks.SCHEDULED_TO_INFO);

    // Translate tasks into instance IDs.
    Multimap<ITaskConfig, Integer> instancesByConfig =
        Multimaps.transformValues(tasksByConfig, Tasks.SCHEDULED_TO_INSTANCE_ID);

    // Reduce instance IDs into contiguous ranges.
    Map<ITaskConfig, Set<Range<Integer>>> rangesByConfig =
        Maps.transformValues(instancesByConfig.asMap(), TO_RANGES);

    ImmutableSet.Builder<InstanceTaskConfig> builder = ImmutableSet.builder();
    for (Map.Entry<ITaskConfig, Set<Range<Integer>>> entry : rangesByConfig.entrySet()) {
      ImmutableSet.Builder<org.apache.aurora.gen.Range> ranges = ImmutableSet.builder();
      for (Range<Integer> range : entry.getValue()) {
        ranges.add(new org.apache.aurora.gen.Range(range.lowerEndpoint(), range.upperEndpoint()));
      }

      builder.add(new InstanceTaskConfig()
          .setTask(entry.getKey().newBuilder())
          .setInstances(ranges.build()));
    }

    return builder.build();
  }

  @Override
  public Response startJobUpdate(
      JobUpdateRequest mutableRequest,
      SessionKey session) {

    // TODO(maxim): validate JobUpdateRequest fields.
    requireNonNull(mutableRequest);
    requireNonNull(session);

    final Response response = emptyResponse();

    final SessionContext context;
    final IJobUpdateRequest request;
    try {
      context = sessionValidator.checkAuthenticated(
          session,
          ImmutableSet.of(mutableRequest.getJobKey().getRole()));

      request = IJobUpdateRequest.build(new JobUpdateRequest(mutableRequest).setTaskConfig(
          ConfigurationManager.validateAndPopulate(
              ITaskConfig.build(mutableRequest.getTaskConfig())).newBuilder()));

    } catch (AuthFailedException e) {
      return addMessage(response, AUTH_FAILED, e);
    } catch (TaskDescriptionException e) {
      return addMessage(response, INVALID_REQUEST, e);
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        final ILock lock;
        try {
          lock = lockManager.acquireLock(
              ILockKey.build(LockKey.job(request.getJobKey().newBuilder())),
              context.getIdentity());
        } catch (LockException e) {
          return addMessage(response, LOCK_ERROR, e);
        }

        // TODO(maxim): Wire in task limits and quota checks from SchedulerCore.

        String updateId = uuidGenerator.createNew().toString();

        IJobUpdate update = IJobUpdate.build(new JobUpdate()
            .setSummary(new JobUpdateSummary()
                .setJobKey(request.getJobKey().newBuilder())
                .setUpdateId(updateId)
                .setUser(context.getIdentity()))
            .setConfiguration(new JobUpdateConfiguration()
                .setSettings(request.getSettings().newBuilder())
                .setInstanceCount(request.getInstanceCount())
                .setNewTaskConfig(request.getTaskConfig().newBuilder())
                .setOldTaskConfigs(buildOldTaskConfigs(request.getJobKey(), storeProvider))));

        try {
          jobUpdateController.start(update, lock.getToken());
          return okResponse(Result.startJobUpdateResult(new StartJobUpdateResult(updateId)));
        } catch (UpdateStateException e) {
          return addMessage(response, INVALID_REQUEST, e);
        }
      }
    });
  }

  @Override
  public Response pauseJobUpdate(final JobKey mutableJobKey, final SessionKey session) {
    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        try {
          IJobKey jobKey = JobKeys.assertValid(IJobKey.build(requireNonNull(mutableJobKey)));
          sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
          jobUpdateController.pause(jobKey);
          return okEmptyResponse();
        } catch (AuthFailedException e) {
          return addMessage(emptyResponse(), AUTH_FAILED, e);
        } catch (UpdateStateException e) {
          return addMessage(emptyResponse(), INVALID_REQUEST, e);
        }
      }
    });
  }

  @Override
  public Response resumeJobUpdate(final JobKey mutableJobKey, final SessionKey session) {
    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        try {
          IJobKey jobKey = JobKeys.assertValid(IJobKey.build(requireNonNull(mutableJobKey)));
          sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
          jobUpdateController.resume(jobKey);
          return okEmptyResponse();
        } catch (AuthFailedException e) {
          return addMessage(emptyResponse(), AUTH_FAILED, e);
        } catch (UpdateStateException e) {
          return addMessage(emptyResponse(), INVALID_REQUEST, e);
        }
      }
    });
  }

  @Override
  public Response abortJobUpdate(final JobKey mutableJobKey, final SessionKey session) {
    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        try {
          IJobKey jobKey = JobKeys.assertValid(IJobKey.build(requireNonNull(mutableJobKey)));
          sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
          jobUpdateController.abort(jobKey);
          return okEmptyResponse();
        } catch (AuthFailedException e) {
          return addMessage(emptyResponse(), AUTH_FAILED, e);
        } catch (UpdateStateException e) {
          return addMessage(emptyResponse(), INVALID_REQUEST, e);
        }
      }
    });
  }

  @Override
  public Response getJobUpdateSummaries(JobUpdateQuery updateQuery) throws TException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Response getJobUpdateDetails(String updateId) {
    throw new UnsupportedOperationException("Not implemented");
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
  static final String NO_TASKS_TO_KILL_MESSAGE = "No tasks to kill.";

  private static Response okEmptyResponse()  {
    return emptyResponse().setResponseCode(OK);
  }

  private static Response okResponse(Result result) {
    return okEmptyResponse().setResult(result);
  }
}
