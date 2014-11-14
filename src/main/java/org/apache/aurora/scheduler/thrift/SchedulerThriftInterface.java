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
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Qualifier;

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
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.Positive;

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
import org.apache.aurora.gen.GetJobUpdateDetailsResult;
import org.apache.aurora.gen.GetJobUpdateSummariesResult;
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
import org.apache.aurora.gen.JobUpdateInstructions;
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
import org.apache.aurora.gen.PendingReason;
import org.apache.aurora.gen.PopulateJobResult;
import org.apache.aurora.gen.QueryRecoveryResult;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
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
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Jobs;
import org.apache.aurora.scheduler.base.Numbers;
import org.apache.aurora.scheduler.base.Query;
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
import org.apache.aurora.scheduler.quota.QuotaCheckResult;
import org.apache.aurora.scheduler.quota.QuotaInfo;
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
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.Recovery.RecoveryException;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateRequest;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSettings;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
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
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
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
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.INSUFFICIENT_QUOTA;
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
  private final CronPredictor cronPredictor;
  private final QuotaManager quotaManager;
  private final NearestFit nearestFit;
  private final StateManager stateManager;
  private final TaskIdGenerator taskIdGenerator;
  private final UUIDGenerator uuidGenerator;
  private final JobUpdateController jobUpdateController;
  private final boolean isUpdaterEnabled;

  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface EnableUpdater { }

  @Inject
  SchedulerThriftInterface(
      NonVolatileStorage storage,
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
      TaskIdGenerator taskIdGenerator,
      UUIDGenerator uuidGenerator,
      JobUpdateController jobUpdateController,
      @EnableUpdater boolean isUpdaterEnabled) {

    this.storage = requireNonNull(storage);
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
    this.taskIdGenerator = requireNonNull(taskIdGenerator);
    this.uuidGenerator = requireNonNull(uuidGenerator);
    this.jobUpdateController = requireNonNull(jobUpdateController);
    this.isUpdaterEnabled = isUpdaterEnabled;
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
      return errorResponse(AUTH_FAILED, e);
    } catch (TaskDescriptionException e) {
      return errorResponse(INVALID_REQUEST, e);
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        final IJobConfiguration job = sanitized.getJobConfig();

        try {
          lockManager.validateIfLocked(
              ILockKey.build(LockKey.job(job.getKey().newBuilder())),
              Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

          if (!storeProvider.getTaskStore().fetchTasks(
              Query.jobScoped(job.getKey()).active()).isEmpty()
              || cronJobManager.hasJob(job.getKey())) {

            return invalidResponse("Job already exists: " + JobKeys.canonicalString(job.getKey()));
          }

          ITaskConfig template = sanitized.getJobConfig().getTaskConfig();
          int count = sanitized.getJobConfig().getInstanceCount();

          validateTaskLimits(template, count, quotaManager.checkInstanceAddition(template, count));

          // TODO(mchucarroll): deprecate cron as a part of create/kill job.(AURORA-454)
          if (sanitized.isCron()) {
            LOG.warning("Deprecated behavior: scheduling job " + job.getKey()
                + " with cron via createJob (AURORA_454)");
            cronJobManager.createJob(SanitizedCronJob.from(sanitized));
          } else {
            LOG.info("Launching " + count + " tasks.");
            stateManager.insertPendingTasks(
                storeProvider,
                template,
                sanitized.getInstanceIds());
          }
          return okEmptyResponse();
        } catch (LockException e) {
          return errorResponse(LOCK_ERROR, e);
        } catch (CronException | TaskValidationException e) {
          return errorResponse(INVALID_REQUEST, e);
        }
      }
    });
  }

  @Override
  public Response scheduleCronJob(
      JobConfiguration mutableJob,
      @Nullable Lock mutableLock,
      SessionKey session) {

    IJobConfiguration job = IJobConfiguration.build(mutableJob);
    IJobKey jobKey = JobKeys.assertValid(job.getKey());
    requireNonNull(session);

    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
    } catch (AuthFailedException e) {
      return errorResponse(AUTH_FAILED, e);
    }

    try {
      SanitizedConfiguration sanitized = SanitizedConfiguration.fromUnsanitized(job);

      lockManager.validateIfLocked(
          ILockKey.build(LockKey.job(jobKey.newBuilder())),
          Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

      if (!sanitized.isCron()) {
        return invalidResponse(noCronScheduleMessage(jobKey));
      }

      ITaskConfig template = sanitized.getJobConfig().getTaskConfig();
      int count = sanitized.getJobConfig().getInstanceCount();

      validateTaskLimits(template, count, quotaManager.checkInstanceAddition(template, count));

      // TODO(mchucarroll): Merge CronJobManager.createJob/updateJob
      if (cronJobManager.hasJob(sanitized.getJobConfig().getKey())) {
        // The job already has a schedule: so update it.
        cronJobManager.updateJob(SanitizedCronJob.from(sanitized));
      } else {
        cronJobManager.createJob(SanitizedCronJob.from(sanitized));
      }

      return okEmptyResponse();
    } catch (LockException e) {
      return errorResponse(LOCK_ERROR, e);
    } catch (TaskDescriptionException | TaskValidationException | CronException e) {
      return errorResponse(INVALID_REQUEST, e);
    }
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
        return invalidResponse(notScheduledCronMessage(jobKey));
      }
      return okEmptyResponse();
    } catch (AuthFailedException e) {
      return errorResponse(AUTH_FAILED, e);
    } catch (LockException e) {
      return errorResponse(LOCK_ERROR, e);
    }
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

    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
    } catch (AuthFailedException e) {
      return errorResponse(AUTH_FAILED, e);
    }

    try {
      lockManager.validateIfLocked(
          ILockKey.build(LockKey.job(jobKey.newBuilder())),
          Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

      cronJobManager.updateJob(SanitizedCronJob.fromUnsanitized(job));
      return okEmptyResponse();
    } catch (LockException e) {
      return errorResponse(LOCK_ERROR, e);
    } catch (CronException | TaskDescriptionException e) {
      return errorResponse(INVALID_REQUEST, e);
    }
  }

  @Override
  public Response populateJobConfig(JobConfiguration description) {
    requireNonNull(description);

    try {
      ITaskConfig populatedTaskConfig = SanitizedConfiguration.fromUnsanitized(
          IJobConfiguration.build(description)).getJobConfig().getTaskConfig();

      PopulateJobResult result = new PopulateJobResult()
          .setPopulatedDEPRECATED(ImmutableSet.of(populatedTaskConfig.newBuilder()))
          .setTaskConfig(populatedTaskConfig.newBuilder());

      return okResponse(Result.populateJobResult(result));
    } catch (TaskDescriptionException e) {
      return invalidResponse("Invalid configuration: " + e.getMessage());
    }
  }

  @Override
  public Response startCronJob(JobKey mutableJobKey, SessionKey session) {
    requireNonNull(session);
    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));

    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
    } catch (AuthFailedException e) {
      return errorResponse(AUTH_FAILED, e);
    }

    try {
      cronJobManager.startJobNow(jobKey);
      return okEmptyResponse();
    } catch (CronException e) {
      return invalidResponse("Failed to start cron job - " + e.getMessage());
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
            task.getAssignedTask().getTask().unsetExecutorConfig();
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

    if (query.isSetSlaveHosts() || query.isSetStatuses()) {
      return invalidResponse("Statuses or slaveHosts are not supported in " + query.toString());
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

    return okResponse(Result.getPendingReasonResult(new GetPendingReasonResult(reasons)));
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
    Set<IJobKey> keys = JobKeys.from(taskQuery).or(ImmutableSet.<IJobKey>of());
    targetRoles.addAll(FluentIterable.from(keys).transform(JobKeys.TO_ROLE));

    if (taskQuery.get().isSetRole()) {
      targetRoles.add(taskQuery.get().getRole());
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

    if (mutableQuery.getJobName() != null && StringUtils.isBlank(mutableQuery.getJobName())) {
      return invalidResponse(String.format("Invalid job name: '%s'", mutableQuery.getJobName()));
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

        Optional<SessionContext> maybeAdminContext = isAdmin(session);
        final SessionContext context;
        if (maybeAdminContext.isPresent()) {
          LOG.info("Granting kill query to admin user: " + query);
          context = maybeAdminContext.get();
        } else {
          try {
            context = validateSessionKeyForTasks(session, query, tasks);
          } catch (AuthFailedException e) {
            return errorResponse(AUTH_FAILED, e);
          }
        }

        try {
          validateLockForTasks(
              Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER),
              tasks);
        } catch (LockException e) {
          return errorResponse(LOCK_ERROR, e);
        }

        LOG.info("Killing tasks matching " + query);

        final boolean cronJobKilled;
        if (isSingleJobScoped) {
          // If this looks like a query for all tasks in a job, instruct the cron
          // scheduler to delete it.
          // TODO(mchucarroll): deprecate cron as a part of create/kill job.  (AURORA-454)
          IJobKey jobKey = Iterables.getOnlyElement(JobKeys.from(query).get());
          LOG.warning("Deprecated behavior: descheduling job " + jobKey
              + " with cron via killTasks. (See AURORA-454)");
          cronJobKilled = cronJobManager.deleteJob(jobKey);
        } else {
          cronJobKilled = false;
        }

        final boolean tasksKilled = storage.write(new MutateWork.Quiet<Boolean>() {
          @Override
          public Boolean apply(MutableStoreProvider storeProvider) {
            boolean match = false;
            for (String taskId : Tasks.ids(tasks)) {
              match |= stateManager.changeState(
                  storeProvider,
                  taskId,
                  Optional.<ScheduleStatus>absent(),
                  ScheduleStatus.KILLING,
                  killedByMessage(context.getIdentity()));
            }
            return match;
          }
        });

        return cronJobKilled || tasksKilled
            ? okEmptyResponse()
            : addMessage(emptyResponse(), OK, NO_TASKS_TO_KILL_MESSAGE);
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
      return errorResponse(AUTH_FAILED, e);
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        try {
          lockManager.validateIfLocked(
              ILockKey.build(LockKey.job(jobKey.newBuilder())),
              Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));
        } catch (LockException e) {
          return errorResponse(LOCK_ERROR, e);
        }

        Query.Builder query = Query.instanceScoped(jobKey, shardIds).active();
        final Set<IScheduledTask> matchingTasks = storeProvider.getTaskStore().fetchTasks(query);
        if (matchingTasks.size() != shardIds.size()) {
          return invalidResponse("Not all requested shards are active.");
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
        return okEmptyResponse();
      }
    });
  }

  @Override
  public Response getQuota(final String ownerRole) {
    checkNotBlank(ownerRole);

    QuotaInfo quotaInfo = quotaManager.getQuotaInfo(ownerRole);
    GetQuotaResult result = new GetQuotaResult(quotaInfo.getQuota().newBuilder())
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

    try {
      quotaManager.saveQuota(ownerRole, IResourceAggregate.build(resourceAggregate));
      return okEmptyResponse();
    } catch (QuotaException e) {
      return errorResponse(INVALID_REQUEST, e);
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
  public Response forceTaskState(
      final String taskId,
      final ScheduleStatus status,
      SessionKey session) {

    checkNotBlank(taskId);
    requireNonNull(status);
    requireNonNull(session);

    final SessionContext context;
    try {
      // TODO(Sathya): Remove this after AOP-style session validation passes in a SessionContext.
      context = sessionValidator.checkAuthorized(session, Capability.ROOT, AuditCheck.REQUIRED);
    } catch (AuthFailedException e) {
      return errorResponse(AUTH_FAILED, e);
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
    try {
      recovery.stage(backupId);
      return okEmptyResponse();
    } catch (RecoveryException e) {
      LOG.log(Level.WARNING, "Failed to stage recovery: " + e, e);
      return errorResponse(ERROR, e);
    }
  }

  @Override
  public Response queryRecovery(TaskQuery query, SessionKey session) {
    try {
      return okResponse(Result.queryRecoveryResult(new QueryRecoveryResult()
              .setTasks(IScheduledTask.toBuildersSet(recovery.query(Query.arbitrary(query))))));
    } catch (RecoveryException e) {
      LOG.log(Level.WARNING, "Failed to query recovery: " + e, e);
      return errorResponse(ERROR, e);
    }
  }

  @Override
  public Response deleteRecoveryTasks(TaskQuery query, SessionKey session) {
    try {
      recovery.deleteTasks(Query.arbitrary(query));
      return okEmptyResponse();
    } catch (RecoveryException e) {
      LOG.log(Level.WARNING, "Failed to delete recovery tasks: " + e, e);
      return errorResponse(ERROR, e);
    }
  }

  @Override
  public Response commitRecovery(SessionKey session) {
    try {
      recovery.commit();
      return okEmptyResponse();
    } catch (RecoveryException e) {
      return errorResponse(ERROR, e);
    }
  }

  @Override
  public Response unloadRecovery(SessionKey session) {
    recovery.unload();
    return okEmptyResponse();
  }

  @Override
  public Response snapshot(SessionKey session) {
    try {
      storage.snapshot();
      return okEmptyResponse();
    } catch (Storage.StorageException e) {
      LOG.log(Level.WARNING, "Requested snapshot failed.", e);
      return errorResponse(ERROR, e);
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
      return errorResponse(AUTH_FAILED, e);
    } catch (TaskDescriptionException e) {
      return errorResponse(INVALID_REQUEST, e);
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        try {
          if (cronJobManager.hasJob(jobKey)) {
            return invalidResponse("Instances may not be added to cron jobs.");
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

          return okEmptyResponse();
        } catch (LockException e) {
          return errorResponse(LOCK_ERROR, e);
        } catch (TaskValidationException | IllegalArgumentException e) {
          return errorResponse(INVALID_REQUEST, e);
        }
      }
    });
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
      return okResponse(Result.acquireLockResult(
          new AcquireLockResult().setLock(lock.newBuilder())));
    } catch (AuthFailedException e) {
      return errorResponse(AUTH_FAILED, e);
    } catch (LockException e) {
      return errorResponse(LOCK_ERROR, e);
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
      return okEmptyResponse();
    } catch (AuthFailedException e) {
      return errorResponse(AUTH_FAILED, e);
    } catch (LockException e) {
      return errorResponse(LOCK_ERROR, e);
    }
  }

  @Override
  public Response getLocks() {
    return okResponse(Result.getLocksResult(
        new GetLocksResult().setLocks(ILock.toBuildersSet(lockManager.getLocks()))));
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
      return invalidResponse("Server-side updates are disabled on this cluster.");
    }

    requireNonNull(mutableRequest);
    requireNonNull(session);

    // TODO(maxim): Switch to key field instead when AURORA-749 is fixed.
    final IJobKey job = JobKeys.assertValid(IJobKey.build(new JobKey()
        .setRole(mutableRequest.getTaskConfig().getOwner().getRole())
        .setEnvironment(mutableRequest.getTaskConfig().getEnvironment())
        .setName(mutableRequest.getTaskConfig().getJobName())));

    JobUpdateSettings settings = requireNonNull(mutableRequest.getSettings());
    if (settings.getUpdateGroupSize() <= 0) {
      return invalidResponse("updateGroupSize must be positive.");
    }

    if (settings.getMaxPerInstanceFailures() < 0) {
      return invalidResponse("maxPerInstanceFailures must be non-negative.");
    }

    if (settings.getMaxFailedInstances() < 0) {
      return invalidResponse("maxFailedInstances must be non-negative.");
    }

    if (settings.getMaxWaitToInstanceRunningMs() < 0) {
      return invalidResponse("maxWaitToInstanceRunningMs must be non-negative.");
    }

    if (settings.getMinWaitInInstanceRunningMs() < 0) {
      return invalidResponse("minWaitInInstanceRunningMs must be non-negative.");
    }

    final SessionContext context;
    final IJobUpdateRequest request;
    try {
      context = sessionValidator.checkAuthenticated(session, ImmutableSet.of(job.getRole()));
      request = IJobUpdateRequest.build(new JobUpdateRequest(mutableRequest).setTaskConfig(
          ConfigurationManager.validateAndPopulate(
              ITaskConfig.build(mutableRequest.getTaskConfig())).newBuilder()));

      if (cronJobManager.hasJob(job)) {
        return invalidResponse("Cron jobs may only be updated by calling replaceCronTemplate.");
      }
    } catch (AuthFailedException e) {
      return errorResponse(AUTH_FAILED, e);
    } catch (TaskDescriptionException e) {
      return errorResponse(INVALID_REQUEST, e);
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override
      public Response apply(MutableStoreProvider storeProvider) {
        String updateId = uuidGenerator.createNew().toString();
        IJobUpdateSettings settings = request.getSettings();

        JobDiff diff = JobDiff.compute(
            storeProvider.getTaskStore(),
            job,
            JobDiff.asMap(request.getTaskConfig(), request.getInstanceCount()),
            settings.getUpdateOnlyTheseInstances());

        if (diff.isNoop()) {
          return addMessage(emptyResponse(), OK, NOOP_JOB_UPDATE_MESSAGE);
        }

        Set<Integer> invalidScope = diff.getOutOfScopeInstances(
            Numbers.rangesToInstanceIds(settings.getUpdateOnlyTheseInstances()));
        if (!invalidScope.isEmpty()) {
          return invalidResponse(
              "updateOnlyTheseInstances contains instances irrelevant to the update: "
                  + invalidScope);
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
          return okResponse(Result.startJobUpdateResult(new StartJobUpdateResult(updateId)));
        } catch (UpdateStateException | TaskValidationException e) {
          return errorResponse(INVALID_REQUEST, e);
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
          SessionContext context = sessionValidator
              .checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
          jobUpdateController.pause(jobKey, context.getIdentity());
          return okEmptyResponse();
        } catch (AuthFailedException e) {
          return errorResponse(AUTH_FAILED, e);
        } catch (UpdateStateException e) {
          return errorResponse(INVALID_REQUEST, e);
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
          SessionContext context = sessionValidator
              .checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
          jobUpdateController.resume(jobKey, context.getIdentity());
          return okEmptyResponse();
        } catch (AuthFailedException e) {
          return errorResponse(AUTH_FAILED, e);
        } catch (UpdateStateException e) {
          return errorResponse(INVALID_REQUEST, e);
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
          SessionContext context = sessionValidator
              .checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
          jobUpdateController.abort(jobKey, context.getIdentity());
          return okEmptyResponse();
        } catch (AuthFailedException e) {
          return errorResponse(AUTH_FAILED, e);
        } catch (UpdateStateException e) {
          return errorResponse(INVALID_REQUEST, e);
        }
      }
    });
  }

  @Override
  public Response pulseJobUpdate(String updateId, SessionKey session) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Response getJobUpdateSummaries(final JobUpdateQuery mutableQuery) {
    final IJobUpdateQuery query = IJobUpdateQuery.build(requireNonNull(mutableQuery));
    return okResponse(Result.getJobUpdateSummariesResult(
        new GetJobUpdateSummariesResult().setUpdateSummaries(IJobUpdateSummary.toBuildersList(
            storage.weaklyConsistentRead(new Work.Quiet<List<IJobUpdateSummary>>() {
              @Override
              public List<IJobUpdateSummary> apply(StoreProvider storeProvider) {
                return storeProvider.getJobUpdateStore().fetchJobUpdateSummaries(query);
              }
            })))));
  }

  @Override
  public Response getJobUpdateDetails(final String updateId) {
    requireNonNull(updateId);
    Optional<IJobUpdateDetails> details =
        storage.weaklyConsistentRead(new Work.Quiet<Optional<IJobUpdateDetails>>() {
          @Override
          public Optional<IJobUpdateDetails> apply(StoreProvider storeProvider) {
            return storeProvider.getJobUpdateStore().fetchJobUpdateDetails(updateId);
          }
        });

    if (details.isPresent()) {
      return okResponse(Result.getJobUpdateDetailsResult(
          new GetJobUpdateDetailsResult().setDetails(details.get().newBuilder())));
    } else {
      return invalidResponse("Invalid update ID:" + updateId);
    }
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
  static final String NO_TASKS_TO_KILL_MESSAGE = "No tasks to kill.";

  @VisibleForTesting
  static final String NOOP_JOB_UPDATE_MESSAGE = "Job is unchanged by proposed update.";

  private static Response okEmptyResponse()  {
    return emptyResponse().setResponseCode(OK);
  }

  private static Response invalidResponse(String message) {
    return addMessage(emptyResponse(), INVALID_REQUEST, message);
  }

  private static Response errorResponse(ResponseCode code, Throwable error) {
    return addMessage(emptyResponse(), code, error);
  }

  private static Response okResponse(Result result) {
    return okEmptyResponse().setResult(result);
  }
}
