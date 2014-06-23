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
import org.apache.aurora.gen.GetQuotaResult;
import org.apache.aurora.gen.Hosts;
import org.apache.aurora.gen.InstanceConfigRewrite;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.JobConfigRewrite;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobSummary;
import org.apache.aurora.gen.JobSummaryResult;
import org.apache.aurora.gen.ListBackupsResult;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.LockKey._Fields;
import org.apache.aurora.gen.LockValidation;
import org.apache.aurora.gen.MaintenanceStatusResult;
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
import org.apache.aurora.gen.StartMaintenanceResult;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Jobs;
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
import org.apache.aurora.scheduler.quota.QuotaInfo;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.quota.QuotaManager.QuotaException;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.state.LockManager.LockException;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.aurora.scheduler.state.SchedulerCore;
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
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.thrift.auth.DecoratedThrift;
import org.apache.aurora.scheduler.thrift.auth.Requires;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

import static org.apache.aurora.auth.SessionValidator.SessionContext;
import static org.apache.aurora.gen.ResponseCode.AUTH_FAILED;
import static org.apache.aurora.gen.ResponseCode.ERROR;
import static org.apache.aurora.gen.ResponseCode.INVALID_REQUEST;
import static org.apache.aurora.gen.ResponseCode.LOCK_ERROR;
import static org.apache.aurora.gen.ResponseCode.OK;
import static org.apache.aurora.gen.ResponseCode.WARNING;
import static org.apache.aurora.gen.apiConstants.CURRENT_API_VERSION;
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
      QuotaManager quotaManager) {

    this(storage,
        schedulerCore,
        lockManager,
        sessionValidator,
        backup,
        recovery,
        maintenance,
        cronJobManager,
        cronPredictor,
        quotaManager);
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
      QuotaManager quotaManager) {

    this.storage = checkNotNull(storage);
    this.schedulerCore = checkNotNull(schedulerCore);
    this.lockManager = checkNotNull(lockManager);
    this.sessionValidator = checkNotNull(sessionValidator);
    this.backup = checkNotNull(backup);
    this.recovery = checkNotNull(recovery);
    this.maintenance = checkNotNull(maintenance);
    this.cronJobManager = checkNotNull(cronJobManager);
    this.cronPredictor = checkNotNull(cronPredictor);
    this.quotaManager = checkNotNull(quotaManager);
  }

  @Override
  public Response createJob(
      JobConfiguration mutableJob,
      @Nullable Lock mutableLock,
      SessionKey session) {

    IJobConfiguration job = IJobConfiguration.build(mutableJob);
    IJobKey jobKey = JobKeys.assertValid(job.getKey());
    checkNotNull(session);

    Response response = Util.emptyResponse();

    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(job.getOwner().getRole()));
    } catch (AuthFailedException e) {
      return addMessage(response.setResponseCode(AUTH_FAILED), e.getMessage());
    }

    try {
      SanitizedConfiguration sanitized = SanitizedConfiguration.fromUnsanitized(job);

      lockManager.validateIfLocked(
          ILockKey.build(LockKey.job(jobKey.newBuilder())),
          Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

      schedulerCore.createJob(sanitized);
      response.setResponseCode(OK);
    } catch (LockException e) {
      addMessage(response, LOCK_ERROR, e.getMessage());
    } catch (TaskDescriptionException | ScheduleException e) {
      addMessage(response, INVALID_REQUEST, e.getMessage());
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
    checkNotNull(session);

    Response response = Util.emptyResponse();

    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(job.getOwner().getRole()));
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED);
      return addMessage(response, e.getMessage());
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
        addMessage(response, INVALID_REQUEST, e.getMessage());
        return response;
      }
      response.setResponseCode(OK);
    } catch (LockException e) {
      addMessage(response, LOCK_ERROR, e.getMessage());
    } catch (TaskDescriptionException e) {
      addMessage(response, INVALID_REQUEST, e.getMessage());
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
      addMessage(response, LOCK_ERROR, e.getMessage());
    }
    return response;
  }

  @Override
  public Response replaceCronTemplate(
      JobConfiguration mutableConfig,
      @Nullable Lock mutableLock,
      SessionKey session) {

    checkNotNull(mutableConfig);
    IJobConfiguration job = IJobConfiguration.build(mutableConfig);
    IJobKey jobKey = JobKeys.assertValid(job.getKey());
    checkNotNull(session);

    Response response = Util.emptyResponse();
    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(job.getOwner().getRole()));
    } catch (AuthFailedException e) {
      return addMessage(response, AUTH_FAILED, e.getMessage());
    }

    try {
      lockManager.validateIfLocked(
          ILockKey.build(LockKey.job(jobKey.newBuilder())),
          Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

      cronJobManager.updateJob(SanitizedCronJob.fromUnsanitized(job));
      return response.setResponseCode(OK);
    } catch (LockException e) {
      return addMessage(response, LOCK_ERROR, e.getMessage());
    } catch (CronException | TaskDescriptionException e) {
      return addMessage(response, INVALID_REQUEST, e.getMessage());
    }
  }

  @Override
  public Response populateJobConfig(JobConfiguration description) {
    checkNotNull(description);

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
    checkNotNull(session);
    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));

    Response response = Util.emptyResponse();
    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
    } catch (AuthFailedException e) {
      return addMessage(response, AUTH_FAILED, e.getMessage());
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
    checkNotNull(query);

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
      TaskQuery taskQuery,
      Iterable<IScheduledTask> tasks) throws AuthFailedException {

    // Authenticate the session against any affected roles, always including the role for a
    // role-scoped query.  This papers over the implementation detail that dormant cron jobs are
    // authenticated this way.
    ImmutableSet.Builder<String> targetRoles = ImmutableSet.<String>builder()
        .addAll(FluentIterable.from(tasks).transform(GET_ROLE));
    if (taskQuery.isSetOwner()) {
      targetRoles.add(taskQuery.getOwner().getRole());
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
  public Response killTasks(final TaskQuery query, Lock mutablelock, SessionKey session) {
    checkNotNull(query);
    checkNotNull(session);

    Response response = Util.emptyResponse();

    if (query.getJobName() != null && StringUtils.isBlank(query.getJobName())) {
      return addMessage(
          response,
          INVALID_REQUEST,
          String.format("Invalid job name: '%s'", query.getJobName()));
    }

    Set<IScheduledTask> tasks = Storage.Util.consistentFetchTasks(storage, Query.arbitrary(query));
    if (Iterables.isEmpty(tasks)) {
      // Short-circuit the call if there are no tasks to kill.
      return addMessage(response, OK, "No tasks to kill.");
    }

    Optional<SessionContext> context = isAdmin(session);
    if (context.isPresent()) {
      LOG.info("Granting kill query to admin user: " + query);
    } else {
      try {
        context = Optional.of(validateSessionKeyForTasks(session, query, tasks));
      } catch (AuthFailedException e) {
        return addMessage(response, AUTH_FAILED, e.getMessage());
      }
    }

    try {
      validateLockForTasks(Optional.fromNullable(mutablelock).transform(ILock.FROM_BUILDER), tasks);
      schedulerCore.killTasks(Query.arbitrary(query), context.get().getIdentity());
    } catch (LockException e) {
      return addMessage(response, LOCK_ERROR, e.getMessage());
    }

    return okEmptyResponse();
  }

  @Override
  public Response restartShards(
      JobKey mutableJobKey,
      Set<Integer> shardIds,
      Lock mutableLock,
      SessionKey session) {

    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));
    checkNotBlank(shardIds);
    checkNotNull(session);

    Response response = Util.emptyResponse();
    SessionContext context;
    try {
      context = sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
    } catch (AuthFailedException e) {
      return addMessage(response, AUTH_FAILED, e.getMessage());
    }

    try {
      lockManager.validateIfLocked(
          ILockKey.build(LockKey.job(jobKey.newBuilder())),
          Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));
      schedulerCore.restartShards(jobKey, shardIds, context.getIdentity());
      response.setResponseCode(OK);
    } catch (LockException e) {
      addMessage(response, LOCK_ERROR, e.getMessage());
    } catch (ScheduleException e) {
      addMessage(response, INVALID_REQUEST, e.getMessage());
    }

    return response;
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
    checkNotNull(resourceAggregate);
    checkNotNull(session);

    Response response = Util.emptyResponse();
    try {
      quotaManager.saveQuota(ownerRole, IResourceAggregate.build(resourceAggregate));
      return response.setResponseCode(OK);
    } catch (QuotaException e) {
      return addMessage(response, INVALID_REQUEST, e.getMessage());
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
    checkNotNull(status);
    checkNotNull(session);

    Response response = Util.emptyResponse();
    SessionContext context;
    try {
      // TODO(Sathya): Remove this after AOP-style session validation passes in a SessionContext.
      context = sessionValidator.checkAuthorized(session, Capability.ROOT, AuditCheck.REQUIRED);
    } catch (AuthFailedException e) {
      addMessage(response, AUTH_FAILED, e.getMessage());
      return response;
    }

    schedulerCore.setTaskStatus(taskId, status, transitionMessage(context.getIdentity()));
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
      addMessage(response, ERROR, e.getMessage());
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
      addMessage(response, ERROR, e.getMessage());
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
      addMessage(response, ERROR, e.getMessage());
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
      addMessage(response, ERROR, e.getMessage());
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
      return addMessage(response, ERROR, e.getMessage());
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

    checkNotNull(config);
    checkNotNull(session);
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
      return addMessage(resp, AUTH_FAILED, e.getMessage());
    } catch (LockException e) {
      return addMessage(resp, LOCK_ERROR, e.getMessage());
    } catch (TaskDescriptionException | ScheduleException e) {
      return addMessage(resp, INVALID_REQUEST, e.getMessage());
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
    checkNotNull(mutableLockKey);
    checkNotNull(session);

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
      return addMessage(response, AUTH_FAILED, e.getMessage());
    } catch (LockException e) {
      return addMessage(response, LOCK_ERROR, e.getMessage());
    }
  }

  @Override
  public Response releaseLock(Lock mutableLock, LockValidation validation, SessionKey session) {
    checkNotNull(mutableLock);
    checkNotNull(validation);
    checkNotNull(session);

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
      return addMessage(response, AUTH_FAILED, e.getMessage());
    } catch (LockException e) {
      return addMessage(response, LOCK_ERROR, e.getMessage());
    }
  }

  @Override
  public Response getLocks() {
    return okResponse(Result.getLocksResult(
        new GetLocksResult().setLocks(ILock.toBuildersSet(lockManager.getLocks()))));
  }

  @VisibleForTesting
  static Optional<String> transitionMessage(String user) {
    return Optional.of("Transition forced by " + user);
  }

  private static Response okEmptyResponse()  {
    return emptyResponse().setResponseCode(OK);
  }

  private static Response okResponse(Result result) {
    return okEmptyResponse().setResult(result);
  }
}
