/*
 * Copyright 2013 Twitter, Inc.
 *
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
package com.twitter.aurora.scheduler.thrift;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.commons.lang.StringUtils;

import com.twitter.aurora.auth.CapabilityValidator;
import com.twitter.aurora.auth.CapabilityValidator.AuditCheck;
import com.twitter.aurora.auth.CapabilityValidator.Capability;
import com.twitter.aurora.auth.SessionValidator.AuthFailedException;
import com.twitter.aurora.gen.AcquireLockResult;
import com.twitter.aurora.gen.AddInstancesConfig;
import com.twitter.aurora.gen.AuroraAdmin;
import com.twitter.aurora.gen.ConfigRewrite;
import com.twitter.aurora.gen.DrainHostsResult;
import com.twitter.aurora.gen.EndMaintenanceResult;
import com.twitter.aurora.gen.GetJobUpdatesResult;
import com.twitter.aurora.gen.GetJobsResult;
import com.twitter.aurora.gen.GetQuotaResult;
import com.twitter.aurora.gen.Hosts;
import com.twitter.aurora.gen.InstanceConfigRewrite;
import com.twitter.aurora.gen.InstanceKey;
import com.twitter.aurora.gen.JobConfigRewrite;
import com.twitter.aurora.gen.JobConfigValidation;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.JobUpdateConfiguration;
import com.twitter.aurora.gen.ListBackupsResult;
import com.twitter.aurora.gen.Lock;
import com.twitter.aurora.gen.LockKey;
import com.twitter.aurora.gen.LockValidation;
import com.twitter.aurora.gen.MaintenanceStatusResult;
import com.twitter.aurora.gen.PopulateJobResult;
import com.twitter.aurora.gen.QueryRecoveryResult;
import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.gen.Response;
import com.twitter.aurora.gen.ResponseCode;
import com.twitter.aurora.gen.Result;
import com.twitter.aurora.gen.RewriteConfigsRequest;
import com.twitter.aurora.gen.RollbackShardsResult;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduleStatusResult;
import com.twitter.aurora.gen.SessionKey;
import com.twitter.aurora.gen.StartMaintenanceResult;
import com.twitter.aurora.gen.StartUpdateResult;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskQuery;
import com.twitter.aurora.gen.UpdateResult;
import com.twitter.aurora.gen.UpdateShardsResult;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.ScheduleException;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.configuration.ConfigurationManager;
import com.twitter.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import com.twitter.aurora.scheduler.configuration.ParsedConfiguration;
import com.twitter.aurora.scheduler.quota.Quotas;
import com.twitter.aurora.scheduler.state.CronJobManager;
import com.twitter.aurora.scheduler.state.JobFilter;
import com.twitter.aurora.scheduler.state.LockManager;
import com.twitter.aurora.scheduler.state.LockManager.LockException;
import com.twitter.aurora.scheduler.state.MaintenanceController;
import com.twitter.aurora.scheduler.state.SchedulerCore;
import com.twitter.aurora.scheduler.state.StateManager;
import com.twitter.aurora.scheduler.state.StateManager.InstanceException;
import com.twitter.aurora.scheduler.storage.JobStore;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.MutateWork;
import com.twitter.aurora.scheduler.storage.Storage.StoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.Work;
import com.twitter.aurora.scheduler.storage.UpdateStore;
import com.twitter.aurora.scheduler.storage.backup.Recovery;
import com.twitter.aurora.scheduler.storage.backup.Recovery.RecoveryException;
import com.twitter.aurora.scheduler.storage.backup.StorageBackup;
import com.twitter.aurora.scheduler.storage.entities.IAssignedTask;
import com.twitter.aurora.scheduler.storage.entities.IJobConfiguration;
import com.twitter.aurora.scheduler.storage.entities.IJobKey;
import com.twitter.aurora.scheduler.storage.entities.ILock;
import com.twitter.aurora.scheduler.storage.entities.ILockKey;
import com.twitter.aurora.scheduler.storage.entities.IQuota;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;
import com.twitter.aurora.scheduler.thrift.auth.DecoratedThrift;
import com.twitter.aurora.scheduler.thrift.auth.Requires;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.base.Supplier;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.BackoffHelper;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.aurora.auth.SessionValidator.SessionContext;
import static com.twitter.aurora.gen.Constants.CURRENT_API_VERSION;
import static com.twitter.aurora.gen.ResponseCode.AUTH_FAILED;
import static com.twitter.aurora.gen.ResponseCode.ERROR;
import static com.twitter.aurora.gen.ResponseCode.INVALID_REQUEST;
import static com.twitter.aurora.gen.ResponseCode.OK;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * Aurora scheduler thrift server implementation.
 * <p>
 * Interfaces between users and the scheduler to access/modify jobs and perform cluster
 * administration tasks.
 */
@DecoratedThrift
class SchedulerThriftInterface implements AuroraAdmin.Iface {
  private static final Logger LOG = Logger.getLogger(SchedulerThriftInterface.class.getName());

  @CmdLine(name = "kill_task_initial_backoff",
      help = "Initial backoff delay while waiting for the tasks to transition to KILLED.")
  private static final Arg<Amount<Long, Time>> KILL_TASK_INITIAL_BACKOFF =
      Arg.create(Amount.of(1L, Time.SECONDS));

  @CmdLine(name = "kill_task_max_backoff",
      help = "Max backoff delay while waiting for the tasks to transition to KILLED.")
  private static final Arg<Amount<Long, Time>> KILL_TASK_MAX_BACKOFF =
      Arg.create(Amount.of(30L, Time.SECONDS));

  private static final Function<IScheduledTask, String> GET_ROLE = Functions.compose(
      new Function<ITaskConfig, String>() {
        @Override public String apply(ITaskConfig task) {
          return task.getOwner().getRole();
        }
      },
      Tasks.SCHEDULED_TO_INFO);

  private final Storage storage;
  private final SchedulerCore schedulerCore;
  private final StateManager stateManager;
  private final LockManager lockManager;
  private final CapabilityValidator sessionValidator;
  private final StorageBackup backup;
  private final Recovery recovery;
  private final MaintenanceController maintenance;
  private final CronJobManager cronJobManager;
  private final JobFilter jobFilter;
  private final Amount<Long, Time> killTaskInitialBackoff;
  private final Amount<Long, Time> killTaskMaxBackoff;

  @Inject
  SchedulerThriftInterface(
      Storage storage,
      SchedulerCore schedulerCore,
      StateManager stateManager,
      LockManager lockManager,
      CapabilityValidator sessionValidator,
      StorageBackup backup,
      Recovery recovery,
      CronJobManager cronJobManager,
      JobFilter jobFilter,
      MaintenanceController maintenance) {

    this(storage,
        schedulerCore,
        stateManager,
        lockManager,
        sessionValidator,
        backup,
        recovery,
        maintenance,
        cronJobManager,
        jobFilter,
        KILL_TASK_INITIAL_BACKOFF.get(),
        KILL_TASK_MAX_BACKOFF.get());
  }

  @VisibleForTesting
  SchedulerThriftInterface(
      Storage storage,
      SchedulerCore schedulerCore,
      StateManager stateManager,
      LockManager lockManager,
      CapabilityValidator sessionValidator,
      StorageBackup backup,
      Recovery recovery,
      MaintenanceController maintenance,
      CronJobManager cronJobManager,
      JobFilter jobFilter,
      Amount<Long, Time> initialBackoff,
      Amount<Long, Time> maxBackoff) {

    this.storage = checkNotNull(storage);
    this.schedulerCore = checkNotNull(schedulerCore);
    this.stateManager = checkNotNull(stateManager);
    this.lockManager = checkNotNull(lockManager);
    this.sessionValidator = checkNotNull(sessionValidator);
    this.backup = checkNotNull(backup);
    this.recovery = checkNotNull(recovery);
    this.maintenance = checkNotNull(maintenance);
    this.cronJobManager = checkNotNull(cronJobManager);
    this.jobFilter = checkNotNull(jobFilter);
    this.killTaskInitialBackoff = checkNotNull(initialBackoff);
    this.killTaskMaxBackoff = checkNotNull(maxBackoff);
  }

  @Override
  public Response createJob(
      JobConfiguration mutableJob,
      @Nullable Lock mutableLock,
      SessionKey session) {

    IJobConfiguration job = IJobConfiguration.build(mutableJob);
    IJobKey jobKey = JobKeys.assertValid(job.getKey());
    checkNotNull(session);

    Response response = new Response();

    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(job.getOwner().getRole()));
    } catch (AuthFailedException e) {
      return response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
    }

    try {
      lockManager.validateIfLocked(
          ILockKey.build(LockKey.job(jobKey.newBuilder())),
          Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

      ParsedConfiguration parsed = ParsedConfiguration.fromUnparsed(job);
      schedulerCore.createJob(parsed);
      response.setResponseCode(OK)
          .setMessage(String.format("%d new tasks pending for job %s",
              parsed.getJobConfig().getInstanceCount(), JobKeys.toPath(job)));
    } catch (LockException | TaskDescriptionException | ScheduleException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
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

    Response response = new Response();
    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(job.getOwner().getRole()));
    } catch (AuthFailedException e) {
      return response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
    }

    try {
      lockManager.validateIfLocked(
          ILockKey.build(LockKey.job(jobKey.newBuilder())),
          Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

      ParsedConfiguration parsed = ParsedConfiguration.fromUnparsed(job);

      if (!cronJobManager.hasJob(jobKey)) {
        return response.setResponseCode(INVALID_REQUEST).setMessage(
            "No cron template found for the given key: " + jobKey);
      }
      cronJobManager.updateJob(parsed);
      return response.setResponseCode(OK).setMessage("Replaced template for: " + jobKey);

    } catch (LockException e) {
      return response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    } catch (TaskDescriptionException e) {
      return response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    } catch (ScheduleException e) {
      return response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    }
  }

  @Override
  public Response populateJobConfig(JobConfiguration description, JobConfigValidation validation) {

    checkNotNull(description);

    Response response = new Response();
    try {
      ParsedConfiguration parsed =
          ParsedConfiguration.fromUnparsed(IJobConfiguration.build(description));

      if (validation != null && validation == JobConfigValidation.RUN_FILTERS) {
        JobFilter.JobFilterResult filterResult = jobFilter.filter(parsed.getJobConfig());
        if (!filterResult.isPass()) {
          return response.setResponseCode(INVALID_REQUEST).setMessage(filterResult.getReason());
        }
      }

      PopulateJobResult result = new PopulateJobResult()
          .setPopulated(ITaskConfig.toBuildersSet(parsed.getTaskConfigs().values()));
      response.setResult(Result.populateJobResult(result))
          .setResponseCode(OK)
          .setMessage("Tasks populated");
    } catch (TaskDescriptionException e) {
      response.setResponseCode(INVALID_REQUEST)
          .setMessage("Invalid configuration: " + e.getMessage());
    }
    return response;
  }

  @Override
  public Response startCronJob(JobKey mutableJobKey, SessionKey session) {
    checkNotNull(session);
    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));

    Response response = new Response();
    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
      return response;
    }

    try {
      schedulerCore.startCronJob(jobKey);
      response.setResponseCode(OK).setMessage("Cron run started.");
    } catch (ScheduleException e) {
      response.setResponseCode(INVALID_REQUEST)
          .setMessage("Failed to start cron job - " + e.getMessage());
    } catch (TaskDescriptionException e) {
      response.setResponseCode(ERROR).setMessage("Invalid task description: " + e.getMessage());
    }

    return response;
  }

  // TODO(William Farner): Provide status information about cron jobs here.
  @Override
  public Response getTasksStatus(TaskQuery query) {
    checkNotNull(query);

    Set<IScheduledTask> tasks =
        Storage.Util.weaklyConsistentFetchTasks(storage, Query.arbitrary(query));

    Response response = new Response();

    if (tasks.isEmpty()) {
      response.setResponseCode(INVALID_REQUEST)
          .setMessage("No tasks found for query: " + query);
    } else {
      response.setResponseCode(OK)
          .setResult(Result.scheduleStatusResult(
              new ScheduleStatusResult().setTasks(IScheduledTask.toBuildersList(tasks))));
    }

    return response;
  }

  @Override
  public Response getJobs(@Nullable String maybeNullRole) {
    Optional<String> ownerRole = Optional.fromNullable(maybeNullRole);


    // Ensure we only return one JobConfiguration for each JobKey.
    Map<IJobKey, IJobConfiguration> jobs = Maps.newHashMap();

    // Query the task store, find immediate jobs, and synthesize a JobConfiguration for them.
    // This is necessary because the ImmediateJobManager doesn't store jobs directly and
    // ImmediateJobManager#getJobs always returns an empty Collection.
    Query.Builder scope = ownerRole.isPresent()
        ? Query.roleScoped(ownerRole.get())
        : Query.unscoped();
    Multimap<IJobKey, IScheduledTask> tasks =
        Tasks.byJobKey(Storage.Util.weaklyConsistentFetchTasks(storage, scope.active()));

    jobs.putAll(Maps.transformEntries(tasks.asMap(),
        new Maps.EntryTransformer<IJobKey, Collection<IScheduledTask>, IJobConfiguration>() {
          @Override
          public IJobConfiguration transformEntry(
              IJobKey jobKey,
              Collection<IScheduledTask> tasks) {

            // Pick an arbitrary task for each immediate job. The chosen task might not be the most
            // recent if the job is in the middle of an update or some shards have been selectively
            // created.
            TaskConfig firstTask = tasks.iterator().next().getAssignedTask().getTask().newBuilder();
            return IJobConfiguration.build(new JobConfiguration()
                .setKey(jobKey.newBuilder())
                .setOwner(firstTask.getOwner())
                .setTaskConfig(firstTask)
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

    return new Response()
        .setResponseCode(OK)
        .setResult(Result.getJobsResult(new GetJobsResult()
            .setConfigs(IJobConfiguration.toBuildersSet(jobs.values()))));
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
    // TODO(wfarner): Determine whether this is a useful function, or if it should simply be
    //     switched to 'killJob'.

    checkNotNull(query);
    checkNotNull(session);

    Response response = new Response();

    if (query.getJobName() != null && StringUtils.isBlank(query.getJobName())) {
      response.setResponseCode(INVALID_REQUEST).setMessage(
          String.format("Invalid job name: '%s'", query.getJobName()));
      return response;
    }

    Set<IScheduledTask> tasks = Storage.Util.consistentFetchTasks(storage, Query.arbitrary(query));

    Optional<SessionContext> context = isAdmin(session);
    if (context.isPresent()) {
      LOG.info("Granting kill query to admin user: " + query);
    } else {
      try {
        context = Optional.of(validateSessionKeyForTasks(session, query, tasks));
      } catch (AuthFailedException e) {
        response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
        return response;
      }
    }

    try {
      validateLockForTasks(Optional.fromNullable(mutablelock).transform(ILock.FROM_BUILDER), tasks);
      schedulerCore.killTasks(Query.arbitrary(query), context.get().getIdentity());
    } catch (LockException e) {
      return response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    } catch (ScheduleException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
      return response;
    }

    // TODO(William Farner): Move this into the client.
    BackoffHelper backoff = new BackoffHelper(killTaskInitialBackoff, killTaskMaxBackoff, true);
    final Query.Builder activeQuery = Query.arbitrary(query.setStatuses(Tasks.ACTIVE_STATES));
    try {
      backoff.doUntilSuccess(new Supplier<Boolean>() {
        @Override public Boolean get() {
          Set<IScheduledTask> tasks = Storage.Util.consistentFetchTasks(storage, activeQuery);
          if (tasks.isEmpty()) {
            LOG.info("Tasks all killed, done waiting.");
            return true;
          } else {
            LOG.info("Jobs not yet killed, waiting...");
            return false;
          }
        }
      });
      response.setResponseCode(OK).setMessage("Tasks killed.");
    } catch (InterruptedException e) {
      LOG.warning("Interrupted while trying to kill tasks: " + e);
      Thread.currentThread().interrupt();
      response.setResponseCode(ERROR).setMessage("killTasks thread was interrupted.");
    } catch (BackoffHelper.BackoffStoppedException e) {
      response.setResponseCode(ERROR).setMessage("Tasks were not killed in time.");
    }
    return response;
  }

  @Override
  public Response startUpdate(JobConfiguration mutableJob, SessionKey session) {
    IJobConfiguration job = IJobConfiguration.build(mutableJob);
    checkNotNull(job);
    checkNotNull(session);

    Response response = new Response();

    if (!JobKeys.isValid(job.getKey())) {
      return response.setResponseCode(INVALID_REQUEST)
          .setMessage("Invalid job key: " + job.getKey());
    }

    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(job.getOwner().getRole()));
    } catch (AuthFailedException e) {
      return response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
    }

    try {
      Optional<String> token =
          schedulerCore.initiateJobUpdate(ParsedConfiguration.fromUnparsed(job));

      StartUpdateResult result = new StartUpdateResult()
          .setRollingUpdateRequired(token.isPresent());

      response.setResult(Result.startUpdateResult(result))
        .setResponseCode(OK);

      if (token.isPresent()) {
        result.setUpdateToken(token.get());
        response.setMessage("Update successfully started.");
      } else {
        response.setMessage("Job successfully updated.");
      }
    } catch (ScheduleException | TaskDescriptionException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public Response updateShards(
      JobKey mutableJobKey,
      Set<Integer> shards,
      String updateToken,
      SessionKey session) {

    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));
    checkNotBlank(shards);
    checkNotBlank(updateToken);
    checkNotNull(session);

    Response response = new Response();
    SessionContext context;
    try {
      context = sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
    } catch (AuthFailedException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
      return response;
    }

    try {
      response
          .setResponseCode(OK)
          .setMessage("Successfully started update of shards: " + shards)
          .setResult(Result.updateShardsResult(new UpdateShardsResult()
          .setShards(schedulerCore.updateShards(jobKey, context.getIdentity(), shards, updateToken))
      ));
    } catch (ScheduleException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public Response rollbackShards(
      JobKey mutableJobKey,
      Set<Integer> shards,
      String updateToken,
      SessionKey session) {

    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));
    checkNotBlank(shards);
    checkNotBlank(updateToken);
    checkNotNull(session);

    Response response = new Response();
    SessionContext context;
    try {
      context = sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
    } catch (AuthFailedException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
      return response;
    }

    try {
      response.setResult(Result.rollbackShardsResult(new RollbackShardsResult()
          .setShards(schedulerCore.rollbackShards(
              jobKey, context.getIdentity(), shards, updateToken))))
          .setResponseCode(OK)
          .setMessage("Successfully started rollback of shards: " + shards);
    } catch (ScheduleException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public Response finishUpdate(
      JobKey mutableJobKey,
      UpdateResult updateResult,
      String updateToken,
      SessionKey session) {

    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));
    checkNotNull(session);

    Response response = new Response();
    SessionContext context;
    try {
      context = sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
      return response;
    }

    Optional<String> token = updateResult == UpdateResult.TERMINATE
        ? Optional.<String>absent() : Optional.of(updateToken);
    try {
      schedulerCore.finishUpdate(jobKey, context.getIdentity(), token, updateResult);
      response.setResponseCode(OK).setMessage("Update successfully finished.");
    } catch (ScheduleException e) {
      response.setResponseCode(ResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public Response restartShards(
      JobKey mutableJobKey,
      Set<Integer> shardIds,
      Lock mutableLock,
      SessionKey session) {

    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(mutableJobKey));
    MorePreconditions.checkNotBlank(shardIds);
    checkNotNull(session);

    Response response = new Response();
    SessionContext context;
    try {
      context = sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
      return response;
    }

    try {
      lockManager.validateIfLocked(
          ILockKey.build(LockKey.job(jobKey.newBuilder())),
          Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));
      schedulerCore.restartShards(jobKey, shardIds, context.getIdentity());
      response.setResponseCode(OK).setMessage("Shards are restarting.");
    } catch (LockException | ScheduleException e) {
      response.setResponseCode(ResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public Response getQuota(final String ownerRole) {
    checkNotBlank(ownerRole);

    IQuota quota = storage.consistentRead(new Work.Quiet<IQuota>() {
      @Override public IQuota apply(StoreProvider storeProvider) {
        return storeProvider.getQuotaStore().fetchQuota(ownerRole).or(Quotas.noQuota());
      }
    });

    return new Response()
        .setResponseCode(OK)
        .setResult(Result.getQuotaResult(new GetQuotaResult()
            .setQuota(quota.newBuilder())));
  }

  @Override
  public Response startMaintenance(Hosts hosts, SessionKey session) {
      return new Response()
          .setResponseCode(OK)
          .setResult(Result.startMaintenanceResult(new StartMaintenanceResult()
              .setStatuses(maintenance.startMaintenance(hosts.getHostNames()))));
  }

  @Override
  public Response drainHosts(Hosts hosts, SessionKey session) {
    return new Response()
        .setResponseCode(OK)
        .setResult(Result.drainHostsResult(new DrainHostsResult()
            .setStatuses(maintenance.drain(hosts.getHostNames()))));
  }

  @Override
  public Response maintenanceStatus(Hosts hosts, SessionKey session) {
    return new Response()
        .setResponseCode(OK)
        .setResult(Result.maintenanceStatusResult(new MaintenanceStatusResult()
            .setStatuses(maintenance.getStatus(hosts.getHostNames()))));
  }

  @Override
  public Response endMaintenance(Hosts hosts, SessionKey session) {
      return new Response()
          .setResponseCode(OK)
          .setResult(Result.endMaintenanceResult(new EndMaintenanceResult()
              .setStatuses(maintenance.endMaintenance(hosts.getHostNames()))));
  }

  @Requires(whitelist = Capability.PROVISIONER)
  @Override
  public Response setQuota(final String ownerRole, final Quota quota, SessionKey session) {
    checkNotBlank(ownerRole);
    checkNotNull(quota);
    checkNotNull(session);

    // TODO(Kevin Sweeney): Input validation for Quota.

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getQuotaStore().saveQuota(ownerRole, IQuota.build(quota));
      }
    });

    return new Response().setResponseCode(OK).setMessage("Quota applied.");
  }

  @Override
  public Response forceTaskState(
      String taskId,
      ScheduleStatus status,
      SessionKey session) {

    checkNotBlank(taskId);
    checkNotNull(status);
    checkNotNull(session);

    Response response = new Response();
    SessionContext context;
    try {
      // TODO(Sathya): Remove this after AOP-style session validation passes in a SessionContext.
      context = sessionValidator.checkAuthorized(session, Capability.ROOT, AuditCheck.REQUIRED);
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
      return response;
    }

    schedulerCore.setTaskStatus(
        Query.taskScoped(taskId), status, transitionMessage(context.getIdentity()));
    return new Response().setResponseCode(OK).setMessage("Transition attempted.");
  }

  @Override
  public Response performBackup(SessionKey session) {
    backup.backupNow();
    return new Response().setResponseCode(OK);
  }

  @Override
  public Response listBackups(SessionKey session) {
    return new Response()
        .setResponseCode(OK)
        .setResult(Result.listBackupsResult(new ListBackupsResult()
            .setBackups(recovery.listBackups())));
  }

  @Override
  public Response stageRecovery(String backupId, SessionKey session) {
    Response response = new Response().setResponseCode(OK);
    try {
      recovery.stage(backupId);
    } catch (RecoveryException e) {
      response.setResponseCode(ERROR).setMessage(e.getMessage());
      LOG.log(Level.WARNING, "Failed to stage recovery: " + e, e);
    }

    return response;
  }

  @Override
  public Response queryRecovery(TaskQuery query, SessionKey session) {
    Response response = new Response();
    try {
      response.setResponseCode(OK)
          .setResult(Result.queryRecoveryResult(new QueryRecoveryResult()
              .setTasks(IScheduledTask.toBuildersSet(recovery.query(Query.arbitrary(query))))));
    } catch (RecoveryException e) {
      response.setResponseCode(ERROR).setMessage(e.getMessage());
      LOG.log(Level.WARNING, "Failed to query recovery: " + e, e);
    }

    return response;
  }

  @Override
  public Response deleteRecoveryTasks(TaskQuery query, SessionKey session) {
    Response response = new Response().setResponseCode(OK);
    try {
      recovery.deleteTasks(Query.arbitrary(query));
    } catch (RecoveryException e) {
      response.setResponseCode(ERROR).setMessage(e.getMessage());
      LOG.log(Level.WARNING, "Failed to delete recovery tasks: " + e, e);
    }

    return response;
  }

  @Override
  public Response commitRecovery(SessionKey session) {
    Response response = new Response().setResponseCode(OK);
    try {
      recovery.commit();
    } catch (RecoveryException e) {
      response.setResponseCode(ERROR).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public Response getJobUpdates(SessionKey session) {
    return storage.weaklyConsistentRead(new Work.Quiet<Response>() {
      @Override public Response apply(StoreProvider storeProvider) {
        GetJobUpdatesResult result = new GetJobUpdatesResult()
            .setJobUpdates(Sets.<JobUpdateConfiguration>newHashSet());
        UpdateStore store = storeProvider.getUpdateStore();
        for (String role : store.fetchUpdatingRoles()) {
          for (JobUpdateConfiguration config : store.fetchUpdateConfigs(role)) {
            result.addToJobUpdates(config);
          }
        }
        return new Response().setResponseCode(OK)
            .setResult(Result.getJobUpdatesResult(result));
      }
    });
  }

  @Override
  public Response unloadRecovery(SessionKey session) {
    recovery.unload();
    return new Response().setResponseCode(OK);
  }

  @Override
  public Response snapshot(SessionKey session) {
    Response response = new Response();
    try {
      storage.snapshot();
      return response.setResponseCode(OK).setMessage("Compaction successful.");
    } catch (Storage.StorageException e) {
      LOG.log(Level.WARNING, "Requested snapshot failed.", e);
      return response.setResponseCode(ERROR).setMessage(e.getMessage());
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
      return new Response()
        .setResponseCode(ResponseCode.ERROR)
        .setMessage("No rewrite commands provided.");
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override public Response apply(MutableStoreProvider storeProvider) {
        List<String> errors = Lists.newArrayList();

        for (ConfigRewrite command : request.getRewriteCommands()) {
          switch (command.getSetField()) {
            case JOB_REWRITE:
              JobConfigRewrite jobRewrite = command.getJobRewrite();
              IJobConfiguration existingJob = IJobConfiguration.build(jobRewrite.getOldJob());
              IJobConfiguration rewrittenJob;
              try {
                rewrittenJob = ConfigurationManager.validateAndPopulate(
                    IJobConfiguration.build(jobRewrite.getRewrittenJob()));
              } catch (TaskDescriptionException e) {
                // We could add an error here, but this is probably a hint of something wrong in
                // the client that's causing a bad configuration to be applied.
                throw Throwables.propagate(e);
              }
              if (!existingJob.getKey().equals(rewrittenJob.getKey())) {
                errors.add("Disallowing rewrite attempting to change job key.");
              } else if (!existingJob.getOwner().equals(rewrittenJob.getOwner())) {
                errors.add("Disallowing rewrite attempting to change job owner.");
              } else {
                JobStore.Mutable jobStore = storeProvider.getJobStore();
                Multimap<String, IJobConfiguration> matches =
                    jobsByKey(jobStore, existingJob.getKey());
                switch (matches.size()) {
                  case 0:
                    errors.add("No jobs found for key " + JobKeys.toPath(existingJob));
                    break;

                  case 1:
                    Map.Entry<String, IJobConfiguration> match =
                        Iterables.getOnlyElement(matches.entries());
                    IJobConfiguration storedJob = match.getValue();
                    if (!storedJob.equals(existingJob)) {
                      errors.add("CAS compare failed for " + JobKeys.toPath(storedJob));
                    } else {
                      jobStore.saveAcceptedJob(match.getKey(), rewrittenJob);
                    }
                    break;

                  default:
                    errors.add("Multiple jobs found for key " + JobKeys.toPath(existingJob));
                }
              }
              break;

            case INSTANCE_REWRITE:
              InstanceConfigRewrite instanceRewrite = command.getInstanceRewrite();
              InstanceKey instanceKey = instanceRewrite.getInstanceKey();
              Iterable<IScheduledTask> tasks = storeProvider.getTaskStore().fetchTasks(
                  Query.instanceScoped(IJobKey.build(instanceKey.getJobKey()),
                      instanceKey.getInstanceId())
                      .active());
              Optional<IAssignedTask> task =
                  Optional.fromNullable(Iterables.getOnlyElement(tasks, null))
                      .transform(Tasks.SCHEDULED_TO_ASSIGNED);
              if (!task.isPresent()) {
                errors.add("No active task found for " + instanceKey);
              } else if (!task.get().getTask().newBuilder().equals(instanceRewrite.getOldTask())) {
                errors.add("CAS compare failed for " + instanceKey);
              } else {
                ITaskConfig newConfiguration = ITaskConfig.build(
                    ConfigurationManager.applyDefaultsIfUnset(instanceRewrite.getRewrittenTask()));
                boolean changed = storeProvider.getUnsafeTaskStore().unsafeModifyInPlace(
                    task.get().getTaskId(), newConfiguration);
                if (!changed) {
                  errors.add("Did not change " + task.get().getTaskId());
                }
              }
              break;

            default:
              throw new IllegalArgumentException("Unhandled command type " + command.getSetField());
          }
        }

        Response resp = new Response();
        if (!errors.isEmpty()) {
          resp.setResponseCode(ResponseCode.WARNING).setMessage(Joiner.on(", ").join(errors));
        } else {
          resp.setResponseCode(OK).setMessage("All rewrites completed successfully.");
        }
        return resp;
      }
    });
  }

  @Override
  public Response getVersion() {
    return new Response()
        .setResponseCode(OK)
        .setResult(Result.getVersionResult(CURRENT_API_VERSION));
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

    Response resp = new Response();
    ITaskConfig task;
    try {
      task = ConfigurationManager.validateAndPopulate(ITaskConfig.build(config.getTaskConfig()));
    } catch (TaskDescriptionException e) {
      return resp.setResponseCode(ERROR).setMessage(e.getMessage());
    }

    if (cronJobManager.hasJob(jobKey)) {
      return resp.setResponseCode(INVALID_REQUEST).setMessage("Cron jobs are not supported here.");
    }

    JobFilter.JobFilterResult filterResult = jobFilter.filter(task, config.getInstanceIdsSize());
    if (!filterResult.isPass()) {
      return resp.setResponseCode(INVALID_REQUEST).setMessage(filterResult.getReason());
    }

    try {
      sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
      lockManager.validateIfLocked(
          ILockKey.build(LockKey.job(jobKey.newBuilder())),
          Optional.fromNullable(mutableLock).transform(ILock.FROM_BUILDER));

      stateManager.addInstances(jobKey, ImmutableSet.copyOf(config.getInstanceIds()), task);
      return resp.setResponseCode(OK).setMessage("Successfully added instances.");
    } catch (AuthFailedException e) {
      return resp.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
    } catch (LockException e) {
      return resp.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    } catch (InstanceException e) {
      return resp.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    }
  }

  private String getRoleFromLockKey(ILockKey lockKey) {
    switch (lockKey.getSetField()) {
      case JOB:
        JobKeys.assertValid(lockKey.getJob());
        return lockKey.getJob().getRole();
      default:
        throw new IllegalArgumentException("Unhandled LockKey: " + lockKey.getSetField());
    }
  }

  @Override
  public Response acquireLock(LockKey mutableLockKey, SessionKey session) {
    checkNotNull(mutableLockKey);
    checkNotNull(session);

    ILockKey lockKey = ILockKey.build(mutableLockKey);
    Response response = new Response();

    try {
      SessionContext context = sessionValidator.checkAuthenticated(
          session,
          ImmutableSet.of(getRoleFromLockKey(lockKey)));

      ILock lock = lockManager.acquireLock(lockKey, context.getIdentity());
      response.setResult(Result.acquireLockResult(
          new AcquireLockResult().setLock(lock.newBuilder())));

      return response.setResponseCode(OK).setMessage("Lock has been acquired.");
    } catch (AuthFailedException e) {
      return response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
    } catch (LockException e) {
      return response.setResponseCode(ResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
    }
  }

  @Override
  public Response releaseLock(Lock mutableLock, LockValidation validation, SessionKey session) {
    checkNotNull(mutableLock);
    checkNotNull(validation);
    checkNotNull(session);

    Response response = new Response();
    ILock lock = ILock.build(mutableLock);

    try {
      sessionValidator.checkAuthenticated(
          session,
          ImmutableSet.of(getRoleFromLockKey(lock.getKey())));

      if (validation == LockValidation.CHECKED) {
        lockManager.validateIfLocked(lock.getKey(), Optional.of(lock));
      }
      lockManager.releaseLock(lock);
      return response.setResponseCode(OK).setMessage("Lock has been released.");
    } catch (AuthFailedException e) {
      return response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
    } catch (LockException e) {
      return response.setResponseCode(ResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
    }
  }

  @VisibleForTesting
  static Optional<String> transitionMessage(String user) {
    return Optional.of("Transition forced by " + user);
  }
}
