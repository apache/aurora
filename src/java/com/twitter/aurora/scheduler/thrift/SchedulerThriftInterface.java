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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.commons.lang.StringUtils;

import com.twitter.aurora.auth.SessionValidator.AuthFailedException;
import com.twitter.aurora.gen.APIVersion;
import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.AuroraAdmin;
import com.twitter.aurora.gen.ConfigRewrite;
import com.twitter.aurora.gen.DrainHostsResponse;
import com.twitter.aurora.gen.EndMaintenanceResponse;
import com.twitter.aurora.gen.GetJobUpdatesResponse;
import com.twitter.aurora.gen.GetJobsResponse;
import com.twitter.aurora.gen.GetQuotaResponse;
import com.twitter.aurora.gen.Hosts;
import com.twitter.aurora.gen.JobConfigRewrite;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.JobUpdateConfiguration;
import com.twitter.aurora.gen.ListBackupsResponse;
import com.twitter.aurora.gen.MaintenanceStatusResponse;
import com.twitter.aurora.gen.PopulateJobResult;
import com.twitter.aurora.gen.QueryRecoveryResponse;
import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.gen.Response;
import com.twitter.aurora.gen.ResponseCode;
import com.twitter.aurora.gen.Result;
import com.twitter.aurora.gen.RewriteConfigsRequest;
import com.twitter.aurora.gen.RollbackShardsResponse;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduleStatusResult;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.SessionKey;
import com.twitter.aurora.gen.ShardConfigRewrite;
import com.twitter.aurora.gen.ShardKey;
import com.twitter.aurora.gen.StartMaintenanceResponse;
import com.twitter.aurora.gen.StartUpdateResult;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskQuery;
import com.twitter.aurora.gen.UpdateResponseCode;
import com.twitter.aurora.gen.UpdateResult;
import com.twitter.aurora.gen.UpdateShardsResponse;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.ScheduleException;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.configuration.ConfigurationManager;
import com.twitter.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import com.twitter.aurora.scheduler.configuration.ParsedConfiguration;
import com.twitter.aurora.scheduler.quota.QuotaManager;
import com.twitter.aurora.scheduler.state.CronJobManager;
import com.twitter.aurora.scheduler.state.MaintenanceController;
import com.twitter.aurora.scheduler.state.SchedulerCore;
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
import com.twitter.aurora.scheduler.thrift.auth.CapabilityValidator;
import com.twitter.aurora.scheduler.thrift.auth.CapabilityValidator.Capability;
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

  private static final Function<ScheduledTask, String> GET_ROLE = Functions.compose(
      new Function<TaskConfig, String>() {
        @Override public String apply(TaskConfig task) {
          return task.getOwner().getRole();
        }
      },
      Tasks.SCHEDULED_TO_INFO);

  private final Storage storage;
  private final SchedulerCore schedulerCore;
  private final CapabilityValidator sessionValidator;
  private final QuotaManager quotaManager;
  private final StorageBackup backup;
  private final Recovery recovery;
  private final MaintenanceController maintenance;
  private final CronJobManager cronJobManager;
  private final Amount<Long, Time> killTaskInitialBackoff;
  private final Amount<Long, Time> killTaskMaxBackoff;

  @Inject
  SchedulerThriftInterface(
      Storage storage,
      SchedulerCore schedulerCore,
      CapabilityValidator sessionValidator,
      QuotaManager quotaManager,
      StorageBackup backup,
      Recovery recovery,
      CronJobManager cronJobManager,
      MaintenanceController maintenance) {

    this(storage,
        schedulerCore,
        sessionValidator,
        quotaManager,
        backup,
        recovery,
        maintenance,
        cronJobManager,
        KILL_TASK_INITIAL_BACKOFF.get(),
        KILL_TASK_MAX_BACKOFF.get());
  }

  @VisibleForTesting
  SchedulerThriftInterface(
      Storage storage,
      SchedulerCore schedulerCore,
      CapabilityValidator sessionValidator,
      QuotaManager quotaManager,
      StorageBackup backup,
      Recovery recovery,
      MaintenanceController maintenance,
      CronJobManager cronJobManager,
      Amount<Long, Time> initialBackoff,
      Amount<Long, Time> maxBackoff) {

    this.storage = checkNotNull(storage);
    this.schedulerCore = checkNotNull(schedulerCore);
    this.sessionValidator = checkNotNull(sessionValidator);
    this.quotaManager = checkNotNull(quotaManager);
    this.backup = checkNotNull(backup);
    this.recovery = checkNotNull(recovery);
    this.maintenance = checkNotNull(maintenance);
    this.cronJobManager = checkNotNull(cronJobManager);
    this.killTaskInitialBackoff = checkNotNull(initialBackoff);
    this.killTaskMaxBackoff = checkNotNull(maxBackoff);
  }

  @Override
  public Response createJob(JobConfiguration job, SessionKey session) {
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
      ParsedConfiguration parsed = ParsedConfiguration.fromUnparsed(job);
      schedulerCore.createJob(parsed);
      response.setResponseCode(OK)
          .setMessage(String.format("%d new tasks pending for job %s",
              parsed.getJobConfig().getShardCount(), JobKeys.toPath(job)));
    } catch (ConfigurationManager.TaskDescriptionException e) {
      response.setResponseCode(INVALID_REQUEST)
          .setMessage("Invalid task description: " + e.getMessage());
    } catch (ScheduleException e) {
      response.setResponseCode(INVALID_REQUEST)
          .setMessage("Failed to schedule job - " + e.getMessage());
    }

    return response;
  }

  @Override
  public Response populateJobConfig(JobConfiguration description) {
    checkNotNull(description);

    // TODO(ksweeney): check valid JobKey in description after deprecating non-environment version.

    Response response = new Response();

    try {
      PopulateJobResult result = new PopulateJobResult()
          .setPopulated(ParsedConfiguration.fromUnparsed(description).getTaskConfigs());
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
  public Response startCronJob(JobKey jobKey, SessionKey session) {
    checkNotNull(session);
    JobKeys.assertValid(jobKey);

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

    Set<ScheduledTask> tasks =
        Storage.Util.weaklyConsistentFetchTasks(storage, Query.arbitrary(query));

    Response response = new Response();

    if (tasks.isEmpty()) {
      response.setResponseCode(INVALID_REQUEST)
          .setMessage("No tasks found for query: " + query);
    } else {
      response.setResponseCode(OK)
          .setResult(Result.scheduleStatusResult(
              new ScheduleStatusResult().setTasks(ImmutableList.copyOf(tasks))
              )
          );
    }

    return response;
  }

  @Override
  public GetJobsResponse getJobs(@Nullable String maybeNullRole) {
    Optional<String> ownerRole = Optional.fromNullable(maybeNullRole);


    // Ensure we only return one JobConfiguration for each JobKey.
    Map<JobKey, JobConfiguration> jobs = Maps.newHashMap();

    // Query the task store, find immediate jobs, and synthesize a JobConfiguration for them.
    // This is necessary because the ImmediateJobManager doesn't store jobs directly and
    // ImmediateJobManager#getJobs always returns an empty Collection.
    Query.Builder scope = ownerRole.isPresent()
        ? Query.roleScoped(ownerRole.get())
        : Query.unscoped();
    Multimap<JobKey, ScheduledTask> tasks =
        Tasks.byJobKey(Storage.Util.weaklyConsistentFetchTasks(storage, scope.active()));

    jobs.putAll(Maps.transformEntries(tasks.asMap(),
        new Maps.EntryTransformer<JobKey, Collection<ScheduledTask>, JobConfiguration>() {
          @Override
          public JobConfiguration transformEntry(JobKey jobKey, Collection<ScheduledTask> tasks) {

            // Pick an arbitrary task for each immediate job. The chosen task might not be the most
            // recent if the job is in the middle of an update or some shards have been selectively
            // created.
            ScheduledTask firstTask = tasks.iterator().next();
            firstTask.getAssignedTask().getTask().unsetShardId();
            return new JobConfiguration()
                .setKey(jobKey)
                .setOwner(firstTask.getAssignedTask().getTask().getOwner())
                .setTaskConfig(firstTask.getAssignedTask().getTask())
                .setShardCount(tasks.size());
          }
        }));

    // Get cron jobs directly from the manager. Do this after querying the task store so the real
    // template JobConfiguration for a cron job will overwrite the synthesized one that could have
    // been created above.
    Predicate<JobConfiguration> configFilter = ownerRole.isPresent()
        ? Predicates.compose(Predicates.equalTo(ownerRole.get()), JobKeys.CONFIG_TO_ROLE)
        : Predicates.<JobConfiguration>alwaysTrue();
    jobs.putAll(Maps.uniqueIndex(
        FluentIterable.from(cronJobManager.getJobs()).filter(configFilter),
        JobKeys.FROM_CONFIG));

    return new GetJobsResponse()
        .setConfigs(ImmutableSet.copyOf(jobs.values()))
        .setResponseCode(OK);
  }

  private SessionContext validateSessionKeyForTasks(SessionKey session, TaskQuery taskQuery)
      throws AuthFailedException {

    Set<ScheduledTask> tasks =
        Storage.Util.consistentFetchTasks(storage, Query.arbitrary(taskQuery));
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
      return Optional.of(sessionValidator.checkAuthorized(session, Capability.ROOT));
    } catch (AuthFailedException e) {
      return Optional.absent();
    }
  }

  @Override
  public Response killTasks(final TaskQuery query, SessionKey session) {
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

    Optional<SessionContext> context = isAdmin(session);
    if (context.isPresent()) {
      LOG.info("Granting kill query to admin user: " + query);
    } else {
      try {
        context = Optional.of(validateSessionKeyForTasks(session, query));
      } catch (AuthFailedException e) {
        response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
        return response;
      }
    }

    try {
      schedulerCore.killTasks(Query.arbitrary(query), context.get().getIdentity());
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
          Set<ScheduledTask> tasks = Storage.Util.consistentFetchTasks(storage, activeQuery);
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
  public Response startUpdate(JobConfiguration job, SessionKey session) {
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
    } catch (ScheduleException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    } catch (ConfigurationManager.TaskDescriptionException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public UpdateShardsResponse updateShards(
      JobKey jobKey,
      Set<Integer> shards,
      String updateToken,
      SessionKey session) {

    JobKeys.assertValid(jobKey);
    checkNotBlank(shards);
    checkNotBlank(updateToken);
    checkNotNull(session);

    UpdateShardsResponse response = new UpdateShardsResponse();
    SessionContext context;
    try {
      context = sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
    } catch (AuthFailedException e) {
      response.setResponseCode(UpdateResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
      return response;
    }

    try {
      response
          .setShards(schedulerCore.updateShards(jobKey, context.getIdentity(), shards, updateToken))
          .setResponseCode(UpdateResponseCode.OK)
          .setMessage("Successfully started update of shards: " + shards);
    } catch (ScheduleException e) {
      response.setResponseCode(UpdateResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public RollbackShardsResponse rollbackShards(
      JobKey jobKey,
      Set<Integer> shards,
      String updateToken,
      SessionKey session) {

    JobKeys.assertValid(jobKey);
    checkNotBlank(shards);
    checkNotBlank(updateToken);
    checkNotNull(session);

    RollbackShardsResponse response = new RollbackShardsResponse();
    SessionContext context;
    try {
      context = sessionValidator.checkAuthenticated(session, ImmutableSet.of(jobKey.getRole()));
    } catch (AuthFailedException e) {
      response.setResponseCode(UpdateResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
      return response;
    }

    try {
      response
          .setShards(schedulerCore.rollbackShards(
              jobKey, context.getIdentity(), shards, updateToken))
          .setResponseCode(UpdateResponseCode.OK)
          .setMessage("Successfully started rollback of shards: " + shards);
    } catch (ScheduleException e) {
      response.setResponseCode(UpdateResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public Response finishUpdate(
      JobKey jobKey,
      UpdateResult updateResult,
      String updateToken,
      SessionKey session) {

    JobKeys.assertValid(jobKey);
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
      JobKey jobKey,
      Set<Integer> shardIds,
      SessionKey session) {

    JobKeys.assertValid(jobKey);
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
      schedulerCore.restartShards(jobKey, shardIds, context.getIdentity());
      response.setResponseCode(OK).setMessage("Shards are restarting.");
    } catch (ScheduleException e) {
      response.setResponseCode(ResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public GetQuotaResponse getQuota(String ownerRole) {
    checkNotBlank(ownerRole);
    return new GetQuotaResponse().setQuota(quotaManager.getQuota(ownerRole));
  }

  @Override
  public StartMaintenanceResponse startMaintenance(Hosts hosts, SessionKey session) {
    return new StartMaintenanceResponse()
        .setStatuses(maintenance.startMaintenance(hosts.getHostNames()))
        .setResponseCode(OK);
  }

  @Override
  public DrainHostsResponse drainHosts(Hosts hosts, SessionKey session) {
    return new DrainHostsResponse()
        .setStatuses(maintenance.drain(hosts.getHostNames()))
        .setResponseCode(OK);
  }

  @Override
  public MaintenanceStatusResponse maintenanceStatus(Hosts hosts, SessionKey session) {
    return new MaintenanceStatusResponse()
        .setStatuses(maintenance.getStatus(hosts.getHostNames()))
        .setResponseCode(OK);
  }

  @Override
  public EndMaintenanceResponse endMaintenance(Hosts hosts, SessionKey session) {
    return new EndMaintenanceResponse()
        .setStatuses(maintenance.endMaintenance(hosts.getHostNames()))
        .setResponseCode(OK);
  }

  @Requires(whitelist = Capability.PROVISIONER)
  @Override
  public Response setQuota(String ownerRole, Quota quota, SessionKey session) {
    checkNotBlank(ownerRole);
    checkNotNull(quota);
    checkNotNull(session);

    quotaManager.setQuota(ownerRole, quota);
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
      context = sessionValidator.checkAuthorized(session, Capability.ROOT);
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
  public ListBackupsResponse listBackups(SessionKey session) {
    return new ListBackupsResponse()
        .setBackups(recovery.listBackups())
        .setResponseCode(OK);
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
  public QueryRecoveryResponse queryRecovery(TaskQuery query, SessionKey session) {
    QueryRecoveryResponse response = new QueryRecoveryResponse().setResponseCode(OK);
    try {
      response.setTasks(recovery.query(Query.arbitrary(query)));
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
  public GetJobUpdatesResponse getJobUpdates(SessionKey session) {
    return storage.weaklyConsistentRead(new Work.Quiet<GetJobUpdatesResponse>() {
      @Override public GetJobUpdatesResponse apply(StoreProvider storeProvider) {
        GetJobUpdatesResponse response = new GetJobUpdatesResponse().setResponseCode(OK);
        response.setJobUpdates(Sets.<JobUpdateConfiguration>newHashSet());
        UpdateStore store = storeProvider.getUpdateStore();
        for (String role : store.fetchUpdatingRoles()) {
          for (JobUpdateConfiguration config : store.fetchUpdateConfigs(role)) {
            response.addToJobUpdates(config);
          }
        }
        return response;
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

  private static Multimap<String, JobConfiguration> jobsByKey(JobStore jobStore, JobKey jobKey) {
    ImmutableMultimap.Builder<String, JobConfiguration> matches = ImmutableMultimap.builder();
    for (String managerId : jobStore.fetchManagerIds()) {
      for (JobConfiguration job : jobStore.fetchJobs(managerId)) {
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
      return new Response(ResponseCode.ERROR, "No rewrite commands provided.");
    }

    return storage.write(new MutateWork.Quiet<Response>() {
      @Override public Response apply(MutableStoreProvider storeProvider) {
        List<String> errors = Lists.newArrayList();

        for (ConfigRewrite command : request.getRewriteCommands()) {
          switch (command.getSetField()) {
            case JOB_REWRITE:
              JobConfigRewrite jobRewrite = command.getJobRewrite();
              JobConfiguration existingJob = jobRewrite.getOldJob();
              JobConfiguration rewrittenJob;
              try {
                rewrittenJob =
                    ConfigurationManager.validateAndPopulate(jobRewrite.getRewrittenJob());
              } catch (TaskDescriptionException e) {
                // We could add an error here, but this is probably a hint of something wrong in
                // the client that's causing a bad configuration to be applied.
                throw Throwables.propagate(e);
              }
              if (!existingJob.getKey().equals(rewrittenJob.getKey())) {
                errors.add("Disallowing rewrite attempting to change job key.");
              } else if (!existingJob.getName().equals(rewrittenJob.getName())) {
                errors.add("Disallowing rewrite attempting to change job name.");
              } else if (!existingJob.getOwner().equals(rewrittenJob.getOwner())) {
                errors.add("Disallowing rewrite attempting to change job owner.");
              } else {
                JobStore.Mutable jobStore = storeProvider.getJobStore();
                Multimap<String, JobConfiguration> matches =
                    jobsByKey(jobStore, existingJob.getKey());
                switch (matches.size()) {
                  case 0:
                    errors.add("No jobs found for key " + JobKeys.toPath(existingJob));
                    break;

                  case 1:
                    Map.Entry<String, JobConfiguration> match =
                        Iterables.getOnlyElement(matches.entries());
                    JobConfiguration storedJob = match.getValue();
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

            case SHARD_REWRITE:
              ShardConfigRewrite shardRewrite = command.getShardRewrite();
              ShardKey shardKey = shardRewrite.getShardKey();
              Iterable<ScheduledTask> tasks = storeProvider.getTaskStore().fetchTasks(
                  Query.shardScoped(shardKey.getJobKey(), shardKey.getShardId()).active());
              Optional<AssignedTask> task =
                  Optional.fromNullable(Iterables.getOnlyElement(tasks, null))
                      .transform(Tasks.SCHEDULED_TO_ASSIGNED);
              if (!task.isPresent()) {
                errors.add("No active task found for " + shardKey);
              } else if (!task.get().getTask().equals(shardRewrite.getOldTask())) {
                errors.add("CAS compare failed for " + shardKey);
              } else {
                TaskConfig newConfiguration =
                    ConfigurationManager.applyDefaultsIfUnset(shardRewrite.getRewrittenTask());
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
  public APIVersion getVersion() {
    return CURRENT_API_VERSION;
  }

  @VisibleForTesting
  static Optional<String> transitionMessage(String user) {
    return Optional.of("Transition forced by " + user);
  }
}
