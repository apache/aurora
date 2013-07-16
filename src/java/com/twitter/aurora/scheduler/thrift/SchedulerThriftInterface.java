package com.twitter.aurora.scheduler.thrift;

import java.util.Collection;

import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.commons.lang.StringUtils;

import com.twitter.aurora.scheduler.thrift.auth.CapabilityValidator;
import com.twitter.aurora.scheduler.thrift.auth.CapabilityValidator.Capability;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.base.Supplier;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.BackoffHelper;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.auth.SessionValidator.AuthFailedException;
import com.twitter.mesos.gen.CommitRecoveryResponse;
import com.twitter.mesos.gen.CreateJobResponse;
import com.twitter.mesos.gen.DeleteRecoveryTasksResponse;
import com.twitter.mesos.gen.DrainHostsResponse;
import com.twitter.mesos.gen.EndMaintenanceResponse;
import com.twitter.mesos.gen.FinishUpdateResponse;
import com.twitter.mesos.gen.ForceTaskStateResponse;
import com.twitter.mesos.gen.GetJobUpdatesResponse;
import com.twitter.mesos.gen.GetJobsResponse;
import com.twitter.mesos.gen.GetQuotaResponse;
import com.twitter.mesos.gen.Hosts;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.JobKey;
import com.twitter.mesos.gen.JobUpdateConfiguration;
import com.twitter.mesos.gen.KillResponse;
import com.twitter.mesos.gen.ListBackupsResponse;
import com.twitter.mesos.gen.MaintenanceStatusResponse;
import com.twitter.mesos.gen.PerformBackupResponse;
import com.twitter.mesos.gen.PopulateJobResponse;
import com.twitter.mesos.gen.QueryRecoveryResponse;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ResponseCode;
import com.twitter.mesos.gen.RestartShardsResponse;
import com.twitter.mesos.gen.RewriteConfigsRequest;
import com.twitter.mesos.gen.RewriteConfigsResponse;
import com.twitter.mesos.gen.RollbackShardsResponse;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduleStatusResponse;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.SessionKey;
import com.twitter.mesos.gen.SetQuotaResponse;
import com.twitter.mesos.gen.SnapshotResponse;
import com.twitter.mesos.gen.StageRecoveryResponse;
import com.twitter.mesos.gen.StartCronResponse;
import com.twitter.mesos.gen.StartMaintenanceResponse;
import com.twitter.mesos.gen.StartUpdateResponse;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UnloadRecoveryResponse;
import com.twitter.mesos.gen.UpdateResponseCode;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.gen.UpdateShardsResponse;
import com.twitter.mesos.scheduler.base.JobKeys;
import com.twitter.mesos.scheduler.base.Query;
import com.twitter.mesos.scheduler.base.ScheduleException;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import com.twitter.mesos.scheduler.configuration.ParsedConfiguration;
import com.twitter.mesos.scheduler.quota.QuotaManager;
import com.twitter.mesos.scheduler.state.CronJobManager;
import com.twitter.mesos.scheduler.state.MaintenanceController;
import com.twitter.mesos.scheduler.state.SchedulerCore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.UpdateStore;
import com.twitter.mesos.scheduler.storage.backup.Recovery;
import com.twitter.mesos.scheduler.storage.backup.Recovery.RecoveryException;
import com.twitter.mesos.scheduler.storage.backup.StorageBackup;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.gen.ResponseCode.AUTH_FAILED;
import static com.twitter.mesos.gen.ResponseCode.ERROR;
import static com.twitter.mesos.gen.ResponseCode.INVALID_REQUEST;
import static com.twitter.mesos.gen.ResponseCode.OK;

/**
 * Mesos scheduler thrift server implementation.
 * Interfaces between mesos users and the scheduler core to perform cluster administration tasks.
 */
class SchedulerThriftInterface implements SchedulerController {
  private static final Logger LOG = Logger.getLogger(SchedulerThriftInterface.class.getName());

  @CmdLine(name = "kill_task_initial_backoff",
      help = "Initial backoff delay while waiting for the tasks to transition to KILLED.")
  private static final Arg<Amount<Long, Time>> KILL_TASK_INITIAL_BACKOFF =
      Arg.create(Amount.of(1L, Time.SECONDS));

  @CmdLine(name = "kill_task_max_backoff",
      help = "Max backoff delay while waiting for the tasks to transition to KILLED.")
  private static final Arg<Amount<Long, Time>> KILL_TASK_MAX_BACKOFF =
      Arg.create(Amount.of(30L, Time.SECONDS));

  @CmdLine(name = "enable_job_creation",
      help = "Allow new jobs to be created, if false all job creation requests will be denied.")
  private static final Arg<Boolean> ENABLE_JOB_CREATION = Arg.create(true);

  private static final Function<ScheduledTask, String> GET_ROLE = Functions.compose(
      new Function<TwitterTaskInfo, String>() {
        @Override public String apply(TwitterTaskInfo task) {
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
  public CreateJobResponse createJob(JobConfiguration job, SessionKey session) {
    checkNotNull(job);
    checkNotNull(session);

    CreateJobResponse response = new CreateJobResponse();
    if (!JobKeys.isValid(job.getKey())) {
      return response.setResponseCode(INVALID_REQUEST)
          .setMessage("Invalid job key: " + job.getKey());
    }

    if (!ENABLE_JOB_CREATION.get()) {
      return response.setResponseCode(INVALID_REQUEST)
          .setMessage("Job creation is disabled on this cluster.");
    }

    try {
      sessionValidator.checkAuthenticated(session, job.getOwner().getRole());
    } catch (AuthFailedException e) {
      return response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
    }

    try {
      ParsedConfiguration parsed = ParsedConfiguration.fromUnparsed(job);
      schedulerCore.createJob(parsed);
      response.setResponseCode(OK)
          .setMessage(String.format("%d new tasks pending for job %s",
              parsed.get().getTaskConfigsSize(), Tasks.jobKey(job)));
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
  public PopulateJobResponse populateJobConfig(JobConfiguration description) {
    checkNotNull(description);

    // TODO(ksweeney): check valid JobKey in description after deprecating non-environment version.

    PopulateJobResponse response = new PopulateJobResponse();
    try {
      response.setPopulated(ConfigurationManager.validateAndPopulate(description).getTaskConfigs())
          .setResponseCode(OK)
          .setMessage("Tasks populated");
    } catch (TaskDescriptionException e) {
      response.setResponseCode(INVALID_REQUEST)
          .setMessage("Invalid configuration: " + e.getMessage());
    }
    return response;
  }

  @Override
  public StartCronResponse startCronJob(JobKey jobKey, SessionKey session) {
    checkNotNull(session);
    JobKeys.assertValid(jobKey);

    StartCronResponse response = new StartCronResponse();
    try {
      sessionValidator.checkAuthenticated(session, jobKey.getRole());
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
    }

    return response;
  }

  // TODO(William Farner): Provide status information about cron jobs here.
  @Override
  public ScheduleStatusResponse getTasksStatus(TaskQuery query) {
    checkNotNull(query);

    Set<ScheduledTask> tasks = Storage.Util.weaklyConsistentFetchTasks(storage, query);

    ScheduleStatusResponse response = new ScheduleStatusResponse();
    if (tasks.isEmpty()) {
      response.setResponseCode(INVALID_REQUEST)
          .setMessage("No tasks found for query: " + query);
    } else {
      response.setResponseCode(OK)
          .setTasks(ImmutableList.copyOf(tasks));
    }

    return response;
  }

  private boolean isAdmin(SessionKey session) {
    try {
      sessionValidator.checkAuthorized(session, Capability.ROOT);
      return true;
    } catch (AuthFailedException e) {
      return false;
    }
  }

  @Override
  public GetJobsResponse getJobs(String ownerRole) {
    checkNotNull(ownerRole);

    // Ensure we only return one JobConfiguration for each JobKey.
    Map<JobKey, JobConfiguration> jobs = Maps.newHashMap();

    // Query the task store, find immediate jobs, and synthesize a JobConfiguration for them.
    // This is necessary because the ImmediateJobManager doesn't store jobs directly and
    // ImmediateJobManager#getJobs always returns an empty Collection.
    Multimap<JobKey, ScheduledTask> tasks =  Multimaps.index(
        Storage.Util.weaklyConsistentFetchTasks(storage, Query.roleScoped(ownerRole).active()),
        Tasks.SCHEDULED_TO_JOB_KEY);

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
    jobs.putAll(Maps.uniqueIndex(
        FluentIterable.from(cronJobManager.getJobs())
            .filter(Predicates.compose(Predicates.equalTo(ownerRole), JobKeys.CONFIG_TO_ROLE)),
        JobKeys.FROM_CONFIG));

    return new GetJobsResponse()
        .setConfigs(ImmutableSet.copyOf(jobs.values()))
        .setResponseCode(OK);
  }

  private void validateSessionKeyForTasks(SessionKey session, TaskQuery taskQuery)
      throws AuthFailedException {

    Set<ScheduledTask> tasks = Storage.Util.consistentFetchTasks(storage, taskQuery);
    for (String role : ImmutableSet.copyOf(Iterables.transform(tasks, GET_ROLE))) {
      sessionValidator.checkAuthenticated(session, role);
    }
  }

  @Override
  public KillResponse killTasks(final TaskQuery query, SessionKey session) {
    // TODO(wfarner): Determine whether this is a useful function, or if it should simply be
    //     switched to 'killJob'.

    checkNotNull(query);
    checkNotNull(session);
    checkNotNull(session.getUser());

    LOG.info("Received kill request from " + session.getUser() + "for tasks: " + query);
    KillResponse response = new KillResponse();

    if (query.getJobName() != null && StringUtils.isBlank(query.getJobName())) {
      response.setResponseCode(INVALID_REQUEST).setMessage(
          String.format("Invalid job name: '%s'", query.getJobName()));
      return response;
    }

    if (isAdmin(session)) {
      LOG.info("Granting kill query to admin user: " + query);
    } else {
      try {
        validateSessionKeyForTasks(session, query);
      } catch (AuthFailedException e) {
        response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
        return response;
      }
    }

    try {
      schedulerCore.killTasks(query, session.getUser());
    } catch (ScheduleException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
      return response;
    }

    BackoffHelper backoff = new BackoffHelper(killTaskInitialBackoff, killTaskMaxBackoff, true);
    final TaskQuery activeQuery = query.setStatuses(Tasks.ACTIVE_STATES);
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
  public StartUpdateResponse startUpdate(JobConfiguration job, SessionKey session) {
    checkNotNull(job);
    checkNotNull(session);

    // TODO(ksweeney): check valid JobKey in job after deprecating non-environment version.

    LOG.info("Received update request for tasks: " + Tasks.jobKey(job));
    StartUpdateResponse response = new StartUpdateResponse();
    try {
      sessionValidator.checkAuthenticated(session, job.getOwner().getRole());
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
      return response;
    }

    try {
      Optional<String> token =
          schedulerCore.initiateJobUpdate(ParsedConfiguration.fromUnparsed(job));
      response.setResponseCode(OK);
      response.setRollingUpdateRequired(token.isPresent());
      if (token.isPresent()) {
        response.setUpdateToken(token.get());
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

    // TODO(ksweeney): Validate session key here

    UpdateShardsResponse response = new UpdateShardsResponse();
    try {
      response
          .setShards(schedulerCore.updateShards(jobKey, session.getUser(), shards, updateToken))
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

    // TODO(ksweeney): Validate session key here

    RollbackShardsResponse response = new RollbackShardsResponse();
    try {
      response
          .setShards(schedulerCore.rollbackShards(jobKey, session.getUser(), shards, updateToken))
          .setResponseCode(UpdateResponseCode.OK)
          .setMessage("Successfully started rollback of shards: " + shards);
    } catch (ScheduleException e) {
      response.setResponseCode(UpdateResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public FinishUpdateResponse finishUpdate(
      JobKey jobKey,
      UpdateResult updateResult,
      String updateToken,
      SessionKey session) {

    JobKeys.assertValid(jobKey);
    checkNotNull(session);

    // TODO(ksweeney): Validate session key here

    FinishUpdateResponse response = new FinishUpdateResponse();
    Optional<String> token = updateResult == UpdateResult.TERMINATE
        ? Optional.<String>absent() : Optional.of(updateToken);
    try {
      schedulerCore.finishUpdate(jobKey, session.getUser(), token, updateResult);
      response.setResponseCode(OK).setMessage("Update successfully finished.");
    } catch (ScheduleException e) {
      response.setResponseCode(ResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public RestartShardsResponse restartShards(
      JobKey jobKey,
      Set<Integer> shardIds,
      SessionKey session) {

    JobKeys.assertValid(jobKey);
    MorePreconditions.checkNotBlank(shardIds);
    checkNotNull(session);

    RestartShardsResponse response = new RestartShardsResponse();
    try {
      sessionValidator.checkAuthenticated(session, jobKey.getRole());
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
      return response;
    }

    try {
      schedulerCore.restartShards(jobKey, shardIds, session.getUser());
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

  @Override
  public SetQuotaResponse setQuota(String ownerRole, Quota quota, SessionKey session) {
    checkNotBlank(ownerRole);
    checkNotNull(quota);
    checkNotNull(session);

    quotaManager.setQuota(ownerRole, quota);
    return new SetQuotaResponse().setResponseCode(OK).setMessage("Quota applied.");
  }

  @Override
  public ForceTaskStateResponse forceTaskState(
      String taskId,
      ScheduleStatus status,
      SessionKey session) {

    checkNotBlank(taskId);
    checkNotNull(status);
    checkNotNull(session);

    schedulerCore.setTaskStatus(
        Query.byId(taskId), status, transitionMessage(session.getUser()));
    return new ForceTaskStateResponse().setResponseCode(OK).setMessage("Transition attempted.");
  }

  @Override
  public PerformBackupResponse performBackup(SessionKey session) {
    backup.backupNow();
    return new PerformBackupResponse().setResponseCode(OK);
  }

  @Override
  public ListBackupsResponse listBackups(SessionKey session) {
    return new ListBackupsResponse()
        .setBackups(recovery.listBackups())
        .setResponseCode(OK);
  }

  @Override
  public StageRecoveryResponse stageRecovery(String backupId, SessionKey session) {
    StageRecoveryResponse response = new StageRecoveryResponse().setResponseCode(OK);
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
      response.setTasks(recovery.query(query));
    } catch (RecoveryException e) {
      response.setResponseCode(ERROR).setMessage(e.getMessage());
      LOG.log(Level.WARNING, "Failed to query recovery: " + e, e);
    }

    return response;
  }

  @Override
  public DeleteRecoveryTasksResponse deleteRecoveryTasks(TaskQuery query, SessionKey session) {
    DeleteRecoveryTasksResponse response = new DeleteRecoveryTasksResponse().setResponseCode(OK);
    try {
      recovery.deleteTasks(query);
    } catch (RecoveryException e) {
      response.setResponseCode(ERROR).setMessage(e.getMessage());
      LOG.log(Level.WARNING, "Failed to delete recovery tasks: " + e, e);
    }

    return response;
  }

  @Override
  public CommitRecoveryResponse commitRecovery(SessionKey session) {
    CommitRecoveryResponse response = new CommitRecoveryResponse().setResponseCode(OK);
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
  public UnloadRecoveryResponse unloadRecovery(SessionKey session) {
    recovery.unload();
    return new UnloadRecoveryResponse().setResponseCode(OK);
  }

  @Override
  public SnapshotResponse snapshot(SessionKey session) {
    SnapshotResponse response = new SnapshotResponse();
    try {
      storage.snapshot();
      return response.setResponseCode(OK).setMessage("Compaction successful.");
    } catch (Storage.StorageException e) {
      LOG.log(Level.WARNING, "Requested snapshot failed.", e);
      return response.setResponseCode(ERROR).setMessage(e.getMessage());
    }
  }

  @Override
  public RewriteConfigsResponse rewriteConfigs(RewriteConfigsRequest request, SessionKey session) {
    // TODO(William Farner): Implement this.
    return new RewriteConfigsResponse()
        .setMessage("Not implemented")
        .setResponseCode(ResponseCode.ERROR);
  }

  @VisibleForTesting
  static Optional<String> transitionMessage(String user) {
    return Optional.of("Transition forced by " + user);
  }
}
