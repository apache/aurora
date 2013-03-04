package com.twitter.mesos.scheduler;

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import org.apache.commons.lang.StringUtils;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotEmpty;
import com.twitter.common.base.Supplier;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.BackoffHelper;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.auth.SessionValidator;
import com.twitter.mesos.auth.SessionValidator.AuthFailedException;
import com.twitter.mesos.gen.CommitRecoveryResponse;
import com.twitter.mesos.gen.CreateJobResponse;
import com.twitter.mesos.gen.DeleteRecoveryTasksResponse;
import com.twitter.mesos.gen.DrainHostsResponse;
import com.twitter.mesos.gen.EndMaintenanceResponse;
import com.twitter.mesos.gen.FinishUpdateResponse;
import com.twitter.mesos.gen.ForceTaskStateResponse;
import com.twitter.mesos.gen.GetQuotaResponse;
import com.twitter.mesos.gen.Hosts;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.KillResponse;
import com.twitter.mesos.gen.ListBackupsResponse;
import com.twitter.mesos.gen.MaintenanceStatusResponse;
import com.twitter.mesos.gen.MesosAdmin;
import com.twitter.mesos.gen.PerformBackupResponse;
import com.twitter.mesos.gen.PopulateJobResponse;
import com.twitter.mesos.gen.QueryRecoveryResponse;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ResponseCode;
import com.twitter.mesos.gen.RollbackShardsResponse;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduleStatusResponse;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.SessionKey;
import com.twitter.mesos.gen.SetQuotaResponse;
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
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import com.twitter.mesos.scheduler.quota.QuotaManager;
import com.twitter.mesos.scheduler.storage.Storage;
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
public class SchedulerThriftInterface implements MesosAdmin.Iface {
  @VisibleForTesting
  @NotEmpty
  @CmdLine(name = "admin_role",
      help = "Auth role that is premitted to run administrative functions.")
  static final Arg<String> ADMIN_ROLE = Arg.create("mesos");

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

  private static final String NOT_ADMIN_MESSAGE = "Only admins may perform this operation.";

  private static final Function<ScheduledTask, String> GET_ROLE = Functions.compose(
      new Function<TwitterTaskInfo, String>() {
        @Override public String apply(TwitterTaskInfo task) {
          return task.getOwner().getRole();
        }
      },
      Tasks.SCHEDULED_TO_INFO);

  private final Storage storage;
  private final SchedulerCore schedulerCore;
  private final SessionValidator sessionValidator;
  private final QuotaManager quotaManager;
  private final StorageBackup backup;
  private final Recovery recovery;
  private final MaintenanceController maintenance;
  private final Amount<Long, Time> killTaskInitialBackoff;
  private final Amount<Long, Time> killTaskMaxBackoff;

  @Inject
  SchedulerThriftInterface(
      Storage storage,
      SchedulerCore schedulerCore,
      SessionValidator sessionValidator,
      QuotaManager quotaManager,
      StorageBackup backup,
      Recovery recovery,
      MaintenanceController maintenance) {

    this(storage,
        schedulerCore,
        sessionValidator,
        quotaManager,
        backup,
        recovery,
        maintenance,
        KILL_TASK_INITIAL_BACKOFF.get(),
        KILL_TASK_MAX_BACKOFF.get());
  }

  @VisibleForTesting
  SchedulerThriftInterface(
      Storage storage,
      SchedulerCore schedulerCore,
      SessionValidator sessionValidator,
      QuotaManager quotaManager,
      StorageBackup backup,
      Recovery recovery,
      MaintenanceController maintenance,
      Amount<Long, Time> initialBackoff,
      Amount<Long, Time> maxBackoff) {

    this.storage = checkNotNull(storage);
    this.schedulerCore = checkNotNull(schedulerCore);
    this.sessionValidator = checkNotNull(sessionValidator);
    this.quotaManager = checkNotNull(quotaManager);
    this.backup = checkNotNull(backup);
    this.recovery = checkNotNull(recovery);
    this.maintenance = checkNotNull(maintenance);
    this.killTaskInitialBackoff = checkNotNull(initialBackoff);
    this.killTaskMaxBackoff = checkNotNull(maxBackoff);
  }

  private void validateSessionKeyForTasks(SessionKey session, TaskQuery taskQuery)
      throws AuthFailedException {
    Set<ScheduledTask> tasks = Storage.Util.fetchTasks(storage, taskQuery);
    for (String role : ImmutableSet.copyOf(Iterables.transform(tasks, GET_ROLE))) {
      sessionValidator.checkAuthenticated(session, role);
    }
  }

  @Override
  public CreateJobResponse createJob(JobConfiguration job, SessionKey session) {
    checkNotNull(job);
    checkNotNull(session);

    LOG.info("Received createJob request: " + Tasks.jobKey(job));
    CreateJobResponse response = new CreateJobResponse();

    if (!ENABLE_JOB_CREATION.get()) {
      return response.setResponseCode(INVALID_REQUEST)
          .setMessage("Job creation is disabled on this cluster.");
    }

    try {
      sessionValidator.checkAuthenticated(session, job.getOwner().getRole());
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
      return response;
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
  public StartCronResponse startCronJob(String role, String jobName, SessionKey session) {
    checkNotBlank(role);
    checkNotBlank(jobName);
    checkNotNull(session);

    StartCronResponse response = new StartCronResponse();
    try {
      sessionValidator.checkAuthenticated(session, role);
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
      return response;
    }

    try {
      schedulerCore.startCronJob(role, jobName);
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

    Set<ScheduledTask> tasks = Storage.Util.fetchTasks(storage, query);

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
          if (Storage.Util.fetchTasks(storage, activeQuery).isEmpty()) {
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
      String role,
      String jobName,
      Set<Integer> shards,
      String updateToken,
      SessionKey session) {

    checkNotBlank(role);
    checkNotBlank(jobName);
    checkNotBlank(shards);
    checkNotBlank(updateToken);
    checkNotNull(session);

    Identity identity = new Identity(role, session.getUser());
    UpdateShardsResponse response = new UpdateShardsResponse();
    try {
      response.setShards(schedulerCore.updateShards(identity, jobName, shards, updateToken))
          .setResponseCode(UpdateResponseCode.OK)
          .setMessage("Successfully started update of shards: " + shards);
    } catch (ScheduleException e) {
      response.setResponseCode(UpdateResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public RollbackShardsResponse rollbackShards(
      String role,
      String jobName,
      Set<Integer> shards,
      String updateToken,
      SessionKey session) {

    checkNotBlank(role);
    checkNotBlank(jobName);
    checkNotBlank(shards);
    checkNotBlank(updateToken);
    checkNotNull(session);

    Identity identity = new Identity(role, session.getUser());
    RollbackShardsResponse response = new RollbackShardsResponse();
    try {
      response.setShards(schedulerCore.rollbackShards(identity, jobName, shards, updateToken))
          .setResponseCode(UpdateResponseCode.OK)
          .setMessage("Successfully started rollback of shards: " + shards);
    } catch (ScheduleException e) {
      response.setResponseCode(UpdateResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public FinishUpdateResponse finishUpdate(
      String role,
      String jobName,
      UpdateResult updateResult,
      String updateToken,
      SessionKey session) {

    checkNotBlank(role);
    checkNotBlank(jobName);
    checkNotNull(session);

    FinishUpdateResponse response = new FinishUpdateResponse();
    Identity identity = new Identity(role, session.getUser());
    Optional<String> token = updateResult == UpdateResult.TERMINATE
        ? Optional.<String>absent() : Optional.of(updateToken);
    try {
      schedulerCore.finishUpdate(identity, jobName, token, updateResult);
      response.setResponseCode(OK).setMessage("Update successfully finished.");
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
    StartMaintenanceResponse response = new StartMaintenanceResponse();
    try {
      assertAdmin(session);
      response.setStatuses(maintenance.startMaintenance(hosts.getHostNames()));
      response.setResponseCode(OK);
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(NOT_ADMIN_MESSAGE);
    }

    return response;
  }

  @Override
  public DrainHostsResponse drainHosts(Hosts hosts, SessionKey session) {
    DrainHostsResponse response = new DrainHostsResponse();
    try {
      assertAdmin(session);
      response.setStatuses(maintenance.drain(hosts.getHostNames()));
      response.setResponseCode(OK);
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(NOT_ADMIN_MESSAGE);
    }

    return response;
  }

  @Override
  public MaintenanceStatusResponse maintenanceStatus(Hosts hosts, SessionKey session) {
    MaintenanceStatusResponse response = new MaintenanceStatusResponse();
    try {
      assertAdmin(session);
      response.setStatuses(maintenance.getStatus(hosts.getHostNames()));
      response.setResponseCode(OK);
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(NOT_ADMIN_MESSAGE);
    }

    return response;
  }

  @Override
  public EndMaintenanceResponse endMaintenance(Hosts hosts, SessionKey session) {
    EndMaintenanceResponse response = new EndMaintenanceResponse();
    try {
      assertAdmin(session);
      response.setStatuses(maintenance.endMaintenance(hosts.getHostNames()));
      response.setResponseCode(OK);
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(NOT_ADMIN_MESSAGE);
    }

    return response;
  }

  private void assertAdmin(SessionKey session) throws AuthFailedException {
    sessionValidator.checkAuthenticated(session, ADMIN_ROLE.get());
  }

  private boolean isAdmin(SessionKey session) {
    try {
      assertAdmin(session);
      return true;
    } catch (AuthFailedException e) {
      return false;
    }
  }

  @Override
  public SetQuotaResponse setQuota(String ownerRole, Quota quota, SessionKey session) {
    checkNotBlank(ownerRole);
    checkNotNull(quota);
    checkNotNull(session);

    SetQuotaResponse response = new SetQuotaResponse();
    try {
      assertAdmin(session);
      quotaManager.setQuota(ownerRole, quota);
      response.setResponseCode(OK).setMessage("Quota applied.");
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(NOT_ADMIN_MESSAGE);
    }

    return response;
  }

  @Override
  public ForceTaskStateResponse forceTaskState(
      String taskId,
      ScheduleStatus status,
      SessionKey session) {

    checkNotBlank(taskId);
    checkNotNull(status);
    checkNotNull(session);

    ForceTaskStateResponse response = new ForceTaskStateResponse();
    try {
      assertAdmin(session);
      schedulerCore.setTaskStatus(Query.byId(taskId), status, transitionMessage(session.getUser()));
      response.setResponseCode(OK).setMessage("Transition attempted.");
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(NOT_ADMIN_MESSAGE);
    }

    return response;
  }

  @Override
  public PerformBackupResponse performBackup(SessionKey session) {
    PerformBackupResponse response = new PerformBackupResponse().setResponseCode(OK);
    try {
      assertAdmin(session);
      backup.backupNow();
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(NOT_ADMIN_MESSAGE);
    }

    return response;
  }

  @Override
  public ListBackupsResponse listBackups(SessionKey session) {
    ListBackupsResponse response = new ListBackupsResponse().setResponseCode(OK);
    try {
      assertAdmin(session);
      response.setBackups(recovery.listBackups());
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(NOT_ADMIN_MESSAGE);
    }

    return response;
  }

  @Override
  public StageRecoveryResponse stageRecovery(String backupId, SessionKey session) {
    StageRecoveryResponse response = new StageRecoveryResponse().setResponseCode(OK);
    try {
      assertAdmin(session);
      recovery.stage(backupId);
    } catch (AuthFailedException e) {
      response.setResponseCode(ResponseCode.AUTH_FAILED).setMessage(NOT_ADMIN_MESSAGE);
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
      assertAdmin(session);
      response.setTasks(recovery.query(query));
    } catch (AuthFailedException e) {
      response.setResponseCode(ResponseCode.AUTH_FAILED).setMessage(NOT_ADMIN_MESSAGE);
      LOG.log(Level.WARNING, "Failed to query recovery: " + e, e);
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
      assertAdmin(session);
      recovery.deleteTasks(query);
    } catch (AuthFailedException e) {
      response.setResponseCode(ResponseCode.AUTH_FAILED).setMessage(NOT_ADMIN_MESSAGE);
    } catch (RecoveryException e) {
      response.setResponseCode(ERROR).setMessage(e.getMessage());
      LOG.log(Level.WARNING, "Failed to delete recovery tasks: " + e, e);
    }

    return response;
  }

  @Override
  public CommitRecoveryResponse commitRecovery(SessionKey session) {
    // TODO(William Farner): As it stands, this is not be enough to apply the backup in a way
    //                       such that all state is internally-consistent.  For example, cron jobs
    //                       will not reflect changes.  The safest route may be to
    //                       commit, write a snapshot to the distributed log, then commit suicide.
    CommitRecoveryResponse response = new CommitRecoveryResponse().setResponseCode(OK);
    try {
      assertAdmin(session);
      recovery.commit();
    } catch (AuthFailedException e) {
      response.setResponseCode(ResponseCode.AUTH_FAILED).setMessage(NOT_ADMIN_MESSAGE);
    } catch (RecoveryException e) {
      response.setResponseCode(ERROR).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public UnloadRecoveryResponse unloadRecovery(SessionKey session) {
    UnloadRecoveryResponse response = new UnloadRecoveryResponse().setResponseCode(OK);
    try {
      assertAdmin(session);
      recovery.unload();
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(NOT_ADMIN_MESSAGE);
    }

    return response;
  }

  @VisibleForTesting
  static Optional<String> transitionMessage(String user) {
    return Optional.of("Transition forced by " + user);
  }
}
