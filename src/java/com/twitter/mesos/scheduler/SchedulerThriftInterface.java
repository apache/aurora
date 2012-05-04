package com.twitter.mesos.scheduler;

import java.util.Set;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotEmpty;
import com.twitter.common.base.Supplier;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.BackoffHelper;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.CreateJobResponse;
import com.twitter.mesos.gen.FinishUpdateResponse;
import com.twitter.mesos.gen.ForceTaskStateResponse;
import com.twitter.mesos.gen.GetQuotaResponse;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.KillResponse;
import com.twitter.mesos.gen.MesosAdmin;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ResponseCode;
import com.twitter.mesos.gen.RestartResponse;
import com.twitter.mesos.gen.RollbackShardsResponse;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduleStatusResponse;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.SessionKey;
import com.twitter.mesos.gen.SetQuotaResponse;
import com.twitter.mesos.gen.StartCronResponse;
import com.twitter.mesos.gen.StartUpdateResponse;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateResponseCode;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.gen.UpdateShardsResponse;
import com.twitter.mesos.scheduler.SchedulerCore.RestartException;
import com.twitter.mesos.scheduler.auth.SessionValidator;
import com.twitter.mesos.scheduler.auth.SessionValidator.AuthFailedException;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.quota.QuotaManager;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.gen.ResponseCode.AUTH_FAILED;
import static com.twitter.mesos.gen.ResponseCode.INVALID_REQUEST;
import static com.twitter.mesos.gen.ResponseCode.OK;
import static com.twitter.mesos.gen.ResponseCode.WARNING;
import static com.twitter.mesos.gen.ScheduleStatus.ROLLBACK;
import static com.twitter.mesos.gen.ScheduleStatus.UPDATING;

/**
 * Mesos scheduler thrift server implementation.
 * Interfaces between mesos users and the scheduler core to perform cluster administration tasks.
 *
 * @author William Farner
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

  private static final String NOT_ADMIN_MESSAGE = "Only admins may perform this operation.";

  private static final Function<ScheduledTask, String> GET_ROLE = Functions.compose(
      new Function<TwitterTaskInfo, String>() {
        @Override public String apply(TwitterTaskInfo task) {
          return task.getOwner().getRole();
        }
      },
      Tasks.SCHEDULED_TO_INFO);

  private final SchedulerCore schedulerCore;
  private final SessionValidator sessionValidator;

  private final QuotaManager quotaManager;
  private final Amount<Long, Time> killTaskInitialBackoff;

  private final Amount<Long, Time> killTaskMaxBackoff;

  @Inject
  public SchedulerThriftInterface(SchedulerCore schedulerCore, SessionValidator sessionValidator,
      QuotaManager quotaManager) {
    this(schedulerCore, sessionValidator, quotaManager,
        KILL_TASK_INITIAL_BACKOFF.get(), KILL_TASK_MAX_BACKOFF.get());
  }

  @VisibleForTesting
  SchedulerThriftInterface(SchedulerCore schedulerCore, SessionValidator sessionValidator,
      QuotaManager quotaManager, Amount<Long, Time> initialBackoff, Amount<Long, Time> maxBackoff) {
    this.schedulerCore = checkNotNull(schedulerCore);
    this.sessionValidator = checkNotNull(sessionValidator);
    this.quotaManager = checkNotNull(quotaManager);
    this.killTaskInitialBackoff = checkNotNull(initialBackoff);
    this.killTaskMaxBackoff = checkNotNull(maxBackoff);
  }

  private void validateSessionKeyForTasks(SessionKey session, TaskQuery taskQuery)
      throws AuthFailedException {
    Set<ScheduledTask> tasks = schedulerCore.getTasks(taskQuery);
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

    try {
      sessionValidator.checkAuthenticated(session, job.getOwner().getRole());
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
      return response;
    }

    try {
      schedulerCore.createJob(job);
      response.setResponseCode(OK)
          .setMessage(String.format("%d new tasks pending for job %s",
              job.getTaskConfigs().size(), Tasks.jobKey(job)));
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

    Set<ScheduledTask> tasks = schedulerCore.getTasks(query);

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
    checkNotNull(query);
    checkNotNull(session);
    checkNotNull(session.getUser());

    // TODO(wfarner): Determine whether this is a useful function, or if it should simply be
    //     switched to 'killJob'.
    LOG.info("Received kill request for tasks: " + query);
    KillResponse response = new KillResponse();

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
          if (schedulerCore.getTasks(activeQuery).isEmpty()) {
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
      response.setResponseCode(ResponseCode.ERROR).setMessage("killTasks thread was interrupted.");
    } catch (BackoffHelper.BackoffStoppedException e) {
      response.setResponseCode(ResponseCode.ERROR).setMessage("Tasks were not killed in time.");
    }
    return response;
  }

  // TODO(William Farner); This should address a job/shard and not task IDs, and should check to
  //     ensure that the shards requested exist (reporting failure and ignoring request otherwise).
  @Override
  public RestartResponse restartTasks(Set<String> taskIds, SessionKey session) {
    checkNotBlank(taskIds);
    checkNotNull(session);

    RestartResponse response = new RestartResponse()
        .setMessage(taskIds.size() + " tasks scheduled for restart.");

    try {
      validateSessionKeyForTasks(session, Query.byId(taskIds));
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
      return response;
    }

    // TODO(wfarner): This is busted, fix when we begin restarting tasks by shard instead of task
    //     ID.
    Set<String> tasksRestarting = null;
    try {
      schedulerCore.restartTasks(taskIds);
    } catch (RestartException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    }
    if (!taskIds.equals(tasksRestarting)) {
      response.setResponseCode(WARNING)
          .setMessage("Unable to restart tasks: " + Sets.difference(taskIds, tasksRestarting));
    }

    return response;
  }

  @Override
  public StartUpdateResponse startUpdate(JobConfiguration job, SessionKey session) {
    checkNotNull(job);
    checkNotNull(session);

    String jobKey = Tasks.jobKey(job);
    LOG.info("Received update request for tasks: " + jobKey);
    StartUpdateResponse response = new StartUpdateResponse();
    try {
      sessionValidator.checkAuthenticated(session, job.getOwner().getRole());
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
      return response;
    }

    // If any tasks for the job are already in UPDATING or ROLLBACK, this call should be rejected.
    TaskQuery query =
        new TaskQuery().setJobKey(jobKey).setStatuses(ImmutableSet.of(UPDATING, ROLLBACK));
    if (!getTasksStatus(query).getTasks().isEmpty()) {
      response
          .setResponseCode(INVALID_REQUEST)
          .setMessage("Update/Rollback already in progress for " + jobKey);
      return response;
    }

    try {
      response.setUpdateToken(schedulerCore.startUpdate(job));
      response.setResponseCode(OK).setMessage("Update successfully started.");
    } catch (ScheduleException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    } catch (ConfigurationManager.TaskDescriptionException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public UpdateShardsResponse updateShards(String role, String jobName,
      Set<Integer> shards, String updateToken, SessionKey session) {
    checkNotBlank(role);
    checkNotBlank(jobName);
    checkNotBlank(shards);
    checkNotBlank(updateToken);
    checkNotNull(session);

    UpdateShardsResponse response = new UpdateShardsResponse();
    try {
      schedulerCore.updateShards(role, jobName, shards, updateToken);
      response.setResponseCode(UpdateResponseCode.OK).
          setMessage("Successfully started update of shards: " + shards);
    } catch (ScheduleException e) {
      response.setResponseCode(UpdateResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public RollbackShardsResponse rollbackShards(String role, String jobName,
      Set<Integer> shards, String updateToken, SessionKey session) {
    checkNotBlank(role);
    checkNotBlank(jobName);
    checkNotBlank(shards);
    checkNotBlank(updateToken);
    checkNotNull(session);

    RollbackShardsResponse response = new RollbackShardsResponse();
    try {
      schedulerCore.rollbackShards(role, jobName, shards, updateToken);
      response.setResponseCode(UpdateResponseCode.OK)
          .setMessage("Successfully started rollback of shards: " + shards);
    } catch (ScheduleException e) {
      response.setResponseCode(UpdateResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public FinishUpdateResponse finishUpdate(String role, String jobName,
      UpdateResult updateResult, String updateToken, SessionKey session) {
    checkNotBlank(role);
    checkNotBlank(jobName);
    checkNotNull(session);

    FinishUpdateResponse response = new FinishUpdateResponse();
    try {
      schedulerCore.finishUpdate(role, jobName,
          updateResult == UpdateResult.TERMINATE
              ? Optional.<String>absent() : Optional.of(updateToken),
          updateResult);
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
      response.setResponseCode(ResponseCode.AUTH_FAILED).setMessage(NOT_ADMIN_MESSAGE);
    }

    return response;
  }

  @Override
  public ForceTaskStateResponse forceTaskState(String taskId, ScheduleStatus status,
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
      response.setResponseCode(ResponseCode.AUTH_FAILED).setMessage(NOT_ADMIN_MESSAGE);
    }

    return response;
  }

  @VisibleForTesting
  static String transitionMessage(String user) {
    return "Transition forced by " + user;
  }
}
