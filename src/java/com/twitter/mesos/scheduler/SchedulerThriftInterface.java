package com.twitter.mesos.scheduler;

import java.util.Set;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotEmpty;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.CreateJobResponse;
import com.twitter.mesos.gen.FinishUpdateResponse;
import com.twitter.mesos.gen.GetQuotaResponse;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.KillResponse;
import com.twitter.mesos.gen.MesosAdmin;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ResponseCode;
import com.twitter.mesos.gen.RestartResponse;
import com.twitter.mesos.gen.RollbackShardsResponse;
import com.twitter.mesos.gen.ScheduleStatusResponse;
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
      help ="Auth role that is premitted to run administrative functions.")
  static final Arg<String> ADMIN_ROLE = Arg.create("mesos");

  private static final Logger LOG = Logger.getLogger(SchedulerThriftInterface.class.getName());

  private final SchedulerCore schedulerCore;
  private final SessionValidator sessionValidator;
  private final QuotaManager quotaManager;

  @Inject
  public SchedulerThriftInterface(SchedulerCore schedulerCore, SessionValidator sessionValidator,
      QuotaManager quotaManager) {
    this.schedulerCore = checkNotNull(schedulerCore);
    this.sessionValidator = checkNotNull(sessionValidator);
    this.quotaManager = checkNotNull(quotaManager);
  }

  private void validateSessionKeyForTasks(SessionKey session, Query taskQuery)
      throws AuthFailedException {
    Set<TaskState> tasks = schedulerCore.getTasks(taskQuery);
    for (String role : ImmutableSet.copyOf(Iterables.transform(tasks, GET_ROLE))) {
      sessionValidator.checkAuthenticated(session, role);
    }
  }

  @Override
  public CreateJobResponse createJob(JobConfiguration job, SessionKey session) {
    checkNotNull(job);
    checkNotNull(session, "Session must be set.");

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
    checkNotBlank(role, "Role must not be blank.");
    checkNotBlank(jobName, "Job name must not be blank.");
    checkNotNull(session, "Session must be provided.");

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

    Set<TaskState> tasks = schedulerCore.getTasks(new Query(query));

    ScheduleStatusResponse response = new ScheduleStatusResponse();
    if (tasks.isEmpty()) {
      response.setResponseCode(INVALID_REQUEST)
          .setMessage("No tasks found for query: " + query);
    } else {
      response.setResponseCode(OK)
          .setTasks(Lists.newArrayList(Iterables.transform(tasks, TaskState.STATE_TO_LIVE)));
    }

    return response;
  }

  private static final Function<TaskState, String> GET_ROLE = Functions.compose(
      new Function<TwitterTaskInfo, String>() {
        @Override public String apply(TwitterTaskInfo task) {
          return task.getOwner().getRole();
        }
      },
      TaskState.STATE_TO_INFO);

  @Override
  public KillResponse killTasks(TaskQuery query, SessionKey session) {
    checkNotNull(query);
    checkNotNull(session, "Session must be set.");
    checkNotNull(session.getUser(), "Session user must be set.");

    // TODO(wfarner): Determine whether this is a useful function, or if it should simply be
    //     switched to 'killJob'.

    LOG.info("Received kill request for tasks: " + query);
    KillResponse response = new KillResponse();

    try {
      validateSessionKeyForTasks(session, new Query(query));
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
      return response;
    }

    try {
      schedulerCore.killTasks(new Query(query));
      response.setResponseCode(OK).setMessage("Tasks will be killed.");
    } catch (ScheduleException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }


  // TODO(William Farner); This should address a job/shard and not task IDs, and should check to
  //     ensure that the shards requested exist (reporting failure and ignoring request otherwise).
  @Override
  public RestartResponse restartTasks(Set<String> taskIds, SessionKey session) {
    checkNotBlank(taskIds, "At least one task ID must be provided.");
    checkNotNull(session, "Session must be set.");

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
      response.setResponseCode(INVALID_REQUEST)
          .setMessage(e.getMessage());
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
    checkNotNull(session, "Session must be set.");

    LOG.info("Received update request for tasks: " + Tasks.jobKey(job));
    StartUpdateResponse response = new StartUpdateResponse();
    try {
      sessionValidator.checkAuthenticated(session, job.getOwner().getRole());
    } catch (AuthFailedException e) {
      response.setResponseCode(AUTH_FAILED).setMessage(e.getMessage());
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
    checkNotBlank(role, "Role may not be blank.");
    checkNotBlank(jobName, "Job may not be blank.");
    checkNotBlank(shards, "At least one shard must be specified.");
    checkNotBlank(updateToken, "Update token may not be blank.");
    checkNotNull(session, "Session must be set.");

    UpdateShardsResponse response = new UpdateShardsResponse();
    try {
      schedulerCore.updateShards(role, jobName, shards, updateToken);
      response.setResponseCode(UpdateResponseCode.OK).
          setMessage("Successful update of shards: " + shards);
    } catch (ScheduleException e) {
      response.setResponseCode(UpdateResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public RollbackShardsResponse rollbackShards(String role, String jobName,
      Set<Integer> shards, String updateToken, SessionKey session) {
    checkNotBlank(role, "Role may not be blank.");
    checkNotBlank(jobName, "Job may not be blank.");
    checkNotBlank(shards, "At least one shard must be specified.");
    checkNotBlank(updateToken, "Update token may not be blank.");
    checkNotNull(session, "Session must be set.");

    RollbackShardsResponse response = new RollbackShardsResponse();
    try {
      schedulerCore.rollbackShards(role, jobName, shards, updateToken);
      response.setResponseCode(UpdateResponseCode.OK).
          setMessage("Successful rollback of shards: " + shards);
    } catch (ScheduleException e) {
      response.setResponseCode(UpdateResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public FinishUpdateResponse finishUpdate(String role, String jobName,
      UpdateResult updateResult, String updateToken, SessionKey session) {
    checkNotBlank(role, "Role may not be blank.");
    checkNotBlank(jobName, "Job may not be blank.");
    checkNotNull(session, "Session must be set.");

    FinishUpdateResponse response = new FinishUpdateResponse();
    try {
      schedulerCore.finishUpdate(role, jobName,
          updateResult == UpdateResult.TERMINATE ? null : updateToken, updateResult);
      response.setResponseCode(UpdateResponseCode.OK);
    } catch (ScheduleException e) {
      response.setResponseCode(UpdateResponseCode.INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public GetQuotaResponse getQuota(String ownerRole) {
    checkNotBlank(ownerRole, "Owner role may not be blank.");
    return new GetQuotaResponse().setQuota(quotaManager.getQuota(ownerRole));
  }

  private void assertAdmin(SessionKey session) throws AuthFailedException {
    sessionValidator.checkAuthenticated(session, ADMIN_ROLE.get());
  }

  private static final String NOT_ADMIN_MESSAGE = "Only admins may perform this operation.";

  @Override
  public SetQuotaResponse setQuota(String ownerRole, Quota quota, SessionKey session) {
    checkNotBlank(ownerRole, "Owner role must not be blank.");
    checkNotNull(quota, "Quota must not be null");
    checkNotNull(session, "Session must be set.");

    SetQuotaResponse response = new SetQuotaResponse();
    try {
      assertAdmin(session);
      quotaManager.setQuota(ownerRole, quota);
      response.setResponseCode(ResponseCode.OK).setMessage("Quota applied.");
    } catch (AuthFailedException e) {
      response.setResponseCode(ResponseCode.AUTH_FAILED).setMessage(NOT_ADMIN_MESSAGE);
    }

    return response;
  }
}
