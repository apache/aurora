package com.twitter.mesos.scheduler;

import java.util.Set;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.commons.lang.StringUtils;

import com.twitter.common.collections.Pair;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;
import com.twitter.common_internal.ldap.Ods;
import com.twitter.common_internal.ldap.User;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.CreateJobResponse;
import com.twitter.mesos.gen.FinishUpdateResponse;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.KillResponse;
import com.twitter.mesos.gen.MesosSchedulerManager;
import com.twitter.mesos.gen.ResponseCode;
import com.twitter.mesos.gen.RestartResponse;
import com.twitter.mesos.gen.RollbackShardsResponse;
import com.twitter.mesos.gen.UpdateResponseCode;
import com.twitter.mesos.gen.ScheduleStatusResponse;
import com.twitter.mesos.gen.SessionKey;
import com.twitter.mesos.gen.StartCronResponse;
import com.twitter.mesos.gen.StartUpdateResponse;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.gen.UpdateShardsResponse;
import com.twitter.mesos.scheduler.SchedulerCore.RestartException;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.identity.AuthorizedKeySet;
import com.twitter.mesos.scheduler.identity.AuthorizedKeySet.KeyParseException;

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
public class SchedulerThriftInterface implements MesosSchedulerManager.Iface {
  private static final Logger LOG = Logger.getLogger(SchedulerThriftInterface.class.getName());
  private static final Amount<Long, Time> MAXIMUM_NONCE_DRIFT = Amount.of(10L, Time.SECONDS);

  private final SchedulerCore schedulerCore;
  private final Ods ods;
  private final Clock clock;

  @Inject
  public SchedulerThriftInterface(SchedulerCore schedulerCore, Ods ods, Clock clock) {
    this.schedulerCore = checkNotNull(schedulerCore);
    this.ods = checkNotNull(ods);
    this.clock = checkNotNull(clock);
  }

  /**
   * Given a sessionKey, determine the response type and provide human-readable error message.
   */
  private Pair<ResponseCode, String> validateSessionKey(SessionKey sessionKey,
      String targetRole) {
    if (StringUtils.isBlank(sessionKey.getUser())
        || !sessionKey.isSetNonce()
        || !sessionKey.isSetNonceSig()) {
      return Pair.of(AUTH_FAILED, "Incorrectly specified session key.");
    }

    long now = this.clock.nowMillis();
    long diff = Math.abs(now - sessionKey.getNonce());
    if (Amount.of(diff, Time.MILLISECONDS).compareTo(MAXIMUM_NONCE_DRIFT) > 0) {
      return Pair.of(AUTH_FAILED, "Session key nonce expired.");
    }

    String userId = sessionKey.getUser();
    if (!userId.equals(targetRole)) {
      if (!ods.isRoleAccount(targetRole)) {
        return Pair.of(AUTH_FAILED,
            String.format("%s is not a role account.", targetRole));
      } else if (!ods.isMember(userId, targetRole)) {
        return Pair.of(AUTH_FAILED,
            String.format("User %s does not have permission for role %s", userId, targetRole));
      }
    }

    User user = ods.getUser(userId);
    if (user == null) {
      return Pair.of(AUTH_FAILED, String.format("User %s not found.", userId));
    }

    AuthorizedKeySet keySet;
    try {
      keySet = AuthorizedKeySet.createFromKeys(user.getSshPubkeys());
    } catch (KeyParseException e) {
      return Pair.of(AUTH_FAILED, "Failed to parse SSH keys for user " + userId);
    }

    if (!keySet.verify(
        Long.toString(sessionKey.getNonce()).getBytes(),
        sessionKey.getNonceSig())) {
      return Pair.of(AUTH_FAILED, "Authentication failed for " + userId);
    }

    return Pair.of(OK, "");
  }

  /**
   * Validate the session key against the roles of a set of tasks.
   */
  private Pair<ResponseCode, String> validateSessionKey(SessionKey session, Set<String> taskIds) {
    Set<TaskState> tasks = schedulerCore.getTasks(Query.byId(taskIds));
    for (String role : ImmutableSet.copyOf(Iterables.transform(tasks, GET_ROLE))) {
      Pair<ResponseCode, String> rc = validateSessionKey(session, role);
      if (rc.getFirst() != OK) {
        return rc;
      }
    }

    return Pair.of(OK, "");
  }

  @Override
  public CreateJobResponse createJob(JobConfiguration job, SessionKey session) {
    checkNotNull(job);
    checkNotNull(session, "Session must be set.");

    LOG.info("Received createJob request: " + Tasks.jobKey(job));
    CreateJobResponse response = new CreateJobResponse();

    Pair<ResponseCode, String> rc = validateSessionKey(session, job.getOwner().getRole());
    if (rc.getFirst() != OK) {
      response.setResponseCode(rc.getFirst()).setMessage(rc.getSecond());
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
    Pair<ResponseCode, String> rc = validateSessionKey(session, role);

    if (rc.getFirst() != OK) {
      response.setResponseCode(rc.getFirst()).setMessage(rc.getSecond());
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

    LOG.info("Received kill request for tasks: " + query);
    KillResponse response = new KillResponse();

    Set<TaskState> tasks = schedulerCore.getTasks(new Query(query));
    for (String role : ImmutableSet.copyOf(Iterables.transform(tasks, GET_ROLE))) {
      Pair<ResponseCode, String> rc = validateSessionKey(session, role);
      if (rc.getFirst() != OK) {
        response.setResponseCode(rc.getFirst())
            .setMessage(rc.getSecond());
        return response;
      }
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

    Pair<ResponseCode, String> rc = validateSessionKey(session, taskIds);
    if (rc.getFirst() != OK) {
      response.setResponseCode(rc.getFirst())
          .setMessage(rc.getSecond());
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
    Pair<ResponseCode, String> rc = validateSessionKey(session, job.getOwner().getRole());
    if (rc.getFirst() != OK) {
      return response.setResponseCode(rc.getFirst()).setMessage(rc.getSecond());
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
}
