package com.twitter.mesos.scheduler;

import java.util.Set;
import java.util.logging.Logger;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import com.twitter.common.collections.Pair;
import com.twitter.common_internal.elfowl.Cookie;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.CreateJobResponse;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.KillResponse;
import com.twitter.mesos.gen.MesosSchedulerManager;
import com.twitter.mesos.gen.ResponseCode;
import com.twitter.mesos.gen.RestartResponse;
import com.twitter.mesos.gen.ScheduleStatusResponse;
import com.twitter.mesos.gen.SessionKey;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.SchedulerCore.RestartException;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;

import static com.google.common.base.Preconditions.checkNotNull;
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

  private final SchedulerCore schedulerCore;

  @Inject
  public SchedulerThriftInterface(SchedulerCore schedulerCore) {
    this.schedulerCore = checkNotNull(schedulerCore);
  }

  /**
   * Given a sessionKey, determine the response type and provide human-readable error message.
   */
  private Pair<ResponseCode, String> validateSessionKey(SessionKey sessionKey,
      String targetRole) {
    if (!sessionKey.isSetOwner()
        || !sessionKey.getOwner().isSetRole()
        || !sessionKey.getOwner().isSetUser()
        || !sessionKey.isSetCookie()) {
      return Pair.of(AUTH_FAILED, "Incorrectly specified session key.");
    }

    Cookie cookie = Cookie.fromBase64(sessionKey.getCookie().toString());

    if (cookie == null) {
      return Pair.of(AUTH_FAILED, "Unable to parse supplied cookie.");
    }

    // Make sure the cookie is properly cryptographically signed by the correct user.
    if (!cookie.isVerified()) {
      return Pair.of(AUTH_FAILED, "Cookie appears to be forged.");
    }

    // The cookie identity and the session key identity must match for the session to
    // be valid.
    if (!cookie.getUser().equals(sessionKey.getOwner().getUser())) {
      return Pair.of(AUTH_FAILED,
          String.format("Supplied cookie and session identity are for different users (%s vs %s)",
              cookie.getUser(),
              sessionKey.getOwner().getUser()));
    }

    // We need to accept this session based upon a targetRole.  The targetRole is going to be
    // one of two cases:
    //   - the username -- not an explicit ODS group but an accepted role
    //       (this will be made explicit when we have the usermap)
    //   - the ODS group that the user has choosed as the role
    if (!cookie.getUser().equals(targetRole) && !cookie.hasGroup(targetRole)) {
      return Pair.of(AUTH_FAILED,
          String.format("User %s does not have permission for role %s",
              cookie.getUser(), targetRole));
    }

    return Pair.of(OK, "");
  }

  @Override
  public CreateJobResponse createJob(JobConfiguration job, SessionKey session) {
    LOG.info("Received createJob request: " + job);

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

  // TODO(William Farner): Provide status information about cron jobs here.
  @Override
  public ScheduleStatusResponse getTasksStatus(TaskQuery query) {
    Set<TaskState> tasks = schedulerCore.getTasks(new Query(query));

    ScheduleStatusResponse response = new ScheduleStatusResponse();
    if (tasks.isEmpty()) {
      response.setResponseCode(INVALID_REQUEST).setMessage("No tasks found for query: " + query);
    } else {
      response.setResponseCode(OK)
          .setTasks(Lists.newArrayList(Iterables.transform(tasks, TaskState.STATE_TO_LIVE)));
    }

    return response;
  }

  @Override
  public KillResponse killTasks(TaskQuery query, SessionKey session) {
    checkNotNull(session, "Session must be set.");

    LOG.info("Received kill request for tasks: " + query);
    KillResponse response = new KillResponse();

    Set<TaskState> tasks = schedulerCore.getTasks(new Query(query));
    String sessionRole = session.getOwner().getRole();
    for (TaskState task : tasks) {
      Identity taskId = task.task.getAssignedTask().getTask().getOwner();
      if (!sessionRole.equals(taskId.getRole()) && !sessionRole.equals(taskId.getUser())) {
        response.setResponseCode(AUTH_FAILED).setMessage(
            "You do not have permission to kill all tasks in this query.");
        return response;
      }
    }

    Pair<ResponseCode, String> rc = validateSessionKey(session, sessionRole);
    if (rc.getFirst() != OK) {
      response.setResponseCode(rc.getFirst()).setMessage(rc.getSecond());
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

  @Override
  public RestartResponse restartTasks(Set<String> taskIds, SessionKey session) {
    checkNotNull(session, "Session must be set.");

    ResponseCode response = OK;
    String message = taskIds.size() + " tasks scheduled for restart.";

    // TODO(wickman): Enforce that taskIds are all a compatible role with the
    // session.
    Pair<ResponseCode, String> rc = validateSessionKey(session, session.getOwner().getRole());
    if (rc.getFirst() != OK) {
      return new RestartResponse(rc.getFirst(), rc.getSecond());
    }

    Set<String> tasksRestarting = null;
    try {
      schedulerCore.restartTasks(Sets.newHashSet(taskIds));
    } catch (RestartException e) {
      response = INVALID_REQUEST;
      message = e.getMessage();
    }
    if (!taskIds.equals(tasksRestarting)) {
      response = WARNING;
      message = "Unable to restart tasks: " + Sets.difference(taskIds, tasksRestarting);
    }

    return new RestartResponse(response, message);
  }
}
