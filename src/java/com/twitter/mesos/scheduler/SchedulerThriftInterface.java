package com.twitter.mesos.scheduler;

import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.twitter.common.thrift.ThriftServer;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.CreateJobResponse;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.KillResponse;
import com.twitter.mesos.gen.MesosSchedulerManager;
import com.twitter.mesos.gen.ResponseCode;
import com.twitter.mesos.gen.RestartResponse;
import com.twitter.mesos.gen.ScheduleStatusResponse;
import com.twitter.mesos.gen.ShardUpdateRequest;
import com.twitter.mesos.gen.ShardUpdateResponse;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.UpdateCompleteResponse;
import com.twitter.mesos.gen.UpdateConfigResponse;
import com.twitter.mesos.gen.UpdateRequest;
import com.twitter.mesos.gen.UpdateResponse;
import com.twitter.mesos.scheduler.SchedulerCore.RestartException;
import com.twitter.mesos.scheduler.SchedulerCore.TaskState;
import com.twitter.mesos.scheduler.UpdateScheduler.UpdateException;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;

import org.apache.commons.lang.StringUtils;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.gen.ResponseCode.*;

/**
 * Mesos scheduler thrift server implementation.
 * Interfaces between mesos users and the scheduler core to perform cluster administration tasks.
 *
 * @author William Farner
 */
public class SchedulerThriftInterface extends ThriftServer implements MesosSchedulerManager.Iface {
  private static final Logger LOG = Logger.getLogger(SchedulerThriftInterface.class.getName());

  private final SchedulerCore schedulerCore;

  @Inject
  public SchedulerThriftInterface(SchedulerCore schedulerCore) {
    super("TwitterMesosScheduler", "1");
    this.schedulerCore = schedulerCore;
  }

  @Override
  public CreateJobResponse createJob(JobConfiguration job) {
    LOG.info("Received createJob request: " + job);
    CreateJobResponse response = new CreateJobResponse();

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
          .setTasks(Lists.newArrayList(Iterables.transform(tasks, Tasks.STATE_TO_LIVE)));
    }

    return response;
  }

  @Override
  public UpdateResponse updateTasks(UpdateRequest request) {
    checkNotNull(request, "Request may not be null.");
    checkNotNull(request.getUpdatedJob(), "Job update may not be null.");

    // TODO(William Farner): JobConfiguration needs to define the update routine for its tasks.
    // TODO(William Farner): This should spin off a new job that will communicate with the scheduler
    //    to fetch the status of tasks, and restart them per the configured update routine.

    JobConfiguration updatedJob = request.getUpdatedJob();
    UpdateResponse response = new UpdateResponse();

    try {
      switch (schedulerCore.updateJob(updatedJob)) {
        case JOB_UNCHANGED:
          response.setMessage("Job was unchanged, no update necessary.");
          break;
        case COMPLETED:
          response.setMessage("Update did not require restert, update complete.");
          break;
        case UPDATER_LAUNCHED:
          response.setMessage("Job updater triggered.");
          break;
        default:
          throw new RuntimeException("Unhandled update result.");
      }
      response.setResponseCode(OK);
    } catch (ScheduleException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage("Job update failed: " + e.getMessage());
    } catch (IllegalArgumentException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    } catch (NullPointerException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    } catch (TaskDescriptionException e) {
      response.setResponseCode(INVALID_REQUEST)
          .setMessage("Invalid job configuration: " + e.getMessage());
    }

    return response;
  }

  @Override
  public KillResponse killTasks(TaskQuery query) {
    LOG.info("Received kill request for tasks: " + query);
    KillResponse response = new KillResponse();

    try {
      schedulerCore.killTasks(new Query(query));
      response.setResponseCode(OK).setMessage("Tasks will be killed.");
    } catch (ScheduleException e) {
      response.setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public RestartResponse restartTasks(Set<String> taskIds) {
    ResponseCode response = OK;
    String message = taskIds.size() + " tasks scheduled for restart.";

    Set<String> tasksRestarting = null;
    try {
      tasksRestarting = schedulerCore.restartTasks(Sets.newHashSet(taskIds));
    } catch (RestartException e) {
      response = INVALID_REQUEST;
      message = e.getMessage();
    }
    if (!taskIds.equals(tasksRestarting)) {
      response = WARNING;
      message = "Unable to restart tasks: " + Sets.difference(taskIds, tasksRestarting);
    }

    return new RestartResponse(response, message, tasksRestarting);
  }

  @Override public UpdateConfigResponse getUpdateConfig(String updateToken) {
    checkNotBlank(updateToken);

    try {
      return schedulerCore.getUpdateConfig(updateToken)
          .setMessage("Configuration found.").setResponseCode(OK);
    } catch (UpdateException e) {
      return new UpdateConfigResponse().setResponseCode(INVALID_REQUEST).setMessage(e.getMessage());
    }
  }

  @Override
  public UpdateCompleteResponse finishUpdate(String updateToken) {
    checkNotBlank(updateToken);

    ResponseCode response = OK;
    String message = "Update finished.";

    try {
      schedulerCore.updateFinished(updateToken);
    } catch (UpdateException e) {
      response = INVALID_REQUEST;
      message = e.getMessage();
    }

    return new UpdateCompleteResponse(response, message);
  }

  @Override
  public UpdateCompleteResponse cancelUpdate(String owner, String jobName) {
    checkNotBlank(owner, "Owner cannot be blank.");
    checkNotBlank(jobName, "Job name cannot be blank.");

    ResponseCode response = OK;
    String message = "Update canceled.";

    try {
      schedulerCore.updateFinished(owner, jobName);
    } catch (UpdateException e) {
      response = INVALID_REQUEST;
      message = e.getMessage();
    }

    return new UpdateCompleteResponse(response, message);
  }

  @Override
  public ShardUpdateResponse updateShards(ShardUpdateRequest request) {
    checkNotNull(request);

    ResponseCode response = OK;
    String message = "Shards updated.";
    Set<String> restartedTaskIds = null;

    if (StringUtils.isBlank(request.getUpdateToken())) {
      response = INVALID_REQUEST;
      message = "Update token must be specified.";
    } else if (request.getRestartShards() == null) {
      response = INVALID_REQUEST;
      message = "Updates cannot be null.";
    } else if (request.getRestartShards().isEmpty()) {
      response = INVALID_REQUEST;
      message = "No updates provided.";
    } else {
      try {
        restartedTaskIds = schedulerCore.updateShards(request.getUpdateToken(),
            request.getRestartShards(), request.isRollback());
      } catch (UpdateException e) {
        response = INVALID_REQUEST;
        message = e.getMessage();
      }
    }

    return new ShardUpdateResponse(response, message, restartedTaskIds);
  }

  @Override
  protected void tryShutdown() throws Exception {
    // TODO(William Farner): Implement.
  }

  @Override
  public String getStatusDetails() {
    // TODO(William Farner): Return something useful here.
    return "Not implemented";
  }

  @Override
  public Map<String, Long> getCounters() {
    // TODO(William Farner): Return something useful here.
    return Maps.newHashMap();
  }

  @Override
  public long getCounter(String key) {
    // TODO(William Farner): Return something useful here.
    return 0;
  }

  @Override
  public void setOption(String key, String value) {
    // TODO(William Farner): Implement.
  }

  @Override
  public String getOption(String key) {
    // TODO(William Farner): Return something useful here.
    return "Not implemented";
  }

  @Override
  public Map<String, String> getOptions() {
    // TODO(William Farner): Return something useful here.
    return Maps.newHashMap();
  }
}
