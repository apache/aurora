package com.twitter.mesos.scheduler;

import com.google.common.base.Preconditions;
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
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.UpdateRequest;
import com.twitter.mesos.gen.UpdateResponse;
import com.twitter.mesos.scheduler.TaskStore.TaskState;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;

import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import static com.twitter.mesos.gen.ResponseCode.*;

/**
 * Mesos scheduler thrift server implementation.
 * Interfaces between mesos users and the scheduler core to perform cluster administration tasks.
 *
 * @author wfarner
 */
class SchedulerThriftInterface extends ThriftServer implements MesosSchedulerManager.Iface {
  private static Logger LOG = Logger.getLogger(SchedulerThriftInterface.class.getName());

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

  // TODO(wfarner): Provide status information about cron jobs here.
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
    Preconditions.checkNotNull(request, "Request may not be null.");
    Preconditions.checkNotNull(request.getUpdatedJob(), "Job update may not be null.");

    // TODO(wfarner): JobConfiguration needs to define the update routine for its tasks.
    // TODO(wfarner): This should spin off a new job that will communicate with the scheduler
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
  public RestartResponse restartTasks(Set<Integer> taskIds) {
    ResponseCode response = OK;
    String message = taskIds.size() + " tasks scheduled for restart.";

    Set<Integer> tasksRestarting = schedulerCore.restartTasks(Sets.newHashSet(taskIds));
    if (!taskIds.equals(tasksRestarting)) {
      response = WARNING;
      message = "Unable to restart tasks: " + Sets.difference(taskIds, tasksRestarting);
    }

    return new RestartResponse(response, message, tasksRestarting);
  }

  @Override
  protected void tryShutdown() throws Exception {
    // TODO(wfarner): Implement.
  }

  @Override
  public String getStatusDetails() {
    // TODO(wfarner): Return something useful here.
    return "Not implemented";
  }

  @Override
  public Map<String, Long> getCounters() {
    // TODO(wfarner): Return something useful here.
    return Maps.newHashMap();
  }

  @Override
  public long getCounter(String key) {
    // TODO(wfarner): Return something useful here.
    return 0;
  }

  @Override
  public void setOption(String key, String value) {
    // TODO(wfarner): Implement.
  }

  @Override
  public String getOption(String key) {
    // TODO(wfarner): Return something useful here.
    return "Not implemented";
  }

  @Override
  public Map<String, String> getOptions() {
    // TODO(wfarner): Return something useful here.
    return Maps.newHashMap();
  }
}
