package com.twitter.mesos.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.twitter.common.thrift.ThriftServer;
import com.twitter.mesos.gen.CreateJobResponse;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.KillResponse;
import com.twitter.mesos.gen.LiveTask;
import com.twitter.mesos.gen.MesosSchedulerManager;
import com.twitter.mesos.gen.ResponseCode;
import com.twitter.mesos.gen.RestartResponse;
import com.twitter.mesos.gen.ScheduleStatusResponse;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.UpdateRequest;
import com.twitter.mesos.gen.UpdateResponse;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Mesos scheduler thrift server implementation.
 * Interfaces between mesos users and the scheduler core to perform cluster administration tasks.
 *
 * @author wfarner
 */
class SchedulerThriftInterface extends ThriftServer implements MesosSchedulerManager.Iface {
  private static Logger LOG = Logger.getLogger(SchedulerThriftInterface.class.getName());

  @Inject private SchedulerCore schedulerCore;

  public SchedulerThriftInterface() {
    super("TwitterMesosScheduler", "1");
  }

  @Override
  public CreateJobResponse createJob(JobConfiguration job) throws TException {
    LOG.info("Received createJob request: " + job);
    CreateJobResponse response = new CreateJobResponse();

    try {
      schedulerCore.createJob(job);
      response.setResponseCode(ResponseCode.OK)
          .setMessage(String.format("%d new tasks pending for job %s/%s",
              job.getTaskConfigs().size(), job.getOwner(), job.getName()));
    } catch (ConfigurationManager.TaskDescriptionException e) {
      response.setResponseCode(ResponseCode.INVALID_REQUEST)
          .setMessage("Invalid task description: " + e.getMessage());
    } catch (ScheduleException e) {
      response.setResponseCode(ResponseCode.INVALID_REQUEST)
          .setMessage("Failed to schedule job - " + e.getMessage());
    }

    return response;
  }

  // TODO(wfarner): Provide status information about cron jobs here.
  @Override
  public ScheduleStatusResponse getTasksStatus(TaskQuery query) throws TException {
    List<LiveTask> tasks = Lists.newArrayList(schedulerCore.getLiveTasks(query));

    ScheduleStatusResponse response = new ScheduleStatusResponse();
    if (tasks.isEmpty()) {
      response.setResponseCode(ResponseCode.INVALID_REQUEST)
          .setMessage("No tasks found for query: " + query);
    } else {
      response.setResponseCode(ResponseCode.OK).setTasks(tasks);
    }

    return response;
  }

  @Override
  public UpdateResponse updateTasks(TaskQuery query, UpdateRequest request)
      throws TException {
    // TODO(wfarner): Need a method to diff stored vs updated TwitterTaskInfo objects.
    // TODO(wfarner): Need a method to determine whether diff requires task restarts.
    // TODO(wfarner): JobConfiguration needs to define the update routine for its tasks.
    // TODO(wfarner): This should spin off a new job that will communicate with the scheduler
    //    to fetch the status of tasks, and restart them per the configured update routine.

    return null;
  }

  @Override
  public KillResponse killTasks(TaskQuery query) throws TException {
    LOG.info("Received kill request for tasks: " + query);
    KillResponse response = new KillResponse();

    try {
      schedulerCore.killTasks(query);
      response.setResponseCode(ResponseCode.OK)
          .setMessage("Tasks will be killed.");
    } catch (ScheduleException e) {
      response.setResponseCode(ResponseCode.INVALID_REQUEST)
          .setMessage(e.getMessage());
    }

    return response;
  }

  @Override
  public RestartResponse restartTasks(Set<Integer> taskIds) throws TException {
    ResponseCode response = ResponseCode.OK;
    String message = taskIds.size() + " tasks scheduled for restart.";

    Set<Integer> tasksRestarting = schedulerCore.restartTasks(Sets.newHashSet(taskIds));
    if (!taskIds.equals(tasksRestarting)) {
      response = ResponseCode.WARNING;
      message = "Unable to restart tasks: " + Sets.difference(taskIds, tasksRestarting);
    }

    return new RestartResponse(response, message, tasksRestarting);
  }

  @Override
  protected void tryShutdown() throws Exception {
    // TODO(wfarner): Implement.
  }

  @Override
  public String getStatusDetails() throws TException {
    // TODO(wfarner): Return something useful here.
    return "Not implemented";
  }

  @Override
  public Map<String, Long> getCounters() throws TException {
    // TODO(wfarner): Return something useful here.
    return Maps.newHashMap();
  }

  @Override
  public long getCounter(String key) throws TException {
    // TODO(wfarner): Return something useful here.
    return 0;
  }

  @Override
  public void setOption(String key, String value) throws TException {
    // TODO(wfarner): Implement.
  }

  @Override
  public String getOption(String key) throws TException {
    // TODO(wfarner): Return something useful here.
    return "Not implemented";
  }

  @Override
  public Map<String, String> getOptions() throws TException {
    // TODO(wfarner): Return something useful here.
    return Maps.newHashMap();
  }
}
