package com.twitter.nexus.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.twitter.common.thrift.ThriftServer;
import com.twitter.nexus.gen.*;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Nexus scheduler thrift server implementation.
 * Interfaces between nexus users and the scheduler core to perform cluster administration tasks.
 *
 * @author wfarner
 */
class SchedulerManager extends ThriftServer implements NexusSchedulerManager.Iface {
  private static Logger LOG = Logger.getLogger(SchedulerManager.class.getName());

  @Inject
  private SchedulerCore schedulerCore;

  public SchedulerManager() {
    super("TwitterNexusScheduler", "1");
  }

  @Override
  public CreateJobResponse createJob(JobConfiguration job) throws TException {
    LOG.info("Received createJob request: " + job);
    String jobId = job.getOwner() + "/" + job.getName();

    for (TwitterTaskInfo config : job.getTaskConfigs()) {
      try {
        ConfigurationManager.populateFields(config);
      } catch (ConfigurationManager.TaskDescriptionException e) {
        return new CreateJobResponse()
            .setResponseCode(ResponseCode.INVALID_REQUEST)
            .setMessage("Invalid configuration, error: " + e.getMessage());
      }
    }

    try {
      schedulerCore.createJob(job);
    } catch (ScheduleException e) {
      return new CreateJobResponse()
          .setResponseCode(ResponseCode.INVALID_REQUEST)
          .setMessage("Failed to schedule job - " + e.getMessage());
    }

    return new CreateJobResponse().setResponseCode(ResponseCode.OK)
        .setMessage(job.getTaskConfigs().size() + " new tasks pending for job " + jobId);
  }

  @Override
  public ScheduleStatusResponse getTasksStatus(TaskQuery query) throws TException {
    List<TrackedTask> tasks = Lists.newArrayList(schedulerCore.getTasks(query));

    if (tasks.isEmpty()) {
      return new ScheduleStatusResponse()
          .setResponseCode(ResponseCode.INVALID_REQUEST)
          .setMessage("No tasks found for query: " + query);
    }

    return new ScheduleStatusResponse()
        .setResponseCode(ResponseCode.OK)
        .setTaskStatuses(tasks);
  }

  @Override
  public UpdateResponse updateTasks(TaskQuery query, UpdateRequest request)
      throws TException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public KillResponse killTasks(TaskQuery query) throws TException {
    LOG.info("Received kill request for tasks: " + query);
    schedulerCore.killTasks(query);
    return new KillResponse().setResponseCode(ResponseCode.OK);
  }

  @Override
  public RestartResponse restartTasks(TaskQuery query) throws TException {
    schedulerCore.restartTasks(query);
    return new RestartResponse().setResponseCode(ResponseCode.OK);
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
