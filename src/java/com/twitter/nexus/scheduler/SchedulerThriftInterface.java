package com.twitter.nexus.scheduler;

import com.google.common.collect.Iterables;
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
class SchedulerThriftInterface extends ThriftServer implements NexusSchedulerManager.Iface {
  private static Logger LOG = Logger.getLogger(SchedulerThriftInterface.class.getName());

  @Inject
  private SchedulerCore schedulerCore;

  public SchedulerThriftInterface() {
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

    CreateJobResponse response = new CreateJobResponse();

    try {
      schedulerCore.createJob(job);
      response.setResponseCode(ResponseCode.OK)
          .setMessage(job.getTaskConfigs().size() + " new tasks pending for job " + jobId);
    } catch (ScheduleException e) {
      response.setResponseCode(ResponseCode.INVALID_REQUEST)
          .setMessage("Failed to schedule job - " + e.getMessage());
    }

    return response;
  }

  @Override
  public ScheduleStatusResponse getTasksStatus(TaskQuery query) throws TException {
    List<TrackedTask> tasks = Lists.newArrayList(schedulerCore.getTasks(query));

    ScheduleStatusResponse response = new ScheduleStatusResponse();
    if (tasks.isEmpty()) {
      response.setResponseCode(ResponseCode.INVALID_REQUEST)
          .setMessage("No tasks found for query: " + query);
    } else {
      response.setResponseCode(ResponseCode.OK)
        .setTaskStatuses(tasks);
    }

    return response;
  }

  @Override
  public UpdateResponse updateTasks(TaskQuery query, UpdateRequest request)
      throws TException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public KillResponse killTasks(TaskQuery query) throws TException {
    LOG.info("Received kill request for tasks: " + query);
    KillResponse response = new KillResponse();

    Iterable<TrackedTask> tasks = schedulerCore.getTasks(query);

    if (Iterables.isEmpty(tasks)) {
      response.setResponseCode(ResponseCode.INVALID_REQUEST)
          .setMessage("No matching tasks found.");
    } else {
      schedulerCore.killTasks(query);
      response.setResponseCode(ResponseCode.OK)
          .setMessage(String.format("%d tasks will be killed.", Iterables.size(tasks)));

    }

    return response;
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
