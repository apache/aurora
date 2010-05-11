package com.twitter.nexus.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.twitter.common.thrift.ThriftServer;
import com.twitter.nexus.gen.*;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Nexus scheduler thrift server implementation.
 * Interfaces between nexus users and the scheduler core to perform cluster administration tasks.
 *
 * @author wfarner
 */
class SchedulerManager extends ThriftServer implements NexusSchedulerManager.Iface {
  private static Logger LOG = Logger.getLogger(SchedulerManager.class.getName());
  private final SchedulerCore schedulerCore;

  @Inject
  public SchedulerManager(SchedulerCore schedulerCore) {
    super("TwitterNexusScheduler", "1");
    this.schedulerCore = schedulerCore;
  }

  @Override
  public CreateJobResponse createJob(JobConfiguration jobDesc) throws TException {
    LOG.info("Received createJob request: " + jobDesc);
    String jobName = jobDesc.getName();
    if (schedulerCore.hasJob(jobName)) {
      return new CreateJobResponse()
          .setResponseCode(ResponseCode.INVALID_REQUEST)
          .setMessage("A job with the name " + jobName + " already exists.");
    }

    List<TwitterTaskInfo> taskInfos = Lists.newArrayList();
    for (TwitterTaskInfo config : jobDesc.getTaskConfigs()) {
      try {
        taskInfos.add(ConfigurationManager.parse(config));
      } catch (ConfigurationManager.TaskDescriptionException e) {
        return new CreateJobResponse()
            .setResponseCode(ResponseCode.INVALID_REQUEST)
            .setMessage("Invalid configuration, error: " + e.getMessage());
      }
    }

    schedulerCore.addTasks(jobName, taskInfos);

    return new CreateJobResponse().setResponseCode(ResponseCode.OK)
        .setMessage(taskInfos.size() + " tasks pending for job " + jobName);
  }

  @Override
  public ScheduleStatusResponse getJobStatus(String jobName) throws TException {
    List<TrackedTask> tasks = Lists.newArrayList(schedulerCore.getTasks(jobName));

    if (tasks.isEmpty()) {
      return new ScheduleStatusResponse()
          .setResponseCode(ResponseCode.INVALID_REQUEST)
          .setMessage("Job not found: " + jobName);
    }

    return new ScheduleStatusResponse()
        .setResponseCode(ResponseCode.OK)
        .setTaskStatuses(tasks);
  }

  @Override
  public ScheduleStatusResponse getTasksStatus(String jobName, Set<Integer> taskIds)
      throws TException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public UpdateResponse updateJob(JobConfiguration description) throws TException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public UpdateResponse updateTasks(String jobName, UpdateRequest request) throws TException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public KillResponse killJob(String jobName) throws TException {
    if (!schedulerCore.hasJob(jobName)) {
      return new KillResponse()
          .setResponseCode(ResponseCode.INVALID_REQUEST)
          .setMessage("Job not found: " + jobName);
    }

    schedulerCore.killJob(jobName);
    return new KillResponse().setResponseCode(ResponseCode.OK);
  }

  @Override
  public KillResponse killTasks(String jobName, Set<Integer> taskIds) throws TException {
    schedulerCore.killTasks(taskIds);
    return new KillResponse().setResponseCode(ResponseCode.OK);
  }

  @Override
  public RestartResponse restartTasks(String jobName, Set<Integer> taskIds) throws TException {
    schedulerCore.restartTasks(taskIds);
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
