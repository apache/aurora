package com.twitter.nexus;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.twitter.common.thrift.ThriftServer;
import com.twitter.nexus.gen.TwitterTaskConfig;
import com.twitter.nexus.gen.TwitterTaskInfo;
import com.twitter.nexus.gen.CreateJobResponse;
import com.twitter.nexus.gen.JobConfiguration;
import com.twitter.nexus.gen.KillResponse;
import com.twitter.nexus.gen.NexusSchedulerManager;
import com.twitter.nexus.gen.ResponseCode;
import com.twitter.nexus.gen.RestartResponse;
import com.twitter.nexus.gen.ScheduleStatus;
import com.twitter.nexus.gen.ScheduleStatusResponse;
import com.twitter.nexus.gen.TrackedTask;
import com.twitter.nexus.gen.UpdateRequest;
import com.twitter.nexus.gen.UpdateResponse;
import nexus.ExecutorInfo;
import nexus.Scheduler;
import nexus.SchedulerDriver;
import nexus.SlaveOfferVector;
import nexus.StringMap;
import nexus.TaskDescriptionVector;
import nexus.TaskStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Central location for communication with the scheduler core.  Handles interfacing with the nexus core and scheduling
 * clients via thrift.
 *
 * @author wfarner
 */
public class SchedulerHub extends Scheduler {
  private static Logger LOG = Logger.getLogger(SchedulerHub.class.getName());

  static {
    System.loadLibrary("nexus");
  }

  private final SchedulerCore schedulerCore;
  private final SchedulerMain.TwitterSchedulerOptions options;

  @Inject
  public SchedulerHub(SchedulerMain.TwitterSchedulerOptions options, SchedulerCore schedulerCore) {
    this.options = Preconditions.checkNotNull(options);
    this.schedulerCore = Preconditions.checkNotNull(schedulerCore);
    isExecutorBinaryValid();
  }

  private void isExecutorBinaryValid() {
    if (options.executorPath.startsWith("hdfs")) {
      Preconditions.checkArgument(options.hdfsConfig != null);
      checkHdfsExecutorBinary(options.executorPath);
    } else if (options.executorPath.startsWith("http")||options.executorPath.startsWith("ftp")) {
      throw new UnsupportedOperationException("Error, currently http/ftp handling isn't supported for Executor binaries.");
    } else {
      Preconditions.checkArgument(new File(options.executorPath).canRead());
    }
  }

  private void checkHdfsExecutorBinary(final String hdfsFileName) {
    try {
    Configuration conf = new Configuration(true);
    conf.addResource(new Path(options.hdfsConfig));
    conf.reloadConfiguration();
    FileSystem hdfs = FileSystem.get(conf);
    FileStatus[] statuses = hdfs.listStatus(new Path(hdfsFileName));
    Preconditions.checkArgument(statuses.length == 1);
    Preconditions.checkArgument(!statuses[0].isDir());
    Preconditions.checkArgument(statuses[0].getBlockSize() > 0L);
    } catch (IOException e) {
      LOG.severe("Error trying to verify Executor binary in HDFS:" + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  public void startThriftServer(int port) {
    SchedulerManager thriftServer = new SchedulerManager();
    thriftServer.start(port, new NexusSchedulerManager.Processor(thriftServer));
  }

  @Override
  public String getFrameworkName(SchedulerDriver driver) {
    return "TwitterScheduler";
  }

  @Override
  public ExecutorInfo getExecutorInfo(SchedulerDriver driver) {
    return new ExecutorInfo(options.executorPath, new byte[0]);
  }

  @Override
  public void registered(SchedulerDriver driver, int frameworkId) {
    LOG.info("Registered with ID " + frameworkId);
    // TODO(wfarner): Register with ZooKeeper
    schedulerCore.setFrameworkId(frameworkId);
  }

  @Override
  public void resourceOffer(SchedulerDriver driver, long offerId, SlaveOfferVector offers) {
    schedulerCore.clearWorkQueue(driver);

    TaskDescriptionVector newlyScheduledTasks = new TaskDescriptionVector();

    for (int i = 0; i < offers.size(); i++) {
      nexus.TaskDescription taskToSchedule = schedulerCore.schedulePendingTask(offers.get(i));
      if (taskToSchedule != null) {
        newlyScheduledTasks.add(taskToSchedule);
      }

      LOG.info("Accepting tasks: " + newlyScheduledTasks.size());
    }
    driver.replyToOffer(offerId, newlyScheduledTasks, new StringMap());
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    LOG.info("Received status update for task " + status.getTaskId()
        + " in state " + status.getState());

    if (schedulerCore.getTask(status.getTaskId()) == null) {
      LOG.severe("Failed to find task id " + status.getTaskId());
    } else {
      boolean removeTask = false;

      switch (status.getState()) {
        case TASK_STARTING:
          schedulerCore.setTaskStatus(status.getTaskId(), ScheduleStatus.STARTING);
          break;
        case TASK_RUNNING:
          schedulerCore.setTaskStatus(status.getTaskId(), ScheduleStatus.RUNNING);
          break;
        case TASK_FINISHED:
          // TODO(wfarner): Some of these states will require the task to be rescheduled,
          // depending on the config (daemon, number of allowed failures, etc).
          schedulerCore.setTaskStatus(status.getTaskId(), ScheduleStatus.FINISHED);
        case TASK_FAILED:
          schedulerCore.setTaskStatus(status.getTaskId(), ScheduleStatus.FAILED);
        case TASK_KILLED:
          schedulerCore.setTaskStatus(status.getTaskId(), ScheduleStatus.KILLED);
        case TASK_LOST:
          schedulerCore.setTaskStatus(status.getTaskId(), ScheduleStatus.LOST);
          removeTask = true;
          break;
        default:
          LOG.severe("Unrecognized task state " + status.getState());
          return;
      }

      if (removeTask) {
        schedulerCore.removeTask(status.getTaskId());
      }
    }
  }

  @Override
  public void error(SchedulerDriver driver, int code, String message) {
    LOG.severe("Received error message: " + message + " with code " + code);
  }

  /**
   * Thrift server.
   */
  private class SchedulerManager extends ThriftServer implements NexusSchedulerManager.Iface {
    public SchedulerManager() {
      super("TwitterNexusScheduler", "1");
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
      for (TwitterTaskConfig config : jobDesc.getTaskConfigs()) {
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
}
