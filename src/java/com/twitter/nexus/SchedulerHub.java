package com.twitter.nexus;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.twitter.common.thrift.ThriftServer;
import com.twitter.nexus.gen.*;
import nexus.ExecutorInfo;
import nexus.Scheduler;
import nexus.SchedulerDriver;
import nexus.SlaveOfferVector;
import nexus.StringMap;
import nexus.TaskDescriptionVector;
import nexus.TaskStatus;
import org.apache.thrift.TException;

/**
 * Central location for communication with the scheduler core.  Handles interfacing with the nexus
 * core and scheduling clients via thrift.
 *
 * @author wfarner
 */
public class SchedulerHub extends Scheduler {
  private static Logger LOG = Logger.getLogger(SchedulerHub.class.getName());

  static {
    System.loadLibrary("nexus");
  }

  // Stores scheduler state and handles actual scheduling decisions.
  private final SchedulerCore schedulerCore = new SchedulerCore();

  private final String executorPath;

  /**
   * Creates a new scheduler hub, which will use the executor at {@code executorPath}.
   *
   * @param executorPath Path to the executor script.
   */
  public SchedulerHub(String executorPath) {
    this.executorPath = Preconditions.checkNotNull(executorPath);
    Preconditions.checkArgument(new File(executorPath).canRead());
  }

  /**
   * Instructs the hub to start the thrift server for management of scheduled jobs.
   *
   * TODO(wfarner): The thrift server should automatically be taken down if/when the scheduler
   * driver dies.
   * TODO(wfarner): Launch on an ephemeral port and register with zookeeper.
   *
   * @param port Port that the thrift server should listen on.
   */
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
    return new ExecutorInfo(executorPath, new byte[0]);
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

      // TODO(wfarner): May want to add a fixed time delay on removing the task from tracking, to
      // allow it to remain in a failed/killed/lost state long enough for someone to debug.
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
   * Thrift server implementation.
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

      List<ConcreteTaskDescription> concreteTasks = Lists.newArrayList();
      for (TaskDescription task : jobDesc.getTaskDescriptions()) {
        try {
          concreteTasks.add(ConfigurationManager.makeConcrete(task));
        } catch (ConfigurationManager.TaskDescriptionException e) {
          return new CreateJobResponse()
              .setResponseCode(ResponseCode.INVALID_REQUEST)
              .setMessage("Invalid configuration, error: " + e.getMessage());
        }
      }

      schedulerCore.addTasks(jobName, concreteTasks);

      return new CreateJobResponse().setResponseCode(ResponseCode.OK)
          .setMessage(concreteTasks.size() + " tasks pending for job " + jobName);
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
