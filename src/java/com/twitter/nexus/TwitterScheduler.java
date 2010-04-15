package com.twitter.nexus;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.twitter.common.thrift.ThriftServer;
import com.twitter.nexus.gen.*;
import nexus.ExecutorInfo;
import nexus.Scheduler;
import nexus.SchedulerDriver;
import nexus.SlaveOffer;
import nexus.SlaveOfferVector;
import nexus.StringMap;
import nexus.TaskDescriptionVector;
import nexus.TaskStatus;
import org.apache.thrift.TException;

public class TwitterScheduler extends Scheduler {
  private static Logger LOG = Logger.getLogger(TwitterScheduler.class.getName());

  static {
    System.loadLibrary("nexus");
  }

  private final Multimap<String, ConcreteTaskDescription> pendingTasks =
      HashMultimap.create();
  private final Map<String, Map<Integer, ConcreteTaskDescription>> scheduledTasks =
      Maps.newHashMap();

  private final String executorPath;

  // TODO(wfarner): Hopefully we can abolish (or at least mask) the concept of canonical task IDs
  // in favor of tasks being canonically named by job/taskIndex.
  private int nextTaskId = 0;

  public TwitterScheduler(String executorPath) {
    this.executorPath = Preconditions.checkNotNull(executorPath);
    Preconditions.checkArgument(new File(executorPath).canRead());

    LOG.info("Preloading with dummy configurations.");
    try {
      pendingTasks.put("talon", ConfigurationManager.makeConcrete(
          new TaskDescription().setConfiguration(ImmutableMap.of(
              "hdfs_path", "/dev/null",
              "cmd_line_args", "foo bar",
              "num_cpus", "0.5",
              "ram_bytes", "536870912"))));
      pendingTasks.put("talon", ConfigurationManager.makeConcrete(
          new TaskDescription().setConfiguration(ImmutableMap.of(
              "hdfs_path", "/dev/null",
              "cmd_line_args", "foo bar",
              "num_cpus", "0.5",
              "ram_bytes", "536870912"))));
      pendingTasks.put("puffin", ConfigurationManager.makeConcrete(
          new TaskDescription().setConfiguration(ImmutableMap.of(
              "hdfs_path", "/dev/null",
              "cmd_line_args", "foo bar",
              "num_cpus", "0.5",
              "ram_bytes", "536870912"))));
    } catch (ConfigurationManager.TaskDescriptionException e) {
      LOG.log(Level.WARNING, "Failed to preload config", e);
    }
  }

  public void startThriftServer(int port) {
    SchedulerManager thriftServer = new SchedulerManager();
    thriftServer.start(port, new NexusSchedulerManager.Processor(thriftServer));
  }

  public String getFrameworkName(SchedulerDriver driver) {
    return "TwitterScheduler";
  }

  public ExecutorInfo getExecutorInfo(SchedulerDriver driver) {
    return new ExecutorInfo(executorPath, new byte[0]);
  }

  public void registered(SchedulerDriver driver, int frameworkId) {
    LOG.info("Registered with ID " + frameworkId);
    // TODO(wfarner): Register with ZooKeeper
  }

  public void resourceOffer(SchedulerDriver driver, long offerId, SlaveOfferVector offers) {
    // Tasks to launch.
    TaskDescriptionVector newlyScheduledTasks = new TaskDescriptionVector();

    for (int i = 0; i < offers.size(); i++) {
      SlaveOffer slaveOffer = offers.get(i);
      LOG.info("Trying offer from slave " + slaveOffer.getSlaveId());
      ConcreteTaskDescription offer;
      try {
        offer = ConfigurationManager.makeConcrete(slaveOffer);
      } catch (ConfigurationManager.TaskDescriptionException e) {
        LOG.log(Level.SEVERE, "Invalid slave offer", e);
        continue;
      }

      LOG.info("Pending tasks: " + pendingTasks.size());

      synchronized (pendingTasks) {
        Set<Map.Entry<String, ConcreteTaskDescription>> satisfiedTasks = Sets.newHashSet();

        // Try to find a slave task description that is satisfied by the slave offer.
        for (Map.Entry<String, ConcreteTaskDescription> taskEntry : pendingTasks.entries()) {
          String jobName = taskEntry.getKey();
          LOG.info("Looking at tasks in job " + jobName);
          ConcreteTaskDescription task = taskEntry.getValue();
          if (ConfigurationManager.satisfied(task, offer)) {
            LOG.info("Offer is being assigned to a task within " + jobName);

            // Found an owner for the resource!
            int taskId = nextTaskId++;

            // TODO(wfarner): Remove this hack once nexus core does not read parameters.
            StringMap params = new StringMap();
            params.set("cpus", String.valueOf(task.getNumCpus()));
            params.set("mem", String.valueOf(task.getRamBytes()));

            newlyScheduledTasks.add(
                new nexus.TaskDescription(taskId, slaveOffer.getSlaveId(),
                    jobName + "-" + taskId, params, new byte[0]));

            // TODO(wfarner): Need to 'consume' the resouce from the slave offer, since the
            // task requirement might be a fraction of the offer.

            satisfiedTasks.add(taskEntry);

            synchronized (scheduledTasks) {
              Map<Integer, ConcreteTaskDescription> scheduledJobTasks = scheduledTasks.get(jobName);
              if (scheduledJobTasks == null) {
                scheduledJobTasks = Maps.newHashMap();
                scheduledTasks.put(jobName, scheduledJobTasks);
              }
              scheduledJobTasks.put(taskId, task);
            }

            break;
          }
        }

        pendingTasks.entries().removeAll(satisfiedTasks);
      }
    }
    driver.replyToOffer(offerId, newlyScheduledTasks, new StringMap());
  }

  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    LOG.info("Received status update for task " + status.getTaskId()
             + " in state " + status.getState());
    String jobName = null;

    synchronized (scheduledTasks) {
      // Find the job associated with the task ID.
      for (Map.Entry<String, Map<Integer, ConcreteTaskDescription>> entry
          : scheduledTasks.entrySet()) {
        if (entry.getValue().containsKey(status.getTaskId())) {
          LOG.info("Task is owned by job " + entry.getKey());
          jobName = entry.getKey();
          break;
        }
      }

      if (jobName == null) {
        LOG.severe("Failed to find task id " + status.getTaskId());
      } else {
        Map<Integer, ConcreteTaskDescription> jobTasks = scheduledTasks.get(jobName);

        boolean removeTask = false;

        switch (status.getState()) {
          case TASK_STARTING:
            break;
          case TASK_RUNNING:
            break;
          case TASK_FINISHED:
            // TODO(wfarner): Some of these states will require the task to be rescheduled, depending
            // on the config (daemon, number of allowed failures, etc).
          case TASK_FAILED:
          case TASK_KILLED:
          case TASK_LOST:
            removeTask = true;
            break;
          default:
            LOG.severe("Unrecognized task state " + status.getState());
            return;
        }

        if (removeTask) {
          jobTasks.remove(status.getTaskId());
          if (jobTasks.isEmpty()) {
            scheduledTasks.remove(jobName);
            LOG.info("Last task for job " + jobName + " completed, removing job.");
          }
        }
      }
    }
  }

  public void error(SchedulerDriver driver, int code, String message) {
    LOG.severe("Received error message: " + message + " with code " + code);
  }

  private class SchedulerManager extends ThriftServer implements NexusSchedulerManager.Iface {
    public SchedulerManager() {
      super("TwitterNexusScheduler", "1");
    }

    @Override
    public CreateJobResponse createJob(JobConfiguration jobDesc) throws TException {
      String jobName = jobDesc.getName();
      if (pendingTasks.containsKey(jobName) || scheduledTasks.containsKey(jobName)) {
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

      synchronized (pendingTasks) {
        pendingTasks.putAll(jobName, concreteTasks);
      }

      return new CreateJobResponse().setResponseCode(ResponseCode.OK)
          .setMessage(concreteTasks.size() + " tasks pending for job " + jobName);
    }

    @Override
    public ScheduleStatusResponse getJobStatus(String jobName) throws TException {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
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
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public KillResponse killTasks(String jobName, Set<Integer> taskIds) throws TException {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public RestartResponse restartTasks(String jobName, Set<Integer> taskIds) throws TException {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
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
