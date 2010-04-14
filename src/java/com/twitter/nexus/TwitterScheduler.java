package com.twitter.nexus;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
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

  public TwitterScheduler(String executorPath, int thriftPort) {
    this.executorPath = Preconditions.checkNotNull(executorPath);
    Preconditions.checkArgument(new File(executorPath).canRead());

    SchedulerManager thriftServer = new SchedulerManager();
    thriftServer.start(thriftPort, new NexusSchedulerManager.Processor(thriftServer));
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
      LOG.info("Trying offer from " + slaveOffer.getSlaveId());
      ConcreteTaskDescription offer = null;
      try {
        offer = ConfigurationManager.makeConcrete(slaveOffer);
      } catch (ConfigurationManager.TaskDescriptionException e) {
        LOG.log(Level.SEVERE, "Invalid slave offer", e);
        continue;
      }

      for (Map.Entry<String, ConcreteTaskDescription> task : pendingTasks.entries()) {
        if (ConfigurationManager.satisfied(task.getValue(), offer)) {
          // Found an owner for the resource!
          int taskId = nextTaskId++;

          // TODO(wfarner): Remove this hack once nexus core does not read parameters.
          StringMap params = new StringMap();
          params.set("cpus", String.valueOf(task.getValue().getNumCpus()));
          params.set("mem", String.valueOf(task.getValue().getRamBytes()));

          newlyScheduledTasks.add(
              new nexus.TaskDescription(taskId, slaveOffer.getSlaveId(),
                  task.getKey() + "-" + taskId, params, new byte[0]));

          synchronized (scheduledTasks) {
            Map<Integer, ConcreteTaskDescription>
          }

          break;
        }
      }

      for (Map.Entry<String, ConcreteTaskDescription> entry : pendingTasks.entries()) {
        String name = entry.getKey();
        ConcreteTaskDescription config = entry.getValue();
        if (!runningTasks.containsKey(name) || runningTasks.get(name).size() < config.instances) {
          // Need another instance ...
          if (config.cpus >= Integer.parseInt(offer.getParams().get("cpus")) &&
              config.mem >= Integer.parseInt(offer.getParams().get("mem"))) {
            // This offer has enough resources ...
            StringMap params = new StringMap();
            params.set("cpus", offer.getParams().get("cpus"));
            params.set("mem", offer.getParams().get("mem"));

            TaskDescription task = new TaskDescription(nextTaskId, offer.getSlaveId(),
                name + "-" + nextTaskId++, params, new byte[0]);

            newlyScheduledTasks.add(task);

            runningTasks.put(name, task);
            break;
          }
        }
      }
    }
    driver.replyToOffer(oid, newlyScheduledTasks, new StringMap());
  }

  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    for (Map.Entry<String, Collection<TaskDescription>> entry : runningTasks.asMap().entrySet()) {
      Collection<TaskDescription> tasks = entry.getValue();
      for (TaskDescription task : tasks) {
        if (task.getTaskId() == status.getTaskId()) {
          tasks.remove(task);
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
      if (pendingTasks.containsKey(jobDesc.getName())
          || scheduledTasks.containsKey(jobDesc.getName())) {
        return new CreateJobResponse()
            .setResponseCode(ResponseCode.INVALID_REQUEST)
            .setMessage("A job with the name " + jobDesc.getName() + " already exists.");
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
        pendingTasks.putAll(jobDesc.getName(), concreteTasks);
      }

      return new CreateJobResponse().setResponseCode(ResponseCode.OK)
          .setMessage(concreteTasks.size() + " tasks pending for job " + jobDesc.getName());
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
