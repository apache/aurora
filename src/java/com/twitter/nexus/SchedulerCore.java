package com.twitter.nexus;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.twitter.common.base.Closure;
import com.twitter.nexus.gen.TwitterTaskInfo;
import com.twitter.nexus.gen.ScheduleStatus;
import com.twitter.nexus.gen.TrackedTask;
import nexus.FrameworkMessage;
import nexus.SchedulerDriver;
import nexus.SlaveOffer;
import nexus.StringMap;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.Deque;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Scheduling core, stores scheduler state and makes decisions about which tasks to schedule when
 * a resource offer is made.
 *
 * @author wfarner
 */
class SchedulerCore {
  private static Logger LOG = Logger.getLogger(SchedulerCore.class.getName());

  private final Map<Integer, TrackedTask> tasks = Maps.newHashMap();
  private final Multimap<String, Integer> jobToTaskIds = HashMultimap.create();
  private final TSerializer serializer = new TSerializer();

  // TODO(wfarner): Hopefully we can abolish (or at least mask) the concept of canonical task IDs
  // in favor of tasks being canonically named by job/taskIndex.
  private int nextTaskId = 0;

  // The nexus framework ID of the scheduler, set to -1 until the framework is registered.
  private final AtomicInteger frameworkId = new AtomicInteger(-1);

  // Stores work to perform using the scheduler driver.
  private Deque<Closure<SchedulerDriver>> workQueue = Lists.newLinkedList();

  public void setFrameworkId(int frameworkId) {
    this.frameworkId.set(frameworkId);
  }

  public boolean hasJob(String jobName) {
    return jobToTaskIds.containsKey(jobName);
  }

  public synchronized Iterable<TrackedTask> getTasks(String jobName) {
    ImmutableList.Builder<TrackedTask> taskDescriptions = ImmutableList.builder();
    for (int taskId : jobToTaskIds.get(jobName)) {
      taskDescriptions.add(tasks.get(taskId));
    }
    return taskDescriptions.build();
  }

  private int generateTaskId() {
    return nextTaskId++;
  }

  /**
   * Adds pending tasks, which will become candidates for scheduling the next time
   * {@link #schedulePendingTask(SlaveOffer)} is called.
   *
   * @param jobName Name of the job that the task is a part of.
   * @param newTasks The tasks to schedule.
   */
  public synchronized void addTasks(String jobName, Iterable<TwitterTaskInfo> newTasks) {
    Preconditions.checkNotNull(jobName);

    for (TwitterTaskInfo task : Preconditions.checkNotNull(newTasks)) {
      int taskId = generateTaskId();
      jobToTaskIds.put(jobName, taskId);
      tasks.put(taskId, new TrackedTask()
          .setTaskId(taskId)
          .setJobName(jobName)
          .setTask(task)
          .setStatus(ScheduleStatus.PENDING));
    }
  }

  /**
   * Schedules one of the pending tasks that is satisfied by {@code slaveOffer}.
   *
   * @param slaveOffer The slave offer.
   * @return A task description that defines the job to run, or {@code null} if there are no pending
   *     tasks that are satisfied by the slave offer.
   */
  public synchronized nexus.TaskDescription schedulePendingTask(SlaveOffer slaveOffer) {
    TwitterTaskInfo offer;
    try {
      offer = ConfigurationManager.makeConcrete(slaveOffer);
    } catch (ConfigurationManager.TaskDescriptionException e) {
      LOG.log(Level.SEVERE, "Invalid slave offer", e);
      return null;
    }

    for (Map.Entry<Integer, TrackedTask> taskEntry : tasks.entrySet()) {
      TrackedTask trackedTask = taskEntry.getValue();

      if (trackedTask.status != ScheduleStatus.PENDING) continue;

      String jobName = trackedTask.jobName;
      TwitterTaskInfo concreteTaskDescription = trackedTask.getTask();
      if (ConfigurationManager.satisfied(concreteTaskDescription, offer)) {
        LOG.info("Offer is being assigned to a concreteTaskDescription within " + jobName);

        // TODO(wfarner): Remove this hack once nexus core does not read parameters.
        StringMap params = new StringMap();
        LOG.info("Consuming cpus: " + String.valueOf(concreteTaskDescription.getNumCpus()));
        LOG.info("Consuming memory: " + String.valueOf(concreteTaskDescription.getRamBytes()));
        params.set("cpus", String.valueOf((int) concreteTaskDescription.getNumCpus()));
        params.set("mem", String.valueOf(concreteTaskDescription.getRamBytes()));

        // TODO(wfarner): Need to 'consume' the resouce from the slave offer, since the
        // concreteTaskDescription requirement might be a fraction of the offer.
        trackedTask.status = ScheduleStatus.STARTING;
        trackedTask.slaveId = slaveOffer.getSlaveId();
        byte[] taskInBytes = null;
        try {
          taskInBytes = serializer.serialize(concreteTaskDescription);
        } catch (TException e) {
           LOG.log(Level.SEVERE,"Error serializing Thrift TwitterTaskInfo",e);
          //todo(flo):maybe cleanup and exit cleanly
          throw new RuntimeException(e);
        }

        return new nexus.TaskDescription(trackedTask.getTaskId(), slaveOffer.getSlaveId(),
                jobName + "-" + trackedTask.getTaskId(), params, taskInBytes);
      }
    }

    return null;
  }

  public synchronized TrackedTask getTask(int taskId) {
    return tasks.get(taskId);
  }

  public synchronized void setTaskStatus(int taskId, ScheduleStatus status) {
    TrackedTask task = tasks.get(taskId);
    if (task != null) task.setStatus(Preconditions.checkNotNull(status));
  }

  public synchronized TrackedTask removeTask(int taskId) {
    TrackedTask task = tasks.remove(taskId);
    if (task != null) {
      jobToTaskIds.remove(task.getJobName(), taskId);
    }
    return task;
  }

  public synchronized void killJob(final String jobName) {
    // Remove all pending tasks for the job.
    Iterables.removeIf(tasks.entrySet(), new Predicate<Map.Entry<Integer, TrackedTask>>() {
      @Override public boolean apply(Map.Entry<Integer, TrackedTask> entry) {
        TrackedTask task = entry.getValue();
        boolean remove = task.status == ScheduleStatus.PENDING;
        if (remove) jobToTaskIds.remove(task.jobName, task.getTaskId());

        return remove;
      }
    });

    scheduleDriverWork(new Closure<SchedulerDriver>() {
      @Override public void execute(SchedulerDriver driver) throws RuntimeException {
        LOG.info("Killing job " + jobName);

        for (int taskId : jobToTaskIds.get(jobName)) {
          driver.killTask(taskId);
        }
      }
    });
  }

  public synchronized void killTasks(final Set<Integer> taskIds) {
    scheduleDriverWork(new Closure<SchedulerDriver>() {
      @Override public void execute(SchedulerDriver driver) throws RuntimeException {
        LOG.info("Killing tasks " + taskIds);

        for (int taskId : taskIds) {
          driver.killTask(taskId);
        }
      }
    });
  }

  public synchronized void restartTasks(final Set<Integer> taskIds) {
    // TODO(wfarner): Need to do this in a cleaner way so that the entire job doesn't flip at once.
    scheduleDriverWork(new Closure<SchedulerDriver>() {
      @Override public void execute(SchedulerDriver driver) throws RuntimeException {
        if (frameworkId.get() == -1) {
          LOG.info("Unable to restart tasks, framework not registered.");
          return;
        }

        LOG.info("Restarting tasks " + taskIds);

        for (int taskId : taskIds) {
          TrackedTask task = tasks.get(taskId);
          if (task != null && task.status != ScheduleStatus.PENDING) {
            // TODO(wfarner): Once scheduler -> executorHub communication is defined, replace the
            // empty byte array with a serialized message.
            driver.sendFrameworkMessage(new FrameworkMessage(frameworkId.get(), task.slaveId,
                new byte[0]));
          }
        }
      }
    });
  }

  private void scheduleDriverWork(Closure<SchedulerDriver> work) {
    workQueue.addLast(Preconditions.checkNotNull(work));
  }

  public synchronized void clearWorkQueue(SchedulerDriver driver) {
    for (Closure<SchedulerDriver> work : workQueue) {
      work.execute(driver);
    }
  }
}
