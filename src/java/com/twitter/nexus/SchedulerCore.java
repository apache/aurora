package com.twitter.nexus;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.twitter.common.base.Closure;
import com.twitter.nexus.gen.ConcreteTaskDescription;
import nexus.FrameworkMessage;
import nexus.SchedulerDriver;
import nexus.SlaveOffer;
import nexus.StringMap;

import java.util.Deque;
import java.util.Iterator;
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

  // Stores the tasks that have been configured with the scheduler, but have not yet been
  // scheduled.
  private final Multimap<String, ConcreteTaskDescription> pendingTasks = ArrayListMultimap.create();

  // Stores the tasks that are currently scheduled.
  private final Map<Integer, ConcreteTaskDescription> scheduledTasks = Maps.newHashMap();

  // Additional indices to serve the above map.
  private final Map<Integer, String> taskIdToJobName = Maps.newHashMap();
  private final Map<Integer, Integer> taskIdToSlaveId = Maps.newHashMap();
  private final Multimap<String, Integer> jobToTaskIds = HashMultimap.create();

  // TODO(wfarner): Hopefully we can abolish (or at least mask) the concept of canonical task IDs
  // in favor of tasks being canonically named by job/taskIndex.
  private int nextTaskId = 0;

  // The nexus framework ID of the scheduler, set to -1 until the framework is registered.
  private final AtomicInteger frameworkId = new AtomicInteger(-1);

  // Stores work to perform using the scheduler driver.
  private Deque<Closure<SchedulerDriver>> workQueue = Lists.newLinkedList();

  public int pendingTaskCount() {
    return pendingTasks.size();
  }

  public int scheduledTaskCount() {
    return scheduledTasks.size();
  }

  public void setFrameworkId(int frameworkId) {
    this.frameworkId.set(frameworkId);
  }

  public boolean hasJob(String jobName) {
    return jobToTaskIds.containsKey(jobName) || pendingTasks.containsKey(jobName);
  }

  /**
   * Adds pending tasks, which will become candidates for scheduling the next time
   * {@link #schedulePendingTask(SlaveOffer)} is called.
   *
   * @param jobName Name of the job that the task is a part of.
   * @param tasks The tasks to schedule.
   */
  public synchronized void addPendingTasks(String jobName,
      Iterable<ConcreteTaskDescription> tasks) {
    pendingTasks.putAll(jobName, tasks);
  }

  /**
   * Schedules one of the pending tasks that is satisfied by {@code slaveOffer}.
   *
   * @param slaveOffer The slave offer.
   * @return A task description that defines the job to run, or {@code null} if there are no pending
   *     tasks that are satisfied by the slave offer.
   */
  public synchronized nexus.TaskDescription schedulePendingTask(SlaveOffer slaveOffer) {
    ConcreteTaskDescription offer;
    try {
      offer = ConfigurationManager.makeConcrete(slaveOffer);
    } catch (ConfigurationManager.TaskDescriptionException e) {
      LOG.log(Level.SEVERE, "Invalid slave offer", e);
      return null;
    }

    Iterator<Map.Entry<String, ConcreteTaskDescription>> pendingIterator =
        pendingTasks.entries().iterator();
    while (pendingIterator.hasNext()) {
      Map.Entry<String, ConcreteTaskDescription> pending = pendingIterator.next();

      String jobName = pending.getKey();
      ConcreteTaskDescription task = pending.getValue();
      if (ConfigurationManager.satisfied(task, offer)) {
        LOG.info("Offer is being assigned to a task within " + jobName);

        // Found an owner for the resource!
        int taskId = nextTaskId++;

        // TODO(wfarner): Remove this hack once nexus core does not read parameters.
        StringMap params = new StringMap();
        LOG.info("Consuming cpus: " + String.valueOf(task.getNumCpus()));
        LOG.info("Consuming memory: " + String.valueOf(task.getRamBytes()));
        params.set("cpus", String.valueOf((int) task.getNumCpus()));
        params.set("mem", String.valueOf(task.getRamBytes()));

        // TODO(wfarner): Need to 'consume' the resouce from the slave offer, since the
        // task requirement might be a fraction of the offer.
        pendingIterator.remove();

        addScheduledTask(jobName, taskId, slaveOffer.getSlaveId(), task);

        return new nexus.TaskDescription(taskId, slaveOffer.getSlaveId(),
                jobName + "-" + taskId, params, new byte[0]);
      }
    }

    return null;
  }

  private void addScheduledTask(String jobName, int taskId, int slaveId,
      ConcreteTaskDescription task) {
    if (scheduledTasks.put(taskId, task) != null) LOG.severe("Collision on task ID " + taskId);
    taskIdToJobName.put(taskId, jobName);
    taskIdToSlaveId.put(taskId, slaveId);
    jobToTaskIds.put(jobName, taskId);
  }

  public synchronized ConcreteTaskDescription getTask(int taskId) {
    return scheduledTasks.get(taskId);
  }

  public synchronized ConcreteTaskDescription removeTask(int taskId) {
    String jobName = taskIdToJobName.remove(taskId);
    jobToTaskIds.remove(jobName, taskId);
    return scheduledTasks.remove(taskId);
  }

  public synchronized void killJob(final String jobName) {
    scheduleDriverWork(new Closure<SchedulerDriver>() {
      @Override public void execute(SchedulerDriver driver) throws RuntimeException {
        LOG.info("Killing job " + jobName);
        pendingTasks.removeAll(jobName);

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
    // TODO(wfarner): Probably need to do this in a cleaner way so that the entire job doesn't
    // flip at once.
    scheduleDriverWork(new Closure<SchedulerDriver>() {
      @Override public void execute(SchedulerDriver driver) throws RuntimeException {
        if (frameworkId.get() == -1) {
          LOG.info("Unable to restart tasks, framework not registered.");
          return;
        }

        LOG.info("Restarting tasks " + taskIds);

        for (int taskId : taskIds) {
          int slaveId = taskIdToSlaveId.get(taskId);
          FrameworkMessage restartMessage = new FrameworkMessage(frameworkId.get(), slaveId,
              new byte[0]);

          driver.sendFrameworkMessage(restartMessage);
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
