package com.twitter.nexus.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.twitter.common.base.Closure;
import com.twitter.nexus.scheduler.ConfigurationManager;
import com.twitter.nexus.gen.TwitterTaskInfo;
import com.twitter.nexus.gen.ScheduleStatus;
import com.twitter.nexus.gen.TrackedTask;
import nexus.FrameworkMessage;
import nexus.SchedulerDriver;
import nexus.SlaveOffer;
import nexus.StringMap;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import javax.annotation.Nullable;
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
public class SchedulerCore {
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

  /**
   * Assigns a framework ID to the scheduler, should be called when the scheduler implementation
   * has received a successful registration signal.
   *
   * @param frameworkId Framework ID.
   */
  public void setFrameworkId(int frameworkId) {
    this.frameworkId.set(frameworkId);
  }

  public boolean hasJob(String jobName) {
    return jobToTaskIds.containsKey(jobName);
  }

  /**
   * Fetches information about all registered tasks for a job.
   *
   * @param jobName The job to look up tasks for.
   * @return An iterable of task objects.
   */
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
   * Fetches an individual task by ID.
   *
   * @param taskId The task to fetch.
   * @return The task object associated with ID {@code taskId} or {@code null} if no such task
   *   exists.
   */
  public synchronized TrackedTask getTask(int taskId) {
    return tasks.get(taskId);
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

    for (TrackedTask task
        : Iterables.filter(tasks.values(), new TaskFilter(ScheduleStatus.PENDING))) {
      String jobName = task.jobName;
      TwitterTaskInfo concreteTaskDescription = task.getTask();
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
        task.status = ScheduleStatus.STARTING;
        task.slaveId = slaveOffer.getSlaveId();
        byte[] taskInBytes = null;
        try {
          taskInBytes = serializer.serialize(concreteTaskDescription);
        } catch (TException e) {
           LOG.log(Level.SEVERE,"Error serializing Thrift TwitterTaskInfo",e);
          //todo(flo):maybe cleanup and exit cleanly
          throw new RuntimeException(e);
        }

        return new nexus.TaskDescription(task.getTaskId(), slaveOffer.getSlaveId(),
                jobName + "-" + task.getTaskId(), params, taskInBytes);
      }
    }

    return null;
  }

  /**
   * Assigns a new state to a task.
   *
   * @param taskId ID of the task changing state.
   * @param status The new state of the task.
   */
  public synchronized void setTaskStatus(int taskId, ScheduleStatus status) {
    TrackedTask task = tasks.get(taskId);
    if (task != null) task.setStatus(Preconditions.checkNotNull(status));
  }

  /**
   * Removes a task from scheduler tracking.
   * Note: This does not actually alter a running task, it simply removes it from tracking.
   *
   * @param taskId The task to remove.
   * @return The task object that was removed, or {@code null} if no such task was found.
   */
  public synchronized TrackedTask removeTask(int taskId) {
    TrackedTask task = tasks.remove(taskId);
    if (task != null) {
      jobToTaskIds.remove(task.getJobName(), taskId);
    }
    return task;
  }

  /**
   * Kills a running job and terminates all of its tasks.
   *
   *
   * @param jobName The job to kill.
   */
  public synchronized void killJob(final String jobName) {
    Preconditions.checkNotNull(jobName);

    // Remove all pending tasks for the job.
    Iterables.removeIf(tasks.entrySet(), new Predicate<Map.Entry<Integer, TrackedTask>>() {
      @Override public boolean apply(Map.Entry<Integer, TrackedTask> entry) {
        TrackedTask task = entry.getValue();
        boolean remove = task.status == ScheduleStatus.PENDING && task.jobName.equals(jobName);
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

  /**
   * Kills a specific set of tasks.
   *
   * @param taskIds The tasks to kill.
   */
  public synchronized void killTasks(final Set<Integer> taskIds) {
    Preconditions.checkNotNull(taskIds);

    scheduleDriverWork(new Closure<SchedulerDriver>() {
      @Override public void execute(SchedulerDriver driver) throws RuntimeException {
        LOG.info("Killing tasks " + taskIds);

        for (int taskId : taskIds) {
          driver.killTask(taskId);
        }
      }
    });
  }

  /**
   * Schedules a restart on a set of tasks.
   *
   * @param taskIds The tasks to restart.
   */
  public synchronized void restartTasks(final Set<Integer> taskIds) {
    Preconditions.checkNotNull(taskIds);

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

  /**
   * A filter to return only tasks that are in a specific set of states.
   */
  private class TaskFilter implements Predicate<TrackedTask> {
    private final Set<ScheduleStatus> statuses;

    public TaskFilter(ScheduleStatus... statuses) {
      this.statuses = ImmutableSet.copyOf(Preconditions.checkNotNull(statuses));
    }

    @Override
    public boolean apply(TrackedTask trackedTask) {
      return statuses.contains(trackedTask.getStatus());
    }
  }
}
