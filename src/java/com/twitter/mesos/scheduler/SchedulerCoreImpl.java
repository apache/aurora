package com.twitter.mesos.scheduler;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.Message;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.ExecutorMessage;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.LiveTaskInfo;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.RestartExecutor;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.SchedulerState;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TrackedTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.persistence.PersistenceLayer;
import mesos.TaskDescription;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.twitter.mesos.gen.ScheduleStatus.*;

/**
 * Implementation of the scheduler core.
 *
 * @author wfarner
 */
public class SchedulerCoreImpl implements SchedulerCore {
  private static final Logger LOG = Logger.getLogger(SchedulerCore.class.getName());

  // Schedulers that are responsible for triggering execution of jobs.
  private final List<JobManager> jobManagers;

  private final AtomicInteger nextTaskId = new AtomicInteger(0);

  // The mesos framework ID of the scheduler, set to null until the framework is registered.
  private final AtomicReference<String> frameworkId = new AtomicReference<String>(null);

  // Stores the configured tasks.
  private final TaskStore taskStore = new TaskStore();

  // Work queue that stores pending asynchronous tasks.
  private final WorkQueue workQueue;

  private final PersistenceLayer<SchedulerState> persistenceLayer;
  private final ExecutorTracker executorTracker;

  // Scheduler driver used for communication with other nodes in the cluster.
  private final AtomicReference<Driver> schedulerDriver = new AtomicReference<Driver>();

  @Inject
  public SchedulerCoreImpl(CronJobManager cronScheduler, ImmediateJobManager immediateScheduler,
      PersistenceLayer<SchedulerState> persistenceLayer, ExecutorTracker executorTracker,
      WorkQueue workQueue) {
    // The immediate scheduler will accept any job, so it's important that other schedulers are
    // placed first.
    jobManagers = Arrays.asList(cronScheduler, immediateScheduler);
    this.persistenceLayer = Preconditions.checkNotNull(persistenceLayer);
    this.executorTracker = Preconditions.checkNotNull(executorTracker);
    this.workQueue = Preconditions.checkNotNull(workQueue);

    restore();
  }

  @Override
  public void registered(Driver driver, String frameworkId) {
    this.schedulerDriver.set(Preconditions.checkNotNull(driver));
    this.frameworkId.set(Preconditions.checkNotNull(frameworkId));
    persist();

    executorTracker.start(new Closure<String>() {
      @Override public void execute(String slaveId) throws RuntimeException {
        try {
          LOG.info("Sending restart request to executor " + slaveId);
          ExecutorMessage message = new ExecutorMessage();
          message.setRestartExecutor(new RestartExecutor());

          sendExecutorMessage(slaveId, message);
        } catch (ThriftBinaryCodec.CodingException e) {
          LOG.log(Level.WARNING, "Failed to send executor status.", e);
        }
      }
    });
  }

  private void sendExecutorMessage(String slaveId, ExecutorMessage message)
      throws ThriftBinaryCodec.CodingException {
    Driver driverRef = schedulerDriver.get();
    if (driverRef == null) {
      LOG.info("No driver available, unable to send executor status.");
      return;
    }

    int result = driverRef.sendMessage(new Message(slaveId, message));
    if (result != 0) {
      LOG.warning("Executor message failed to send, return code " + result);
    }
  }

  @Override
  public synchronized Iterable<TrackedTask> getTasks(final TaskQuery query) {
    return getTasks(query, Predicates.<TrackedTask>alwaysTrue());
  }

  @Override
  public synchronized Iterable<TrackedTask> getTasks(final TaskQuery query,
      Predicate<TrackedTask>... filters) {
    return taskStore.fetch(query, filters);
  }

  @Override
  public synchronized boolean hasActiveJob(final String owner, final String jobName) {
    TaskQuery query = new TaskQuery().setOwner(owner).setJobName(jobName)
        .setStatuses(Tasks.ACTIVE_STATES);
    return !Iterables.isEmpty(getTasks(query)) || Iterables.any(jobManagers,
        new Predicate<JobManager>() {
          @Override public boolean apply(JobManager manager) {
            return manager.hasJob(owner, jobName);
          }
    });
  }

  private int generateTaskId() {
    return nextTaskId.incrementAndGet();
  }

  // TODO(wfarner): This is does not currently clear out tasks when a host is decommissioned.
  //    Figure out a solution that will work.  Might require mesos support for fetching the list
  //    of slaves.
  @Override
  public synchronized void updateRegisteredTasks(RegisteredTaskUpdate update) {
    Preconditions.checkNotNull(update);
    Preconditions.checkNotNull(update.getTaskInfos());

    try {
      final Map<Integer, LiveTaskInfo> taskInfoMap = Maps.newHashMap();

      for (LiveTaskInfo taskInfo : update.getTaskInfos()) {
        taskInfoMap.put(taskInfo.getTaskId(), taskInfo);
      }

      // TODO(wfarner): Have the scheduler only retain configurations for live jobs,
      //    and acquire all other state from slaves.
      //    This will allow the scheduler to only persist active tasks.

      // Look for any tasks that we don't know about, or this slave should not be modifying.
      final Set<Integer> recognizedTasks = Sets.newHashSet(Iterables.transform(
          taskStore.fetch(
              new TaskQuery().setTaskIds(taskInfoMap.keySet()).setSlaveHost(update.getSlaveHost())),
          Tasks.GET_TASK_ID));
      Set<Integer> unknownTasks = Sets.difference(taskInfoMap.keySet(), recognizedTasks);
      if (!unknownTasks.isEmpty()) {
        LOG.severe("Received task info update from executor " + update.getSlaveHost()
                   + " for tasks unknown or belonging to a different host: " + unknownTasks);
      }

      // Remove unknown tasks from the request to prevent badness later.
      taskInfoMap.keySet().removeAll(unknownTasks);

      // Update the resource information for the tasks that we currently have on record.
      taskStore.mutate(new TaskQuery().setTaskIds(recognizedTasks),
          new ExceptionalClosure<TrackedTask, RuntimeException>() {
        @Override public void execute(TrackedTask task) {
          LiveTaskInfo update = taskInfoMap.get(task.getTaskId());
          Preconditions.checkNotNull(update);
          task.setResources(update.getResources());
        }
      });

      Predicate<LiveTaskInfo> getKilledTasks = new Predicate<LiveTaskInfo>() {
        @Override public boolean apply(LiveTaskInfo update) {
          return update.getStatus() == KILLED;
        }
      };

      Function<LiveTaskInfo, Integer> getTaskId = new Function<LiveTaskInfo, Integer>() {
        @Override public Integer apply(LiveTaskInfo update) {
          return update.getTaskId();
        }
      };

      // Find any tasks that we believe to be running, but the slave reports as dead.
      Set<Integer> reportedDeadTasks = Sets.newHashSet(
          Iterables.transform(Iterables.filter(taskInfoMap.values(), getKilledTasks), getTaskId));
      Set<Integer> deadTasks = Sets.newHashSet(
          Iterables.transform(
              taskStore.fetch(new TaskQuery().setTaskIds(reportedDeadTasks)
                  .setStatuses(Sets.newHashSet(RUNNING))),
              Tasks.GET_TASK_ID));
      if (!deadTasks.isEmpty()) {
        LOG.info("Found tasks that were recorded as RUNNING but slave " + update.getSlaveHost()
                 + " reports as KILLED: " + deadTasks);
        setTaskStatus(new TaskQuery().setTaskIds(deadTasks), KILLED);
      }

      // Find any tasks assigned to this slave but the slave does not report.
      Predicate<TrackedTask> isTaskReported = new Predicate<TrackedTask>() {
        @Override public boolean apply(TrackedTask task) {
          return recognizedTasks.contains(task.getTaskId());
        }
      };
      Predicate<TrackedTask> lastEventBeyondGracePeriod = new Predicate<TrackedTask>() {
        @Override public boolean apply(TrackedTask task) {
          long taskAgeMillis = System.currentTimeMillis()
              - Iterables.getLast(task.getTaskEvents()).getTimestamp();
          return taskAgeMillis > Amount.of(10, Time.MINUTES).as(Time.MILLISECONDS);
        }
      };

      TaskQuery slaveAssignedTaskQuery = new TaskQuery().setSlaveHost(update.getSlaveHost());
      Set<TrackedTask> missingNotRunningTasks = Sets.newHashSet(
          taskStore.fetch(slaveAssignedTaskQuery,
              Predicates.not(isTaskReported),
              Predicates.not(Tasks.makeStatusFilter(RUNNING)),
              lastEventBeyondGracePeriod));
      if (!missingNotRunningTasks.isEmpty()) {
        LOG.info("Removing non-running tasks no longer reported by slave " + update.getSlaveHost()
                 + ": " + Iterables.transform(missingNotRunningTasks, Tasks.GET_TASK_ID));
        taskStore.remove(missingNotRunningTasks);
      }

      Set<TrackedTask> missingRunningTasks = Sets.newHashSet(
          taskStore.fetch(slaveAssignedTaskQuery,
              Predicates.not(isTaskReported),
              Tasks.makeStatusFilter(RUNNING)));
      if (!missingRunningTasks.isEmpty()) {
        Set<Integer> missingIds = Sets.newHashSet(
            Iterables.transform(missingRunningTasks, Tasks.GET_TASK_ID));
        LOG.info("Slave " + update.getSlaveHost() + " no longer reports running tasks: "
                 + missingIds + ", reporting as LOST.");
        setTaskStatus(new TaskQuery().setTaskIds(missingIds), LOST);
      }
    } catch (Throwable t) {
      LOG.log(Level.WARNING, "Uncaught exception.", t);
    }

    persist();
  }

  @Override
  public synchronized void createJob(JobConfiguration job) throws ScheduleException,
      ConfigurationManager.TaskDescriptionException {
    Preconditions.checkNotNull(job);

    for (TwitterTaskInfo config : job.getTaskConfigs()) {
      ConfigurationManager.populateFields(job, config);
    }

    if (hasActiveJob(job.getOwner(), job.getName())) {
      throw new ScheduleException(String.format("Job already exists: %s/%s",
          job.getOwner(), job.getName()));
    }

    boolean accepted = false;
    for (JobManager manager : jobManagers) {
      if (manager.receiveJob(job)) {
        accepted = true;
        LOG.info("Job accepted by scheduler: " + manager.getClass().getName());
        persist();
        break;
      }
    }

    if (!accepted) {
      LOG.severe("Job was not accepted by any of the configured schedulers, discarding.");
      LOG.severe("Discarded job: " + job);
      throw new ScheduleException("Job not accepted, discarding.");
    }
  }

  @Override
  public synchronized void runJob(JobConfiguration job) {
    List<TrackedTask> newTasks = Lists.newArrayList();

    for (TwitterTaskInfo task : Preconditions.checkNotNull(job.getTaskConfigs())) {
      int taskId = generateTaskId();
      TrackedTask trackedTask = changeTaskStatus(new TrackedTask()
          .setTaskId(taskId)
          .setJobName(job.getName())
          .setOwner(job.getOwner())
          .setTask(task), PENDING);

      newTasks.add(trackedTask);
    }

    taskStore.add(newTasks);
    persist();
  }

  @Override
  public synchronized TwitterTask offer(final String slaveId, final String slaveHost,
      Map<String, String> offerParams) throws ScheduleException {
    MorePreconditions.checkNotBlank(slaveId);
    MorePreconditions.checkNotBlank(slaveHost);
    Preconditions.checkNotNull(offerParams);

    final TwitterTaskInfo offer;
    try {
      offer = ConfigurationManager.makeConcrete(offerParams);
    } catch (ConfigurationManager.TaskDescriptionException e) {
      LOG.log(Level.SEVERE, "Invalid slave offer", e);
      return null;
    }

    TaskQuery query = new TaskQuery();
    query.addToStatuses(PENDING);

    Predicate<TrackedTask> satisfiedFilter = new Predicate<TrackedTask>() {
      @Override public boolean apply(TrackedTask task) {
        return ConfigurationManager.satisfied(task.getTask(), offer);
      }
    };

    Iterable<TrackedTask> candidates = taskStore.fetch(query, satisfiedFilter);
    if (Iterables.isEmpty(candidates)) return null;

    LOG.info("Found " + Iterables.size(candidates) + " candidates for offer.");

    TrackedTask task = Iterables.get(candidates, 0);
    task = taskStore.mutate(task, new ExceptionalClosure<TrackedTask, RuntimeException>() {
        @Override public void execute(TrackedTask mutable) {
          mutable.setSlaveId(slaveId).setSlaveHost(slaveHost);
          changeTaskStatus(mutable, STARTING);
        }
    });

    if (task == null) {
      LOG.log(Level.SEVERE, "Unable to find matching mutable task!");
      return null;
    }

    // TODO(wfarner): Remove this hack once mesos core does not read parameters.
    Map<String, String> params = ImmutableMap.of(
      "cpus", String.valueOf((int) task.getTask().getNumCpus()),
      "mem", String.valueOf(task.getTask().getRamMb())
    );

    LOG.info(String.format("Offer on slave %s (id %s) is being assigned task for %s/%s.",
        slaveHost, task.slaveId, task.getTask().getOwner(), task.getTask().getJobName()));

    persist();
    return new TwitterTask(task.getTaskId(), slaveId, task.jobName + "-" + task.getTaskId(), params,
        task.getTask());
  }

  private void scheduleTaskCopies(List<TrackedTask> tasks) {
    for (TrackedTask task : tasks) {
      task.unsetSlaveId();
      task.unsetSlaveHost();
      task.unsetResources();
      task.unsetTaskEvents();
      task.setAncestorId(task.getTaskId());
      task.setTaskId(generateTaskId());
      changeTaskStatus(task, PENDING);
    }

    LOG.info("Tasks being rescheduled: " + tasks);

    taskStore.add(tasks);
  }

  /**
   * Sets the current status for a task, and records the status change into the task events
   * audit log.
   *
   * @param task Task whose status is changing.
   * @param status New status for the task.
   * @return A reference to the task.
   */
  private TrackedTask changeTaskStatus(TrackedTask task, ScheduleStatus status) {
    task.setStatus(status);
    task.addToTaskEvents(new TaskEvent()
        .setTimestamp(System.currentTimeMillis())
        .setStatus(status));
    return task;
  }

  @Override
  public synchronized void setTaskStatus(TaskQuery query, final ScheduleStatus status) {
    Preconditions.checkNotNull(query);
    Preconditions.checkNotNull(status);

    // Only allow state transition from non-terminal state.
    Iterable<TrackedTask> modifiedTasks = getTasks(query, Predicates.not(Tasks.TERMINATED_FILTER));

    final List<TrackedTask> newTasks = Lists.newLinkedList();

    switch (status) {
      case PENDING:
      case STARTING:
      case RUNNING:
        // Simply assign the new state.
        taskStore.mutate(modifiedTasks, new ExceptionalClosure<TrackedTask, RuntimeException>() {
          @Override public void execute(TrackedTask mutable) {
            changeTaskStatus(mutable, status);
          }
        });

        break;

      case FINISHED:
        // Assign the FINISHED state to non-daemon tasks, move daemon tasks to PENDING.
        taskStore.mutate(modifiedTasks, new ExceptionalClosure<TrackedTask, RuntimeException>() {
          @Override public void execute(TrackedTask mutable) {
            if (mutable.getTask().isIsDaemon()) {
              LOG.info("Rescheduling daemon task " + mutable.getTaskId());
              newTasks.add(new TrackedTask(mutable));
            }

            changeTaskStatus(mutable, FINISHED);
          }
        });

        break;

      case FAILED:
        // Increment failure count, move to pending state, unless failure limit has been reached.
        taskStore.mutate(modifiedTasks, new ExceptionalClosure<TrackedTask, RuntimeException>() {
          @Override public void execute(TrackedTask mutable) {
            mutable.setFailureCount(mutable.getFailureCount() + 1);

            boolean failureLimitReached = (mutable.getTask().getMaxTaskFailures() != -1)
                && (mutable.getFailureCount() >= mutable.getTask().getMaxTaskFailures());

            if (!failureLimitReached) {
              LOG.info("Rescheduling failed task below failure limit: " + mutable.getTaskId());
              newTasks.add(new TrackedTask(mutable));
            }

            changeTaskStatus(mutable, FAILED);
          }
        });

        break;

      case KILLED:
        taskStore.mutate(modifiedTasks, new ExceptionalClosure<TrackedTask, RuntimeException>() {
          @Override public void execute(TrackedTask mutable) {
            // This can happen when the executor is killed, or the task process itself is killed.
            LOG.info("Rescheduling " + status + " task: " + mutable.getTaskId());
            newTasks.add(new TrackedTask(mutable));
            changeTaskStatus(mutable, status);
          }
        });

        break;

      case LOST:
      case NOT_FOUND:
        // Move to pending state.
        taskStore.mutate(modifiedTasks, new ExceptionalClosure<TrackedTask, RuntimeException>() {
          @Override public void execute(TrackedTask mutable) {
            LOG.info("Rescheduling " + status + " task: " + mutable.getTaskId());
            newTasks.add(new TrackedTask(mutable));
            changeTaskStatus(mutable, status);
          }
        });

        break;

      default:
        LOG.severe("Unknown schedule status " + status + " cannot be applied to query " + query);
    }

    if (newTasks.isEmpty()) return;
    scheduleTaskCopies(newTasks);
    persist();
  }

  @Override
  public synchronized void killTasks(final TaskQuery query) throws ScheduleException {
    Preconditions.checkNotNull(query);

    LOG.info("Killing tasks matching " + query);

    // If this looks like a query for all tasks in a job, instruct the scheduler modules to delete
    // the job.
    boolean matchingScheduler = false;
    if (!StringUtils.isEmpty(query.getOwner()) && !StringUtils.isEmpty(query.getJobName())
        && query.getStatusesSize() == 0
        && query.getTaskIdsSize() == 0) {
      for (JobManager manager : jobManagers) {
        if (manager.deleteJob(query.getOwner(), query.getJobName())) matchingScheduler = true;
      }
    }

    // KillTasks will not change state of terminated tasks.
    query.setStatuses(Tasks.ACTIVE_STATES);

    Iterable<TrackedTask> toKill = getTasks(query);

    if (!matchingScheduler && Iterables.isEmpty(toKill)) {
      throw new ScheduleException("No tasks matching query found.");
    }

    List<TrackedTask> toRemove = Lists.newArrayList();
    for (final TrackedTask task : toKill) {
      if (task.getStatus() == PENDING) {
        toRemove.add(task);
      } else {
        taskStore.mutate(task, new Closure<TrackedTask>() {
          @Override public void execute(TrackedTask mutable) {
            changeTaskStatus(mutable, KILLED_BY_CLIENT);
          }
        });

        doWorkWithDriver(new Function<Driver, Integer>() {
          @Override public Integer apply(Driver driver) {
            return driver.killTask(task.getTaskId());
          }
        });
      }
    }

    taskStore.remove(toRemove);
    persist();
  }

  /**
   * Executes a unit of work that uses the scheduler driver.  This exists as a convenience function
   * for any tasks that require use of the {@link Driver}, for automatic retrying in the
   * event that the driver is not available.
   *
   * @param work The work to execute.  Should return the status code provided by the driver
   *    (0 denotes success, non-zero denotes a failure that should be retried).
   */
  private void doWorkWithDriver(final Function<Driver, Integer> work) {
    workQueue.doWork(new Callable<Boolean>() {
      @Override public Boolean call() {
        if (frameworkId.get() == null) {
          LOG.info("Unable to send framework messages, framework not registered.");
          return false;
        }

        Driver driver = schedulerDriver.get();
        return driver != null && work.apply(driver) == 0;
      }
    });
  }

  @Override
  public synchronized Set<Integer> restartTasks(Set<Integer> taskIds) {
    MorePreconditions.checkNotBlank(taskIds);
    LOG.info("Restart requested for tasks " + taskIds);

    Iterable<TrackedTask> tasks = getTasks(new TaskQuery().setTaskIds(taskIds));
    if (Iterables.size(tasks) != taskIds.size()) {
      Set<Integer> unknownTasks = Sets.difference(taskIds,
          Sets.newHashSet(Iterables.transform(tasks, Tasks.GET_TASK_ID)));

      LOG.warning("Restart requested for unknown tasks " + unknownTasks);
    }

    Iterable<TrackedTask> activeTasks = Iterables.filter(tasks, Tasks.ACTIVE_FILTER);
    Iterable<TrackedTask> inactiveTasks = Iterables.filter(tasks,
        Predicates.not(Tasks.ACTIVE_FILTER));
    if (!Iterables.isEmpty(inactiveTasks)) {
      LOG.warning("Restart request rejected for inactive tasks "
                  + Iterables.transform(inactiveTasks, Tasks.GET_TASK_ID));
    }

    for (final TrackedTask task : activeTasks) {
      TrackedTask copy = new TrackedTask(task);
      taskStore.mutate(task, new Closure<TrackedTask>() {
        @Override public void execute(TrackedTask mutable) {
          changeTaskStatus(mutable, KILLED_BY_CLIENT);
        }
      });

      scheduleTaskCopies(Arrays.asList(copy));

      if (task.status != PENDING) {
        doWorkWithDriver(new Function<Driver, Integer>() {
          @Override public Integer apply(Driver driver) {
            return driver.killTask(task.getTaskId());
          }
        });
      }
    }

    persist();

    return Sets.newHashSet(Iterables.transform(activeTasks, Tasks.GET_TASK_ID));
  }

  private void persist() {
    LOG.info("Saving scheduler state.");
    SchedulerState state = new SchedulerState();
    state.setFrameworkId(frameworkId.get());
    state.setNextTaskId(nextTaskId.get());
    state.setConfiguredTasks(Lists.newArrayList(taskStore.fetch(new TaskQuery())));
    Map<String, List<JobConfiguration>> moduleState = Maps.newHashMap();
    for (JobManager manager : jobManagers) {
      // TODO(wfarner): This is fragile - stored state will not survive a code refactor.
      moduleState.put(manager.getClass().getCanonicalName(),
          Lists.newArrayList(manager.getState()));
    }
    state.setModuleJobs(moduleState);

    try {
      persistenceLayer.commit(state);
    } catch (PersistenceLayer.PersistenceException e) {
      LOG.log(Level.SEVERE, "Failed to persist scheduler state.", e);
    }
  }

  @Override
  public String getFrameworkId() {
    return frameworkId.get();
  }

  private void restore() {
    LOG.info("Attempting to recover persisted state.");

    SchedulerState state;
    try {
      state = persistenceLayer.fetch();
      if (state == null) {
        LOG.info("No persisted state found for restoration.");
        return;
      }
    } catch (PersistenceLayer.PersistenceException e) {
      LOG.log(Level.SEVERE, "Failed to fetch persisted state.", e);
      return;
    }

    frameworkId.set(state.getFrameworkId());
    nextTaskId.set((int) state.getNextTaskId());
    taskStore.add(state.getConfiguredTasks());

    for (final Map.Entry<String, List<JobConfiguration>> entry : state.getModuleJobs().entrySet()) {
      JobManager manager = Iterables.find(jobManagers, new Predicate<JobManager>() {
        @Override public boolean apply(JobManager scheduler) {
          return scheduler.getClass().getCanonicalName().equals(entry.getKey());
        }
      });

      for (JobConfiguration job : entry.getValue()) {
        try {
          manager.receiveJob(job);
        } catch (ScheduleException e) {
          LOG.log(Level.SEVERE, "While trying to restore state, scheduler module failed.", e);
        }
      }
    }
  }
}
