package com.twitter.mesos.scheduler;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.mesos.ExecutorMessageMux;
import com.twitter.mesos.FrameworkMessageCodec;
import com.twitter.mesos.codec.Codec;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.ExecutorMessage;
import com.twitter.mesos.gen.ExecutorMessageType;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.LiveTaskInfo;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.RestartExecutor;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.SchedulerState;
import com.twitter.mesos.gen.SchedulingSignal;
import com.twitter.mesos.gen.SignalType;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TrackedTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.persistence.PersistenceLayer;
import mesos.FrameworkMessage;
import mesos.SchedulerDriver;
import mesos.SlaveOffer;
import mesos.StringMap;
import mesos.TaskDescription;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of the scheduler core.
 *
 * @author wfarner
 */
public class SchedulerCoreImpl implements SchedulerCore {
  private static Logger LOG = Logger.getLogger(SchedulerCore.class.getName());

  private static final Codec<TwitterTaskInfo, byte[]> TASK_CODEC =
      new ThriftBinaryCodec<TwitterTaskInfo>(TwitterTaskInfo.class);

  private static final Codec<SchedulingSignal, byte[]> SIGNAL_CODEC =
      new ThriftBinaryCodec<SchedulingSignal>(SchedulingSignal.class);

  // Schedulers that are responsible for triggering execution of jobs.
  private final List<JobManager> jobManagers;

  private AtomicInteger nextTaskId = new AtomicInteger(0);

  // The mesos framework ID of the scheduler, set to null until the framework is registered.
  private final AtomicReference<String> frameworkId = new AtomicReference<String>(null);

  // Stores the configured tasks.
  private TaskStore taskStore = new TaskStore();

  // Work queue that stores pending asynchronous tasks.
  @Inject private WorkQueue workQueue;

  private final PersistenceLayer<SchedulerState> persistenceLayer;
  private final ExecutorTracker executorTracker;

  // Scheduler driver used for communication with other nodes in the cluster.
  private final AtomicReference<SchedulerDriver> schedulerDriver =
      new AtomicReference<SchedulerDriver>();

  @Inject
  public SchedulerCoreImpl(CronJobManager cronScheduler, ImmediateJobManager immediateScheduler,
      PersistenceLayer<SchedulerState> persistenceLayer,
      ExecutorTracker executorTracker) {
    // The immediate scheduler will accept any job, so it's important that other schedulers are
    // placed first.
    jobManagers = Arrays.asList(cronScheduler, immediateScheduler);
    this.persistenceLayer = Preconditions.checkNotNull(persistenceLayer);
    this.executorTracker = Preconditions.checkNotNull(executorTracker);

    restore();
  }

  @Override
  public void registered(SchedulerDriver driver, String frameworkId) {
    this.schedulerDriver.set(Preconditions.checkNotNull(driver));
    this.frameworkId.set(Preconditions.checkNotNull(frameworkId));
    persist();

    executorTracker.start(new Closure<String>() {
      @Override public void execute(String slaveId) throws RuntimeException {
        try {
          sendExecutorMessage(slaveId, ExecutorMessageMux.mux(ExecutorMessageType.RESTART_EXECUTOR,
              RestartExecutor.class, new RestartExecutor()));
        } catch (Codec.CodingException e) {
          LOG.log(Level.WARNING, "Failed to send executor status.", e);
        }
      }
    });
  }

  private void sendExecutorMessage(String slaveId, ExecutorMessage message)
      throws Codec.CodingException {
    SchedulerDriver driverRef = schedulerDriver.get();
    if (driverRef == null) {
      LOG.info("No driver available, unable to send executor status.");
      return;
    }

    FrameworkMessage frameworkMessage = new FrameworkMessageCodec<ExecutorMessage>(
        ExecutorMessage.class).encode(message);
    frameworkMessage.setSlaveId(slaveId);

    int result = driverRef.sendFrameworkMessage(frameworkMessage);
    if (result != 0) {
      LOG.warning("Executor message failed to send, return code " + result);
    }
  }

  @Override
  public synchronized Iterable<TrackedTask> getTasks(final TaskQuery query,
      Predicate<TrackedTask>... filters) {
    return taskStore.fetch(query, filters);
  }

  @Override
  public synchronized boolean hasActiveJob(final String owner, final String jobName) {
    TaskQuery query = new TaskQuery().setOwner(owner).setJobName(jobName)
        .setStatuses(Sets.newHashSet(ScheduleStatus.PENDING, ScheduleStatus.STARTING,
            ScheduleStatus.RUNNING));
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
  public void updateRegisteredTasks(RegisteredTaskUpdate update) {
    Preconditions.checkNotNull(update);

    try {
      final Map<Integer, LiveTaskInfo> taskInfoMap = Maps.newHashMap();
      for (LiveTaskInfo taskInfo : update.getTaskInfos()) {
        taskInfoMap.put(taskInfo.getTaskId(), taskInfo);
      }

      LOG.info(String.format("Slave %s reports records for tasks %s", update.getSlaveHost(),
          taskInfoMap.keySet()));

      // Update the resource information for the tasks that we currently have on record.
      taskStore.mutate(new TaskQuery().setTaskIds(taskInfoMap.keySet()),
          new ExceptionalClosure<TrackedTask, RuntimeException>() {
        @Override public void execute(TrackedTask task) {
          task.setResources(taskInfoMap.get(task.getTaskId()).getResources());
        }
      });

      // Remove records for tasks that the slave no longer reports.
      taskStore.remove(taskStore.fetch(new TaskQuery().setSlaveHost(update.getSlaveHost()),
          new Predicate<TrackedTask>() {
            @Override public boolean apply(TrackedTask task) {
              return !taskInfoMap.containsKey(task.getTaskId());
            }
          }));
    } catch (Throwable t) {
      LOG.log(Level.WARNING, "Uncaught exception.", t);
    }
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
      newTasks.add(new TrackedTask()
          .setTaskId(taskId)
          .setJobName(job.getName())
          .setOwner(job.getOwner())
          .setTask(task)
          .setStatus(ScheduleStatus.PENDING));
    }

    taskStore.add(newTasks);
    persist();
  }

  @Override
  public synchronized TaskDescription offer(final SlaveOffer slaveOffer)
      throws ScheduleException {
    final TwitterTaskInfo offer;
    try {
      offer = ConfigurationManager.makeConcrete(slaveOffer);
    } catch (ConfigurationManager.TaskDescriptionException e) {
      LOG.log(Level.SEVERE, "Invalid slave offer", e);
      return null;
    }

    TaskQuery query = new TaskQuery();
    query.addToStatuses(ScheduleStatus.PENDING);

    Predicate<TrackedTask> satisfiedFilter = new Predicate<TrackedTask>() {
      @Override public boolean apply(TrackedTask task) {
        return ConfigurationManager.satisfied(task.getTask(), offer);
      }
    };

    Iterable<TrackedTask> candidates = taskStore.fetch(query, satisfiedFilter);
    if (Iterables.isEmpty(candidates)) return null;

    TrackedTask task = Iterables.get(candidates, 0);
    task = taskStore.mutate(task,
        new ExceptionalClosure<TrackedTask, RuntimeException>() {
      @Override public void execute(TrackedTask mutable) {
        mutable.setStatus(ScheduleStatus.STARTING)
            .setSlaveId(slaveOffer.getSlaveId())
            .setSlaveHost(slaveOffer.getHost());
      }
    });

    if (task == null) {
      LOG.log(Level.SEVERE, "Unable to find matching mutable task!");
      return null;
    }

    // TODO(wfarner): Remove this hack once mesos core does not read parameters.
    StringMap params = new StringMap();
    params.set("cpus", String.valueOf((int) task.getTask().getNumCpus()));
    params.set("mem", String.valueOf(task.getTask().getRamBytes()));

    byte[] taskInBytes;
    try {
      taskInBytes = TASK_CODEC.encode(task.getTask());
    } catch (Codec.CodingException e) {
      LOG.log(Level.SEVERE, "Unable to serialize task.", e);
      throw new ScheduleException("Internal error.", e);
    }

    LOG.info(String.format("Offer on slave %s (id %s) is being assigned task for %s/%s.",
        slaveOffer.getHost(), task.slaveId, task.getOwner(), task.getJobName()));

    persist();
    return new TaskDescription(task.getTaskId(), slaveOffer.getSlaveId(),
            task.jobName + "-" + task.getTaskId(), params, taskInBytes);
  }

  private void scheduleTaskCopies(List<TrackedTask> tasks) {
    for (TrackedTask task : tasks) {
      task.setSlaveId(null)
          .setSlaveHost(null)
          .setStatus(ScheduleStatus.PENDING)
          .setTaskId(generateTaskId());
    }

    LOG.info("Tasks being rescheduled: " + tasks);

    taskStore.add(tasks);
  }

  @Override
  public synchronized void setTaskStatus(TaskQuery query, final ScheduleStatus status) {
    Preconditions.checkNotNull(query);
    Preconditions.checkNotNull(status);

    final List<TrackedTask> newTasks = Lists.newLinkedList();

    switch (status) {
      case PENDING:
      case STARTING:
      case RUNNING:
        // Simply assign the new state.
        taskStore.mutate(getTasks(query), new ExceptionalClosure<TrackedTask, RuntimeException>() {
          @Override public void execute(TrackedTask mutable) {
            mutable.setStatus(status);
          }
        });

        break;

      case FINISHED:
        // Assign the FINISHED state to non-daemon tasks, move daemon tasks to PENDING.
        taskStore.mutate(getTasks(query), new ExceptionalClosure<TrackedTask, RuntimeException>() {
          @Override public void execute(TrackedTask mutable) {
            if (mutable.getTask().isIsDaemon()) {
              LOG.info("Rescheduling daemon task " + mutable.getTaskId());
              newTasks.add(new TrackedTask(mutable));
            }

            mutable.setStatus(ScheduleStatus.FINISHED);
          }
        });

        break;

      case FAILED:
        // Increment failure count, move to pending state, unless failure limit has been reached.
        taskStore.mutate(getTasks(query), new ExceptionalClosure<TrackedTask, RuntimeException>() {
          @Override public void execute(TrackedTask mutable) {
            mutable.setFailureCount(mutable.getFailureCount() + 1);

            boolean failureLimitReached = (mutable.getTask().getMaxTaskFailures() != -1)
                && (mutable.getFailureCount() >= mutable.getTask().getMaxTaskFailures());

            if (!failureLimitReached) {
              LOG.info("Rescheduling failed task below failure limit: " + mutable.getTaskId());
              newTasks.add(new TrackedTask(mutable));
            }

            mutable.setStatus(ScheduleStatus.FAILED);
          }
        });

        break;

      case KILLED:
        // Move to killed state.
        taskStore.mutate(getTasks(query), new ExceptionalClosure<TrackedTask, RuntimeException>() {
          @Override public void execute(TrackedTask mutable) {
            mutable.setStatus(ScheduleStatus.KILLED);
          }
        });

        break;

      case LOST:
      case NOT_FOUND:
        // Move to pending state.
        taskStore.mutate(getTasks(query), new ExceptionalClosure<TrackedTask, RuntimeException>() {
          @Override public void execute(TrackedTask mutable) {
            LOG.info("Rescheduling " + status + " task: " + mutable.getTaskId());
            newTasks.add(new TrackedTask(mutable));
            mutable.setStatus(status);
          }
        });

        break;

      default:
        LOG.severe("Unknown schedule status " + status + " cannot be applied to query " + query);
    }

    if (newTasks.isEmpty()) return;
    scheduleTaskCopies(newTasks);
  }

  private static Predicate<TrackedTask> filter(final ScheduleStatus status) {
    return new Predicate<TrackedTask>() {
      @Override public boolean apply(TrackedTask task) {
        return task.getStatus() == status;
      }
    };
  }

  private static final Predicate<TrackedTask> DAEMON_TASKS = new Predicate<TrackedTask>() {
      @Override public boolean apply(TrackedTask task) {
        return task.getTask().isIsDaemon();
      }
    };

  @Override
  public synchronized void killTasks(final TaskQuery query) throws ScheduleException {
    Preconditions.checkNotNull(query);

    // KillTasks will not change state of terminated tasks.
    query.setStatuses(Sets.newHashSet(
        ScheduleStatus.PENDING, ScheduleStatus.STARTING, ScheduleStatus.RUNNING));

    LOG.info("Killing tasks matching " + query);

    Iterable<TrackedTask> toKill = getTasks(query);

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

    if (!matchingScheduler && Iterables.isEmpty(toKill)) {
      throw new ScheduleException("No tasks matching query found.");
    }

    List<TrackedTask> toRemove = Lists.newArrayList();
    for (final TrackedTask task : toKill) {
      if (task.getStatus() == ScheduleStatus.PENDING) {
        toRemove.add(task);
      } else if (task.getStatus() != ScheduleStatus.KILLED) {
        doWorkWithDriver(new Function<SchedulerDriver, Integer>() {
          @Override public Integer apply(SchedulerDriver driver) {
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
   * for any tasks that require use of the {@link SchedulerDriver}, for automatic retrying in the
   * event that the driver is not available.
   *
   * @param work The work to execute.  Should return the status code provided by the driver
   *    (0 denotes success, non-zero denotes a failure that should be retried).
   */
  private void doWorkWithDriver(final Function<SchedulerDriver, Integer> work) {
    workQueue.doWork(new Callable<Boolean>() {
      @Override public Boolean call() {
        if (frameworkId.get() == null) {
          LOG.info("Unable to send framework messages, framework not registered.");
          return false;
        }

        SchedulerDriver driver = schedulerDriver.get();
        return driver != null && work.apply(driver) == 0;
      }
    });
  }

  @Override
  public synchronized void restartTasks(final TaskQuery query) {
    Preconditions.checkNotNull(query);

    // TODO(wfarner): Need to do this in a cleaner way so that the entire job doesn't flip at once.
    LOG.info("Restarting tasks " + query);

    final SchedulingSignal signal = new SchedulingSignal().setSignal(SignalType.RESTART_TASK);

    for (final TrackedTask task : getTasks(query)) {
      if (task != null && task.status != ScheduleStatus.PENDING) {
        doWorkWithDriver(new Function<SchedulerDriver, Integer>() {
          @Override public Integer apply(SchedulerDriver driver) {
            try {
              return signalExecutor(driver, task.getSlaveId(), task.getTaskId(), signal);
            } catch (Codec.CodingException e) {
              LOG.log(Level.SEVERE, "Failed to encode scheduling signal.", e);
              return 0;
            }
          }
        });
      }
    }
  }

  private int signalExecutor(SchedulerDriver driver, String slaveId, int taskId,
      SchedulingSignal signal) throws Codec.CodingException {
    return driver.sendFrameworkMessage(new FrameworkMessage(slaveId, taskId,
        SIGNAL_CODEC.encode(signal)));
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

    // TODO(wfarner): Should communicate with executors here to fetch state information about all
    //    tasks and verify that slaves (and their tasks) all exist as expected in the recovered
    //    state.
  }
}
