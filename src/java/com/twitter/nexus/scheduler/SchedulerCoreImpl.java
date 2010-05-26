package com.twitter.nexus.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.nexus.gen.JobConfiguration;
import com.twitter.nexus.gen.ScheduleStatus;
import com.twitter.nexus.gen.SchedulerState;
import com.twitter.nexus.gen.TaskQuery;
import com.twitter.nexus.gen.TrackedTask;
import com.twitter.nexus.gen.TwitterTaskInfo;
import com.twitter.nexus.scheduler.configuration.ConfigurationManager;
import com.twitter.nexus.scheduler.persistence.PersistenceLayer;
import nexus.FrameworkMessage;
import nexus.SchedulerDriver;
import nexus.SlaveOffer;
import nexus.StringMap;
import nexus.TaskDescription;
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

  // Schedulers that are responsible for triggering execution of jobs.
  private final List<JobManager> jobManagers;

  // TODO(wfarner): Hopefully we can abolish (or at least mask) the concept of canonical task IDs
  // in favor of tasks being canonically named by job/taskIndex.
  private int nextTaskId = 0;

  // The nexus framework ID of the scheduler, set to -1 until the framework is registered.
  private final AtomicInteger frameworkId = new AtomicInteger(-1);

  // Stores the configured tasks.
  private TaskStore taskStore = new TaskStore();

  // Work queue that stores pending asynchronous tasks.
  @Inject
  private WorkQueue workQueue;

  @Inject
  private ZooKeeperClient zkClient;

  private final PersistenceLayer persistenceLayer;

  // Scheduler driver used for communication with other nodes in the cluster.
  private final AtomicReference<SchedulerDriver> schedulerDriver =
      new AtomicReference<SchedulerDriver>();

  @Inject
  public SchedulerCoreImpl(CronJobManager cronScheduler, ImmediateJobManager immediateScheduler,
      PersistenceLayer persistenceLayer) {
    Closure<JobConfiguration> jobRunner = new Closure<JobConfiguration>() {
      @Override
      public void execute(JobConfiguration job) {
        LOG.info("Running job: " + job);
        runJob(job);
      }
    };

    // The immediate scheduler will accept any job, so it's important that other schedulers are
    // placed first.
    jobManagers = Arrays.asList(cronScheduler, immediateScheduler);

    this.persistenceLayer = Preconditions.checkNotNull(persistenceLayer);

    // Attempt to recover from a persisted state.
    restore();
  }

  @Override
  public void registered(SchedulerDriver driver, int frameworkId) {
    this.schedulerDriver.set(Preconditions.checkNotNull(driver));
    this.frameworkId.set(frameworkId);
  }

  @Override
  public synchronized Iterable<TrackedTask> getTasks(final TaskQuery query,
      Predicate<TrackedTask>... filters) {
    return taskStore.fetch(query, filters);
  }

  @Override
  public synchronized boolean hasActiveJob(final String owner, final String jobName) {
    TaskQuery query = new TaskQuery().setOwner(owner).setJobName(jobName);
    for (ScheduleStatus status : ScheduleStatus.values()) {
      if (status != ScheduleStatus.KILLED) query.addToStatuses(status);
    }

    return !Iterables.isEmpty(getTasks(query)) || Iterables.any(jobManagers,
        new Predicate<JobManager>() {
          @Override public boolean apply(JobManager manager) {
            return manager.hasJob(owner, jobName);
          }
    });
  }

  private int generateTaskId() {
    return nextTaskId++;
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
  public synchronized nexus.TaskDescription offer(final SlaveOffer slaveOffer)
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

    // TODO(wfarner): Remove this hack once nexus core does not read parameters.
    StringMap params = new StringMap();
    params.set("cpus", String.valueOf((int) task.getTask().getNumCpus()));
    params.set("mem", String.valueOf(task.getTask().getRamBytes()));

    byte[] taskInBytes;
    try {
      taskInBytes = Codec.serialize(task.getTask());
    } catch (Codec.SerializationException e) {
      LOG.log(Level.SEVERE, "Unable to serialize task.", e);
      throw new ScheduleException("Internal error.", e);
    }

    LOG.info(String.format("Offer on slave %s (id %d) is being assigned task for %s/%s.",
        slaveOffer.getHost(), task.slaveId, task.getOwner(), task.getJobName()));

    persist();
    return new TaskDescription(task.getTaskId(), slaveOffer.getSlaveId(),
            task.jobName + "-" + task.getTaskId(), params, taskInBytes);
  }

  @Override
  public synchronized void setTaskStatus(TaskQuery query, final ScheduleStatus status) {
    Preconditions.checkNotNull(query);
    Preconditions.checkNotNull(status);

    // Assign status to tasks.
    taskStore.mutate(getTasks(query), new ExceptionalClosure<TrackedTask, RuntimeException>() {
      @Override public void execute(TrackedTask mutable) {
        mutable.setStatus(status);
      }
    });

    // Fetch all completed tasks.
    TaskQuery completedQuery = new TaskQuery(query);
    completedQuery.setStatuses(Sets.newHashSet(
        ScheduleStatus.FINISHED,
        ScheduleStatus.KILLED,
        ScheduleStatus.FAILED,
        ScheduleStatus.LOST,
        ScheduleStatus.NOT_FOUND));

    // TODO(wfarner): Add task failure tracking to TaskQuery and allow the configuration to
    // specify the number of per-task failures before giving up.
    Predicate<TrackedTask> daemonTaskFinder = new Predicate<TrackedTask>() {
      @Override public boolean apply(TrackedTask task) {
        return task.getTask().isIsDaemon();
      }
    };

    // Non-daemon tasks are finished and may be removed.
    taskStore.remove(getTasks(completedQuery, Predicates.not(daemonTaskFinder)));

    // Re-run the completed query, this time finding daemon tasks that should be re-run.
    taskStore.mutate(getTasks(completedQuery, daemonTaskFinder),
        new ExceptionalClosure<TrackedTask, RuntimeException>() {
      @Override public void execute(TrackedTask mutable) {
        LOG.info("Moving daemon task to PENDING state: "
                 + mutable.getOwner() + "/" + mutable.getJobName());
        mutable.setStatus(ScheduleStatus.PENDING);
        mutable.setSlaveIdIsSet(false);
        mutable.setSlaveId(-1);
      }
    });

    persist();
  }

  @Override
  public synchronized void killTasks(final TaskQuery query) throws ScheduleException {
    Preconditions.checkNotNull(query);
    LOG.info("Killing tasks matching " + query);

    Iterable<TrackedTask> toKill = getTasks(query);

    // If this looks like a query for all tasks in a job, instruct the scheduler modules to delete
    // the job.
    boolean matchingScheduler = false;
    if (!StringUtils.isEmpty(query.getOwner()) && !StringUtils.isEmpty(query.getJobName())
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
        if (frameworkId.get() == -1) {
          LOG.info("Unable to send framework messages, framework not registered.");
          return false;
        }

        // TODO(wfarner): What happens when this fails?  Do we have to reconnect manually, or does
        // the driver automatically try to reconnect when it sends a non-zero status code?
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

    for (final TrackedTask task : getTasks(query)) {
      if (task != null && task.status != ScheduleStatus.PENDING) {
        // TODO(wfarner): Once scheduler -> executorHub communication is defined, replace the
        // empty byte array with a serialized message.
        doWorkWithDriver(new Function<SchedulerDriver, Integer>() {
          @Override public Integer apply(SchedulerDriver driver) {
            return driver.sendFrameworkMessage(
                new FrameworkMessage(frameworkId.get(), task.slaveId, new byte[0]));
          }
        });
      }
    }
  }

  private void persist() {
    SchedulerState state = new SchedulerState();
    state.setConfiguredTasks(Lists.newArrayList(taskStore.fetch(new TaskQuery())));
    Map<String, List<JobConfiguration>> moduleState = Maps.newHashMap();
    for (JobManager manager : jobManagers) {
      moduleState.put(manager.getClass().getCanonicalName(),
          Lists.newArrayList(manager.getState()));
    }
    state.setModuleJobs(moduleState);

    try {
      persistenceLayer.commit(Codec.serialize(state));
    } catch (Codec.SerializationException e) {
      LOG.log(Level.SEVERE, "Failed to serialize scheduler state, unable to persist!", e);
    } catch (PersistenceLayer.PersistenceException e) {
      LOG.log(Level.SEVERE, "Failed to persist scheduler state.", e);
    }
  }

  private void restore() {
    SchedulerState state = new SchedulerState();
    try {
      byte[] data = persistenceLayer.fetch();
      if (data == null) {
        LOG.info("No persisted state found for restoration.");
        return;
      }

      Codec.deserialize(state, data);
    } catch (PersistenceLayer.PersistenceException e) {
      LOG.log(Level.SEVERE, "Failed to fetch persisted state.", e);
      return;
    } catch (Codec.SerializationException e) {
      LOG.log(Level.SEVERE, "Failed to deserialize persisted state.", e);
      return;
    }

    // Clear fields associated with unknown state.
    List<TrackedTask> restoredTasks = state.getConfiguredTasks();
    for (TrackedTask task : restoredTasks) {
      task.setSlaveId(-1).setSlaveHost(null);
      switch (task.getStatus()) {
        case RUNNING:
        case STARTING:
          task.setStatus(ScheduleStatus.PENDING);
      }
    }
    taskStore.add(restoredTasks);

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
