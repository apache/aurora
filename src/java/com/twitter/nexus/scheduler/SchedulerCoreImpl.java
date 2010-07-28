package com.twitter.nexus.scheduler;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.nexus.gen.ExecutorQuery;
import com.twitter.nexus.gen.ExecutorQueryResponse;
import com.twitter.nexus.gen.JobConfiguration;
import com.twitter.nexus.gen.ResourceConsumption;
import com.twitter.nexus.gen.ResponseCode;
import com.twitter.nexus.gen.ScheduleStatus;
import com.twitter.nexus.gen.SchedulerState;
import com.twitter.nexus.gen.SchedulingSignal;
import com.twitter.nexus.gen.SignalType;
import com.twitter.nexus.gen.TaskQuery;
import com.twitter.nexus.gen.TrackedTask;
import com.twitter.nexus.gen.TwitterTaskInfo;
import com.twitter.nexus.scheduler.configuration.ConfigurationManager;
import com.twitter.nexus.scheduler.persistence.Codec;
import com.twitter.nexus.scheduler.persistence.PersistenceLayer;
import com.twitter.nexus.scheduler.persistence.ThriftBinaryCodec;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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

  // The nexus framework ID of the scheduler, set to null until the framework is registered.
  private final AtomicReference<String> frameworkId = new AtomicReference<String>(null);

  // Stores the configured tasks.
  private TaskStore taskStore = new TaskStore();

  private final Codec<ExecutorQuery, byte[]> queryCodec = new ThriftBinaryCodec<ExecutorQuery>(
      ExecutorQuery.class);

  // Work queue that stores pending asynchronous tasks.
  @Inject private WorkQueue workQueue;

  private final PersistenceLayer<SchedulerState> persistenceLayer;

  // Scheduler driver used for communication with other nodes in the cluster.
  private final AtomicReference<SchedulerDriver> schedulerDriver =
      new AtomicReference<SchedulerDriver>();

  @Inject
  public SchedulerCoreImpl(CronJobManager cronScheduler, ImmediateJobManager immediateScheduler,
      PersistenceLayer<SchedulerState> persistenceLayer) {
    // The immediate scheduler will accept any job, so it's important that other schedulers are
    // placed first.
    jobManagers = Arrays.asList(cronScheduler, immediateScheduler);
    this.persistenceLayer = Preconditions.checkNotNull(persistenceLayer);

    restore();
    scheduleResourceInfoFetch();
  }

  @Override
  public void registered(SchedulerDriver driver, String frameworkId) {
    this.schedulerDriver.set(Preconditions.checkNotNull(driver));
    this.frameworkId.set(Preconditions.checkNotNull(frameworkId));
    persist();
    scheduleResourceInfoFetch();
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
    return nextTaskId.incrementAndGet();
  }

  private void scheduleResourceInfoFetch() {
    ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ExecutorQuery-%d").build());

    Runnable fetcher = new Runnable() {
      @Override public void run() {
        final ArrayListMultimap<String, Integer> slaveIdToTaskIds = ArrayListMultimap.create();

        TaskQuery query = new TaskQuery();
        query.addToStatuses(ScheduleStatus.RUNNING);

        for (TrackedTask task : taskStore.fetch(query)) {
          slaveIdToTaskIds.put(task.getSlaveId(), task.getTaskId());
        }

        if (slaveIdToTaskIds.isEmpty()) return;

        doWorkWithDriver(new Function<SchedulerDriver, Integer>() {
          @Override public Integer apply(SchedulerDriver driver) {
            for (String slaveId : slaveIdToTaskIds.keySet()) {
              FrameworkMessage message = new FrameworkMessage();
              message.setSlaveId(slaveId);
              try {
                message.setData(queryCodec.encode(
                    new ExecutorQuery().setTaskIds(slaveIdToTaskIds.get(slaveId))));
              } catch (Codec.CodingException e) {
                LOG.log(Level.SEVERE, "Failed to encode executor query.", e);
                return -1;
              }

              int result = driver.sendFrameworkMessage(message);
              if (result != 0) return result;
            }

            return 0;
          }
        });
      }
    };

    // TODO(wfarner): Make configurable.
    executor.scheduleAtFixedRate(fetcher, 10, 2, TimeUnit.SECONDS);
  }

  @Override
  public void executorQueryResponse(ExecutorQueryResponse response) {
    Preconditions.checkNotNull(response);

    if (response.getResponseCode() != ResponseCode.OK) {
      LOG.info("Executor query failed: " + response.getMessage());
      return;
    }

    final Map<Integer, ResourceConsumption> resources = response.getTaskResources();
    if (resources.isEmpty()) return;

    Iterable<TrackedTask> tasks = taskStore.fetch(new TaskQuery().setTaskIds(resources.keySet()));
    taskStore.mutate(tasks, new ExceptionalClosure<TrackedTask, RuntimeException>() {
      @Override public void execute(TrackedTask task) {
        task.setResources(resources.get(task.getTaskId()));
      }
    });
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

    // Tasks that were killed should be removed.
    taskStore.remove(getTasks(completedQuery, filter(ScheduleStatus.KILLED)));

    // Increment the failure count on all failed tasks.
    taskStore.mutate(getTasks(completedQuery, filter(ScheduleStatus.FAILED)),
        new ExceptionalClosure<TrackedTask, RuntimeException>() {
          @Override public void execute(TrackedTask mutable) throws RuntimeException {
            mutable.setFailureCount(mutable.getFailureCount() + 1);
          }
        });

    // Remove all tasks that have exceeded the failure limit.
    // TODO(wfarner): The semantics here may need to change - it might be necessary to count
    //    all failures in a job, and kill the associated job.
    taskStore.remove(getTasks(completedQuery, new Predicate<TrackedTask>() {
      @Override public boolean apply(TrackedTask task) {
        boolean remove = (task.getTask().getMaxTaskFailures() != -1)
            && (task.getFailureCount() >= task.getTask().getMaxTaskFailures());
        if (remove) LOG.info("Task exceeded max allowed failures: " + task);

        return remove;
      }
    }));

    // Non-daemon tasks are finished and may be removed.
    taskStore.remove(getTasks(completedQuery, Predicates.not(DAEMON_TASKS),
        filter(ScheduleStatus.FINISHED)));

    // All remaining tasks should be rescheduled.
    taskStore.mutate(getTasks(completedQuery),
        new ExceptionalClosure<TrackedTask, RuntimeException>() {
          @Override public void execute(TrackedTask mutable) {
            LOG.info("Moving task back to PENDING state: " + mutable.getOwner() + "/"
                     + mutable.getJobName());
            mutable.setStatus(ScheduleStatus.PENDING);
            mutable.setSlaveIdIsSet(false);
            mutable.setSlaveId(null);
          }
        });

    persist();
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
