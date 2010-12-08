package com.twitter.mesos.scheduler;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.Message;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ExecutorMessage;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.LiveTaskInfo;
import com.twitter.mesos.gen.NonVolatileSchedulerState;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.ResourceConsumption;
import com.twitter.mesos.gen.RestartExecutor;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.JobManager.JobUpdateResult;
import com.twitter.mesos.scheduler.TaskStore.TaskState;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import com.twitter.mesos.scheduler.persistence.PersistenceLayer;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.twitter.mesos.gen.ScheduleStatus.*;
import static com.twitter.mesos.scheduler.JobManager.JobUpdateResult.*;

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

  // Filter to determine whether a task should be scheduled.
  private final SchedulingFilter schedulingFilter;

  // Handles job updates that require a restart.
  private final JobUpdateLauncher jobUpdater;

  private final PersistenceLayer<NonVolatileSchedulerState> persistenceLayer;
  private final ExecutorTracker executorTracker;
  // Scheduler driver used for communication with other nodes in the cluster.
  private final AtomicReference<Driver> schedulerDriver = new AtomicReference<Driver>();

  @Inject
  public SchedulerCoreImpl(CronJobManager cronScheduler, ImmediateJobManager immediateScheduler,
      PersistenceLayer<NonVolatileSchedulerState> persistenceLayer,
      ExecutorTracker executorTracker, WorkQueue workQueue, SchedulingFilter schedulingFilter,
      JobUpdateLauncher jobUpdater) {
    // The immediate scheduler will accept any job, so it's important that other schedulers are
    // placed first.
    jobManagers = Arrays.asList(cronScheduler, immediateScheduler);
    this.persistenceLayer = Preconditions.checkNotNull(persistenceLayer);
    this.executorTracker = Preconditions.checkNotNull(executorTracker);
    this.workQueue = Preconditions.checkNotNull(workQueue);
    this.schedulingFilter = Preconditions.checkNotNull(schedulingFilter);
    this.jobUpdater = Preconditions.checkNotNull(jobUpdater);

    restore();
  }

  @Override
  public void registered(Driver driver, String frameworkId) {
    this.schedulerDriver.set(Preconditions.checkNotNull(driver));
    this.frameworkId.set(Preconditions.checkNotNull(frameworkId));
    persist();

    executorTracker.start(new Closure<String>() {
      @Override public void execute(String slaveId) {
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
  public synchronized Set<TaskState> getTasks(Query query) {
    return taskStore.fetch(query);
  }

  private boolean hasActiveJob(JobConfiguration job) {
    return Iterables.any(jobManagers, managerHasJob(job));
  }

  private boolean hasRunningTasks(JobConfiguration job) {
    return !getTasks(Query.activeQuery(job)).isEmpty();
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

    final AtomicBoolean mutated = new AtomicBoolean(false);

    final Map<Integer, LiveTaskInfo> taskInfoMap = Maps.newHashMap();

    for (LiveTaskInfo taskInfo : update.getTaskInfos()) {
      taskInfoMap.put(taskInfo.getTaskId(), taskInfo);
    }

    // TODO(wfarner): Have the scheduler only retain configurations for live jobs,
    //    and acquire all other state from slaves.
    //    This will allow the scheduler to only persist active tasks.

    // Look for any tasks that we don't know about, or this slave should not be modifying.
    Query tasksForHost = new Query(new TaskQuery().setTaskIds(taskInfoMap.keySet())
        .setSlaveHost(update.getSlaveHost()));
    final Set<Integer> recognizedTasks = taskStore.fetchIds(tasksForHost);
    Set<Integer> unknownTasks = ImmutableSet.copyOf(
        Sets.difference(taskInfoMap.keySet(), recognizedTasks));
    if (!unknownTasks.isEmpty()) {
      LOG.severe("Received task info update from executor " + update.getSlaveHost()
                 + " for tasks unknown or belonging to a different host: " + unknownTasks);
    }

    // Remove unknown tasks from the request to prevent badness later.
    taskInfoMap.keySet().removeAll(unknownTasks);

    // Update the resource information for the tasks that we currently have on record.
    taskStore.mutate(Query.byId(recognizedTasks),
        new Closure<TaskState>() {
          @Override public void execute(TaskState task) throws RuntimeException {
            LiveTaskInfo taskUpdate = taskInfoMap.get(Tasks.id(task));
            if (taskUpdate.getResources() != null) {
              task.volatileState.resources = new ResourceConsumption(taskUpdate.getResources());
            }
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
    Set<Integer> deadTasks = taskStore.fetchIds(
        new Query(new TaskQuery().setTaskIds(reportedDeadTasks).setStatuses(EnumSet.of(RUNNING))));
    if (!deadTasks.isEmpty()) {
      LOG.info("Found tasks that were recorded as RUNNING but slave " + update.getSlaveHost()
               + " reports as KILLED: " + deadTasks);
      mutated.set(true);
      setTaskStatus(Query.byId(deadTasks), KILLED);
    }

    // Find any tasks assigned to this slave but the slave does not report.
    Predicate<TaskState> isTaskReported = new Predicate<TaskState>() {
      @Override public boolean apply(TaskState state) {
        return recognizedTasks.contains(Tasks.id(state.task));
      }
    };
    Predicate<TaskState> lastEventBeyondGracePeriod = new Predicate<TaskState>() {
      @Override public boolean apply(TaskState state) {
        long taskAgeMillis = System.currentTimeMillis()
            - Iterables.getLast(state.task.getTaskEvents()).getTimestamp();
        return taskAgeMillis > Amount.of(10, Time.MINUTES).as(Time.MILLISECONDS);
      }
    };

    TaskQuery slaveAssignedTaskQuery = new TaskQuery().setSlaveHost(update.getSlaveHost());
    Set<Integer> missingNotRunningTasks = taskStore.fetchIds(new Query(slaveAssignedTaskQuery,
            Predicates.not(isTaskReported), Predicates.not(Tasks.hasStatus(RUNNING)),
            lastEventBeyondGracePeriod));
    if (!missingNotRunningTasks.isEmpty()) {
      LOG.info("Removing non-running tasks no longer reported by slave " + update.getSlaveHost()
               + ": " + missingNotRunningTasks);
      mutated.set(true);
      taskStore.remove(missingNotRunningTasks);
    }

    Set<Integer> missingRunningTasks = taskStore.fetchIds(new Query(slaveAssignedTaskQuery,
            Predicates.not(isTaskReported), Tasks.hasStatus(RUNNING)));
    if (!missingRunningTasks.isEmpty()) {
      LOG.info("Slave " + update.getSlaveHost() + " no longer reports running tasks: "
               + missingRunningTasks + ", reporting as LOST.");
      mutated.set(true);
      setTaskStatus(Query.byId(missingRunningTasks), LOST);
    }

    if (mutated.get()) persist();
  }

  @Override
  public synchronized void createJob(JobConfiguration job) throws ScheduleException,
      ConfigurationManager.TaskDescriptionException {
    JobConfiguration populated = populateAndVerify(job);

    if (hasActiveJob(populated)) {
      throw new ScheduleException("Job already exists: " + Tasks.jobKey(populated));
    }

    boolean accepted = false;
    for (JobManager manager : jobManagers) {
      if (manager.receiveJob(populated)) {
        accepted = true;
        LOG.info("Job accepted by manager: " + manager.getUniqueKey());
        persist();
        break;
      }
    }

    if (!accepted) {
      LOG.severe("Job was not accepted by any of the configured schedulers, discarding.");
      LOG.severe("Discarded job: " + populated);
      throw new ScheduleException("Job not accepted, discarding.");
    }
  }

  @Override
  public synchronized void runJob(JobConfiguration job) {
    Preconditions.checkState(!hasRunningTasks(job));

    launchTasks(Preconditions.checkNotNull(job.getTaskConfigs()));
  }

  private Function<TwitterTaskInfo, ScheduledTask> taskCreator =
      new Function<TwitterTaskInfo, ScheduledTask>() {
        @Override
        public ScheduledTask apply(TwitterTaskInfo task) {
          AssignedTask assigned = new AssignedTask()
              .setTaskId(generateTaskId())
              .setTask(task);
          return changeTaskStatus(new ScheduledTask().setAssignedTask(assigned), PENDING);
        }
      };

  private void launchTasks(Set<TwitterTaskInfo> tasks) {
    if (tasks.isEmpty()) return;

    LOG.info("Launching " + tasks.size() + " tasks.");
    taskStore.add(ImmutableSet.copyOf(Iterables.transform(tasks, taskCreator)));
    persist();
  }

  @Override
  public JobUpdateResult doJobUpdate(JobConfiguration job) throws ScheduleException {
    LOG.info("Updating job " + Tasks.jobKey(job));

    // First, get comparable views of the start and end states.  Mapped by shard IDs.
    final Map<Integer, AssignedTask> existingTasks = Maps.uniqueIndex(Iterables.transform(
        taskStore.fetch(Query.activeQuery(job)), Tasks.STATE_TO_ASSIGNED),
        Tasks.ASSIGNED_TO_SHARD_ID);
    final Map<Integer, TwitterTaskInfo> updatedTasks = Maps.uniqueIndex(job.getTaskConfigs(),
        Tasks.INFO_TO_SHARD_ID);

    // Tasks that are unchanged.
    Set<TwitterTaskInfo> unmodifiedTasks = Sets.newHashSet();

    // Tasks that we can silently modify, and the scheduler will automatically compensate for.
    Set<Integer> shardIdsMutatedInPlace = Sets.newHashSet();

    // Tasks that we have to restart in order for the change to take effect.
    Set<TwitterTaskInfo> tasksRequiringRestart = Sets.newHashSet();

    // Inspect each task and decide how to ahndle it.
    for (Map.Entry<Integer, TwitterTaskInfo> updatedTask : updatedTasks.entrySet()) {
      int shardId = updatedTask.getKey();

      // New task - handled by shardIdsAdded.
      if (!existingTasks.containsKey(shardId)) continue;

      TwitterTaskInfo updated = updatedTask.getValue();
      AssignedTask existing = existingTasks.get(shardId);

      if (updated.equals(existing.getTask())) {
        unmodifiedTasks.add(updated);
        continue;
      }

      // If we assign the existing values for all fields that (if changed) do not require a restart
      // to the updated task, we can determine whether a restart is required.
      TwitterTaskInfo existingInfo = existing.getTask();
      boolean restartRequired = !new TwitterTaskInfo(updated)
          .setConfiguration(existingInfo.getConfiguration())
          .setIsDaemon(existingInfo.isIsDaemon())
          .setPriority(existingInfo.getPriority())
          .setMaxTaskFailures(existingInfo.getMaxTaskFailures())
          .equals(existing.getTask());

      if (restartRequired) {
        tasksRequiringRestart.add(updated);
      } else {
        shardIdsMutatedInPlace.add(shardId);
      }
    }

    // Calculate the set-complements.
    Set<Integer> shardIdsRemoved = Sets.difference(existingTasks.keySet(), updatedTasks.keySet());
    Set<Integer> shardIdsAdded = Sets.difference(updatedTasks.keySet(), existingTasks.keySet());

    if (shardIdsAdded.isEmpty()
        && shardIdsRemoved.isEmpty()
        && shardIdsMutatedInPlace.isEmpty()
        && tasksRequiringRestart.isEmpty()) {
      return JOB_UNCHANGED;
    }

    Preconditions.checkState((unmodifiedTasks.size()
                             + shardIdsAdded.size()
                             + shardIdsMutatedInPlace.size()
                             + tasksRequiringRestart.size()) == updatedTasks.size(),
        "Consistency check failed - incorrect number of modified tasks after update.");

    if (!tasksRequiringRestart.isEmpty()) {
      jobUpdater.launchUpdater(job);
      return UPDATER_LAUNCHED;
    }

    // First launch any new tasks.  Gets all updatedTasks whose shard ID was added.
    launchTasks(Sets.newHashSet(Iterables.filter(updatedTasks.values(),
        Predicates.compose(Predicates.in(shardIdsAdded), Tasks.INFO_TO_SHARD_ID))));

    Function<Integer, Integer> shardToExistingTaskId = new Function<Integer, Integer>() {
      @Override public Integer apply(Integer shardId) {
        return existingTasks.get(shardId).getTaskId();
      }
    };

    // Then kill any removed tasks.
    if (!shardIdsRemoved.isEmpty()) {
      killTasks(Query.byId(Iterables.transform(shardIdsRemoved, shardToExistingTaskId)));
    }

    // Perform any in-place mutations.
    if (!shardIdsMutatedInPlace.isEmpty()) {
      Iterable<Integer> taskIds =
          Iterables.transform(shardIdsMutatedInPlace, shardToExistingTaskId);
      taskStore.mutate(Query.byId(taskIds),
          new Closure<TaskState>() {
            @Override public void execute(TaskState state) {
              int shardId = state.task.getAssignedTask().getTask().getShardId();
              state.task.getAssignedTask().setTask(updatedTasks.get(shardId));
            }
          });
    }

    return COMPLETED;
  }

  /**
   * Creates a predicate that will determine whether a job manager has a job matching a job key.
   *
   * @param job Job to match.
   * @return A new predicate matching the job owner and name given.
   */
  private static Predicate<JobManager> managerHasJob(final JobConfiguration job) {
    return new Predicate<JobManager>() {
      @Override public boolean apply(JobManager manager) {
        return manager.hasJob(Tasks.jobKey(job));
      }
    };
  }

  private Closure<TaskState> taskLauncher(final String slaveId, final String slaveHost) {
    return new Closure<TaskState>() {
      @Override public void execute(TaskState state) {
        state.task.getAssignedTask().setSlaveId(slaveId).setSlaveHost(slaveHost);
        changeTaskStatus(state.task, STARTING);
      }
    };
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

    SortedSet<TaskState> candidates = taskStore.fetch(Query.byStatus(PENDING),
        schedulingFilter.makeFilter(offer, slaveHost));

    if (candidates.isEmpty()) return null;

    LOG.info("Found " + candidates.size() + " candidates for offer.");

    // Choose the first (top) candidate and launch it.
    ScheduledTask task = Iterables.getOnlyElement(taskStore.mutate(
        Query.byId(Tasks.id(Iterables.get(candidates, 0))), taskLauncher(slaveId, slaveHost))).task;

    // TODO(wfarner): Remove this hack once mesos core does not read parameters.
    Map<String, String> params = ImmutableMap.of(
      "cpus", String.valueOf((int) task.getAssignedTask().getTask().getNumCpus()),
      "mem", String.valueOf(task.getAssignedTask().getTask().getRamMb())
    );

    AssignedTask assignedTask = task.getAssignedTask();
    LOG.info(String.format("Offer on slave %s (id %s) is being assigned task for %s.",
        slaveHost, assignedTask.getSlaveId(), Tasks.jobKey(assignedTask)));

    persist();
    return new TwitterTask(assignedTask.getTaskId(), slaveId,
        assignedTask.getTask().getJobName() + "-" + assignedTask.getTaskId(), params,
        assignedTask);
  }

  /**
   * Schedules {@code tasks}, which are expected to be copies of existing tasks.  The tasks provided
   * will be modified.
   *
   * @param tasks Copies of other tasks, to be scheduled.
   */
  private void scheduleTaskCopies(List<ScheduledTask> tasks) {
    for (ScheduledTask task : tasks) {
      // The shard ID in the assigned task is left unchanged.
      task.getAssignedTask().unsetSlaveId();
      task.getAssignedTask().unsetSlaveHost();
      task.unsetTaskEvents();
      task.setAncestorId(Tasks.id(task));
      task.getAssignedTask().setTaskId(generateTaskId());
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
  private ScheduledTask changeTaskStatus(ScheduledTask task, ScheduleStatus status) {
    task.setStatus(status);
    task.addToTaskEvents(new TaskEvent()
        .setTimestamp(System.currentTimeMillis())
        .setStatus(status));
    return task;
  }

  @Override
  public synchronized void setTaskStatus(Query rawQuery, final ScheduleStatus status) {
    Preconditions.checkNotNull(rawQuery);
    Preconditions.checkNotNull(status);

    // Only allow state transition from non-terminal state.
    Query query = Query.and(rawQuery, Predicates.not(Tasks.TERMINATED_FILTER));

    final List<ScheduledTask> newTasks = Lists.newLinkedList();

    Closure<TaskState> mutateOperation;

    switch (status) {
      case PENDING:
      case STARTING:
      case RUNNING:
        // Simply assign the new state.
        mutateOperation = new Closure<TaskState>() {
          @Override public void execute(TaskState state) {
            changeTaskStatus(state.task, status);
          }
        };

        break;

      case FINISHED:
        // Assign the FINISHED state to non-daemon tasks, move daemon tasks to PENDING.
        mutateOperation = new Closure<TaskState>() {
          @Override public void execute(TaskState state) {
            if (state.task.getAssignedTask().getTask().isIsDaemon()) {
              LOG.info("Rescheduling daemon task " + Tasks.id(state.task));
              newTasks.add(new ScheduledTask(state.task));
            }

            changeTaskStatus(state.task, FINISHED);
          }
        };

        break;

      case FAILED:
        // Increment failure count, move to pending state, unless failure limit has been reached.
        mutateOperation = new Closure<TaskState>() {
          @Override public void execute(TaskState state) {
            state.task.setFailureCount(state.task.getFailureCount() + 1);

            boolean failureLimitReached =
                (state.task.getAssignedTask().getTask().getMaxTaskFailures() != -1)
                && (state.task.getFailureCount()
                    >= state.task.getAssignedTask().getTask().getMaxTaskFailures());

            if (!failureLimitReached) {
              LOG.info("Rescheduling failed task below failure limit: " + Tasks.id(state.task));
              newTasks.add(new ScheduledTask(state.task));
            }

            changeTaskStatus(state.task, FAILED);
          }
        };

        break;

      case KILLED: // This can happen if the executor dies, or the task process itself is killed.
      case LOST:
      case NOT_FOUND:
        // Move to pending state.
        mutateOperation = new Closure<TaskState>() {
          @Override public void execute(TaskState state) {
            LOG.info("Rescheduling " + status + " task: " + Tasks.id(state.task));
            newTasks.add(new ScheduledTask(state.task));
            changeTaskStatus(state.task, status);
          }
        };

        break;

      default:
        LOG.severe("Unknown schedule status " + status + " cannot be applied to query " + query);
        return;
    }

    taskStore.mutate(query, mutateOperation);

    if (newTasks.isEmpty()) return;
    scheduleTaskCopies(newTasks);
    persist();
  }

  @Override
  public synchronized void killTasks(final Query query) throws ScheduleException {
    Preconditions.checkNotNull(query);

    LOG.info("Killing tasks matching " + query);

    // If this looks like a query for all tasks in a job, instruct the scheduler modules to delete
    // the job.
    boolean matchingScheduler = false;

    if (query.specifiesJobOnly()) {
      String jobKey = query.getJobKey();

      for (JobManager manager : jobManagers) {
        if (manager.deleteJob(jobKey)) matchingScheduler = true;
      }
    }

    // Don't change the state of terminated tasks.
    if (!matchingScheduler && taskStore.fetch(Query.and(query, Tasks.ACTIVE_FILTER)).isEmpty()) {
      throw new ScheduleException("No tasks matching query found.");
    }

    // First remove any pending tasks matching the query.
    taskStore.remove(Query.and(query, Tasks.hasStatus(PENDING)));

    Closure<TaskState> mutate = new Closure<TaskState>() {
      @Override public void execute(final TaskState state) {
        changeTaskStatus(state.task, KILLED_BY_CLIENT);

        final int idToKill = Tasks.id(state.task);
        doWorkWithDriver(new Function<Driver, Integer>() {
          @Override public Integer apply(Driver driver) {
            return driver.killTask(idToKill);
          }
        });
      }
    };

    taskStore.mutate(Query.and(query, Predicates.not(Tasks.hasStatus(PENDING))), mutate);

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
          LOG.severe("Unable to send framework messages, framework not registered.");
          return false;
        }

        Driver driver = schedulerDriver.get();

        if (driver == null) {
          LOG.warning("Driver requested but not available.");
          return false;
        } else {
          return work.apply(driver) == 0;
        }
      }
    });
  }

  @Override
  public synchronized Set<Integer> restartTasks(Set<Integer> taskIds) {
    MorePreconditions.checkNotBlank(taskIds);
    LOG.info("Restart requested for tasks " + taskIds);

    Query byId = Query.byId(taskIds);
    Set<TaskState> tasks = taskStore.fetch(byId);
    if (tasks.size() != taskIds.size()) {
      Set<Integer> unknownTasks = Sets.difference(taskIds,
          Sets.newHashSet(Iterables.transform(tasks, Tasks.STATE_TO_ID)));

      LOG.warning("Restart requested for unknown tasks " + unknownTasks);
    }

    Iterable<TaskState> inactiveTasks = Iterables.filter(tasks,
        Predicates.not(Tasks.ACTIVE_FILTER));
    if (!Iterables.isEmpty(inactiveTasks)) {
      LOG.warning("Restart request ignored for inactive tasks "
                  + Iterables.transform(inactiveTasks, Tasks.STATE_TO_ID));
    }

    Query activeQuery = Query.and(byId, Tasks.ACTIVE_FILTER);
    Set<Integer> activeTaskIds = taskStore.fetchIds(activeQuery);
    taskStore.mutate(activeQuery, new Closure<TaskState>() {
      @Override public void execute(final TaskState state) {
        ScheduleStatus originalStatus = state.task.getStatus();
        changeTaskStatus(state.task, KILLED_BY_CLIENT);
        scheduleTaskCopies(Arrays.asList(new ScheduledTask(state.task)));

        if (originalStatus != PENDING) {
          final int killTaskId = Tasks.id(state.task);
          doWorkWithDriver(new Function<Driver, Integer>() {
            @Override public Integer apply(Driver driver) {
              return driver.killTask(killTaskId);
            }
          });
        }
      }
    });

    persist();

    return activeTaskIds;
  }

  @Override
  public synchronized JobUpdateResult updateJob(JobConfiguration updatedJob)
      throws ScheduleException, TaskDescriptionException {
    JobConfiguration populated = populateAndVerify(updatedJob);

    try {
      return Iterables.find(jobManagers, managerHasJob(populated)).updateJob(populated);
    } catch (NoSuchElementException e) {
      throw new ScheduleException("Job not found: " + Tasks.jobKey(populated));
    }
  }

  private void persist() {
    LOG.info("Saving scheduler state.");
    NonVolatileSchedulerState state = new NonVolatileSchedulerState();
    state.setFrameworkId(frameworkId.get());
    state.setNextTaskId(nextTaskId.get());
    state.setTasks(Lists.newArrayList(
        Iterables.transform(taskStore.fetch(Query.GET_ALL), Tasks.STATE_TO_SCHEDULED)));
    Map<String, List<JobConfiguration>> moduleState = Maps.newHashMap();
    for (JobManager manager : jobManagers) {
      moduleState.put(manager.getUniqueKey(), Lists.newArrayList(manager.getJobs()));
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

    NonVolatileSchedulerState state;
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

    taskStore.add(state.getTasks());

    for (final Map.Entry<String, List<JobConfiguration>> entry : state.getModuleJobs().entrySet()) {
      JobManager manager = Iterables.find(jobManagers, new Predicate<JobManager>() {
        @Override public boolean apply(JobManager manager) {
          return manager.getUniqueKey().equals(entry.getKey());
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

   private JobConfiguration populateAndVerify(JobConfiguration job)
       throws TaskDescriptionException {
     JobConfiguration copy = new JobConfiguration(job);

     Set<Integer> shardIds = Sets.newHashSet();

     if (copy.getTaskConfigsSize() == 0) throw new TaskDescriptionException("No tasks specified.");

     List<TwitterTaskInfo> configsCopy = Lists.newArrayList(copy.getTaskConfigs());
     for (TwitterTaskInfo config : configsCopy) {
       if (!config.isSetShardId()) {
         throw new TaskDescriptionException("Tasks must have a shard ID.");
       }

       if (!shardIds.add(config.getShardId())) {
         throw new TaskDescriptionException("Duplicate shard ID " + config.getShardId());
       }

       ConfigurationManager.populateFields(copy, config);
     }

     // The configs were mutated, so we need to refresh the Set.
     copy.setTaskConfigs(Sets.newHashSet(configsCopy));

     for (int i = 0; i < copy.getTaskConfigsSize(); i++) {
       if (!shardIds.contains(i)) {
         throw new TaskDescriptionException("Shard ID " + i + " is missing.");
       }
     }

     return copy;
  }
}
