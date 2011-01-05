package com.twitter.mesos.scheduler;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.RequestStats;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.LiveTaskInfo;
import com.twitter.mesos.gen.NonVolatileSchedulerState;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.ResourceConsumption;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateConfig;
import com.twitter.mesos.gen.UpdateConfigResponse;
import com.twitter.mesos.scheduler.JobManager.JobUpdateResult;
import com.twitter.mesos.scheduler.TaskStore.TaskState;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import com.twitter.mesos.scheduler.persistence.PersistenceLayer;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.Tasks.INFO_TO_SHARD_ID;
import static com.twitter.mesos.Tasks.jobKey;
import static com.twitter.mesos.gen.ScheduleStatus.*;
import static com.twitter.mesos.scheduler.JobManager.JobUpdateResult.*;

/**
 * Implementation of the scheduler core.
 *
 * @author wfarner
 */
public class SchedulerCoreImpl implements SchedulerCore, UpdateScheduler {
  private static final Logger LOG = Logger.getLogger(SchedulerCore.class.getName());

  // Schedulers that are responsible for triggering execution of jobs.
  private final List<JobManager> jobManagers;

  // The mesos framework ID of the scheduler, set to null until the framework is registered.
  private final AtomicReference<String> frameworkId = new AtomicReference<String>(null);

  // Stores the configured tasks.
  private final TaskStore taskStore = new TaskStore();

  // Handles communication with the rest of the mesos cluster.
  private final Driver driver;

  // Filter to determine whether a task should be scheduled.
  private final SchedulingFilter schedulingFilter;

  // Tracks updates that are in-progress, mapping from update token to update spec.
  @VisibleForTesting final Map<String, JobUpdate> updatesInProgress = Maps.newHashMap();

  private final PersistenceLayer<NonVolatileSchedulerState> persistenceLayer;

  private final Function<String, TwitterTaskInfo> updaterTaskBuilder;

  @Inject
  public SchedulerCoreImpl(CronJobManager cronScheduler,
      ImmediateJobManager immediateScheduler,
      PersistenceLayer<NonVolatileSchedulerState> persistenceLayer,
      Driver driver,
      SchedulingFilter schedulingFilter,
      Function<String, TwitterTaskInfo> updaterTaskBuilder) {
    // The immediate scheduler will accept any job, so it's important that other schedulers are
    // placed first.
    this.jobManagers = Arrays.asList(checkNotNull(cronScheduler), checkNotNull(immediateScheduler));
    this.persistenceLayer = checkNotNull(persistenceLayer);
    this.driver = checkNotNull(driver);
    this.schedulingFilter = checkNotNull(schedulingFilter);
    this.updaterTaskBuilder = checkNotNull(updaterTaskBuilder);

    restore();
  }

  @Override
  public void registered(String frameworkId) {
    this.frameworkId.set(checkNotNull(frameworkId));
    persist();
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

  /**
   * Creates a new task ID that is permanently unique (not guaranteed, but highly confident),
   * and by default sorts in chronological order.
   *
   * @param task Task that an ID is being generated for.
   * @return New task ID.
   */
  private String generateTaskId(TwitterTaskInfo task) {
    return new StringBuilder()
        .append(System.currentTimeMillis())      // Allows chronological sorting.
        .append("-")
        .append(jobKey(task))              // Identification and collision prevention.
        .append("-")
        .append(task.getShardId())               // Collision prevention within job.
        .append("-")
        .append(UUID.randomUUID())               // Just-in-case collision prevention.
        .toString().replaceAll("[^\\w-]", "-");  // Constrain character set.
  }

  // TODO(wfarner): This is does not currently clear out tasks when a host is decommissioned.
  //    Figure out a solution that will work.  Might require mesos support for fetching the list
  //    of slaves.
  @Override
  public synchronized void updateRegisteredTasks(RegisteredTaskUpdate update) {
    checkNotNull(update);
    checkNotBlank(update.getSlaveHost());
    checkNotNull(update.getTaskInfos());

    List<LiveTaskInfo> taskInfos = update.isSetTaskInfos() ? update.getTaskInfos()
        : Arrays.<LiveTaskInfo>asList();

    final AtomicBoolean mutated = new AtomicBoolean(false);

    // Wrap with a mutable map so we can modify later.
    final Map<String, LiveTaskInfo> taskInfoMap = Maps.newHashMap(
        Maps.uniqueIndex(taskInfos, Tasks.LIVE_TO_ID));

    // TODO(wfarner): Have the scheduler only retain configurations for live jobs,
    //    and acquire all other state from slaves.
    //    This will allow the scheduler to only persist active tasks.

    // Look for any tasks that we don't know about, or this slave should not be modifying.
    Query tasksForHost = new Query(new TaskQuery().setTaskIds(taskInfoMap.keySet())
        .setSlaveHost(update.getSlaveHost()));
    final Set<String> recognizedTasks = taskStore.fetchIds(tasksForHost);
    Set<String> unknownTasks = ImmutableSet.copyOf(
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

    Predicate<LiveTaskInfo> getTerminatedTasks = new Predicate<LiveTaskInfo>() {
      @Override public boolean apply(LiveTaskInfo update) {
        return !Tasks.isActive(update.getStatus());
      }
    };

    // Find any tasks that we believe to be running, but the slave reports as dead.
    Set<String> reportedDeadTasks = ImmutableSet.copyOf(
        transform(filter(taskInfoMap.values(), getTerminatedTasks), Tasks.LIVE_TO_ID));
    Set<String> deadTasks = taskStore.fetchIds(
        new Query(new TaskQuery().setTaskIds(reportedDeadTasks), Tasks.ACTIVE_FILTER));
    if (!deadTasks.isEmpty()) {
      LOG.info("Found tasks that were recorded as RUNNING but slave " + update.getSlaveHost()
               + " reports as KILLED: " + deadTasks);
      // We don't set mutated here, since it is implicit in changing task state.

      for (String deadTask : deadTasks) {
        final ScheduleStatus status = taskInfoMap.get(deadTask).getStatus();
        setTaskStatus(Query.byId(deadTask), status);
      }
    }

    // Find any tasks assigned to this slave but the slave does not report.
    Predicate<TaskState> isTaskReported = new Predicate<TaskState>() {
      @Override public boolean apply(TaskState state) {
        return recognizedTasks.contains(Tasks.id(state));
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
    Set<String> missingNotRunningTasks = taskStore.fetchIds(new Query(slaveAssignedTaskQuery,
            Predicates.not(isTaskReported), Predicates.not(Tasks.hasStatus(RUNNING)),
            lastEventBeyondGracePeriod));
    if (!missingNotRunningTasks.isEmpty()) {
      LOG.info("Removing non-running tasks no longer reported by slave " + update.getSlaveHost()
               + ": " + missingNotRunningTasks);
      mutated.set(true);
      taskStore.remove(missingNotRunningTasks);
    }

    Set<String> missingRunningTasks = taskStore.fetchIds(new Query(slaveAssignedTaskQuery,
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
    JobConfiguration populated = ConfigurationManager.validateAndPopulate(job);

    // TODO(wfarner): Add a check to make sure the job name cannot conflict with the name format
    //    used for the updater (ending with ".updater")

    if (hasActiveJob(populated)) {
      throw new ScheduleException("Job already exists: " + jobKey(populated));
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
    checkState(!hasRunningTasks(job));

    launchTasks(checkNotNull(job.getTaskConfigs()));
  }

  // TODO(wfarner): Kill function with side effects, it _will_ cause bugs.
  private Function<TwitterTaskInfo, ScheduledTask> taskCreator =
      new Function<TwitterTaskInfo, ScheduledTask>() {
        @Override
        public ScheduledTask apply(TwitterTaskInfo task) {
          AssignedTask assigned = new AssignedTask()
              .setTaskId(generateTaskId(task))
              .setTask(task);
          return changeTaskStatus(new ScheduledTask().setAssignedTask(assigned), PENDING);
        }
      };

  /**
   * Launches tasks.
   *
   * @param tasks Tasks to launch.
   * @return The task IDs of the new tasks.
   */
  private Set<String> launchTasks(Set<TwitterTaskInfo> tasks) {
    if (tasks.isEmpty()) return ImmutableSet.of();

    LOG.info("Launching " + tasks.size() + " tasks.");
    Set<ScheduledTask> scheduledTasks = ImmutableSet.copyOf(transform(tasks, taskCreator));
    taskStore.add(scheduledTasks);
    persist();

    return ImmutableSet.copyOf(transform(scheduledTasks, Tasks.SCHEDULED_TO_ID));
  }

  @Override
  public synchronized JobUpdateResult doJobUpdate(JobConfiguration job) throws ScheduleException {
    LOG.info("Updating job " + jobKey(job));

    // First, get comparable views of the start and end states.  Mapped by shard IDs.
    final Map<Integer, AssignedTask> existingTasks = Tasks.mapAssignedByShardId(
        transform(taskStore.fetch(Query.activeQuery(job)), Tasks.STATE_TO_ASSIGNED));
    final Map<Integer, TwitterTaskInfo> updatedTasks = Tasks.mapInfoByShardId(job.getTaskConfigs());

    // Tasks that are unchanged.
    Set<TwitterTaskInfo> unmodifiedTasks = Sets.newHashSet();

    // Tasks that we can silently modify, and the scheduler will automatically compensate for.
    Set<Integer> shardIdsMutatedInPlace = Sets.newHashSet();

    // Tasks that we have to restart in order for the change to take effect.
    Set<TwitterTaskInfo> tasksRequiringRestart = Sets.newHashSet();

    // Inspect each task and decide how to handle it.
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

    checkState((unmodifiedTasks.size()
                + shardIdsAdded.size()
                + shardIdsMutatedInPlace.size()
                + tasksRequiringRestart.size()) == updatedTasks.size(),
        "Consistency check failed - incorrect number of modified tasks after update.");

    if (!tasksRequiringRestart.isEmpty()) {
      Map<Integer, TwitterTaskInfo> updateFrom = Tasks.mapInfoByShardId(
          transform(existingTasks.values(), Tasks.ASSIGNED_TO_INFO));

      try {
        launchUpdater(job.getOwner(), job.getName(),
            registerUpdate(job.getUpdateConfig(), updateFrom, updatedTasks));
        return UPDATER_LAUNCHED;
      } catch (UpdateException e) {
        LOG.log(Level.WARNING, "Failed to register update for " + jobKey(job), e);
        throw new ScheduleException("Failed to register update, internal error.", e);
      }
    }

    Function<Integer, String> shardToExistingTaskId = new Function<Integer, String>() {
      @Override public String apply(Integer shardId) {
        return existingTasks.get(shardId).getTaskId();
      }
    };

    // First kill any removed tasks.
    if (!shardIdsRemoved.isEmpty()) {
      killTasks(Query.byId(transform(shardIdsRemoved, shardToExistingTaskId)));
    }

    // Launch any new tasks.  Gets all updatedTasks whose shard ID was added.
    LOG.info("Launching tasks for shard IDs being added by this update: " + shardIdsAdded);
    launchTasks(ImmutableSet.copyOf(filter(updatedTasks.values(),
        Predicates.compose(Predicates.in(shardIdsAdded), INFO_TO_SHARD_ID))));

    // Perform any in-place mutations.
    if (!shardIdsMutatedInPlace.isEmpty()) {
      Iterable<String> taskIds =
          transform(shardIdsMutatedInPlace, shardToExistingTaskId);
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

  private void launchUpdater(String owner, String updatingJobName, String updateToken) {
    TwitterTaskInfo baseUpdaterTask = updaterTaskBuilder.apply(updateToken);
    Preconditions.checkNotNull(baseUpdaterTask, "Unable to get updater task.");

    TwitterTaskInfo updaterTask = baseUpdaterTask.deepCopy()
        .setOwner(owner)
        .setJobName(updatingJobName + ".updater");
    ConfigurationManager.applyDefaultsIfUnset(updaterTask);

    launchTasks(ImmutableSet.of(updaterTask));
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
        return manager.hasJob(jobKey(job));
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
    checkNotBlank(slaveId);
    checkNotBlank(slaveHost);
    checkNotNull(offerParams);

    vars.resourceOffers.incrementAndGet();

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
        "mem", String.valueOf(task.getAssignedTask().getTask().getRamMb()));

    AssignedTask assignedTask = task.getAssignedTask();
    LOG.info(String.format("Offer on slave %s (id %s) is being assigned task for %s.",
        slaveHost, assignedTask.getSlaveId(), jobKey(assignedTask)));

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
   * @return Task IDs of the rescheduled tasks.
   */
  private Set<String> scheduleTaskCopies(Iterable<ScheduledTask> tasks) {
    if (Iterables.isEmpty(tasks)) return ImmutableSet.of();

    Set<String> newTaskIds = Sets.newHashSet();
    for (ScheduledTask task : tasks) {
      // The shard ID in the assigned task is left unchanged.
      task.getAssignedTask().unsetSlaveId();
      task.getAssignedTask().unsetSlaveHost();
      task.unsetTaskEvents();
      task.setAncestorId(Tasks.id(task));
      String taskId = generateTaskId(task.getAssignedTask().getTask());
      task.getAssignedTask().setTaskId(taskId);
      newTaskIds.add(taskId);
      changeTaskStatus(task, PENDING);
    }

    LOG.info("Tasks being rescheduled: " + tasks);

    taskStore.add(tasks);

    return newTaskIds;
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
    checkNotNull(rawQuery);
    checkNotNull(status);

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
              LOG.info("Rescheduling daemon task " + Tasks.id(state));
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
              LOG.info("Rescheduling failed task below failure limit: " + Tasks.id(state));
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
            LOG.info("Rescheduling " + status + " task: " + Tasks.id(state));
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

    if (!newTasks.isEmpty()) scheduleTaskCopies(newTasks);
    persist();
  }

  @Override
  public synchronized void killTasks(final Query query) throws ScheduleException {
    checkNotNull(query);

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
        driver.killTask(Tasks.id(state));
      }
    };

    taskStore.mutate(Query.and(query, Tasks.ACTIVE_FILTER), mutate);

    persist();
  }

  private static class JobUpdate {
    final String jobKey;
    private final UpdateConfig updateConfig;
    final Map<Integer, TwitterTaskInfo> updateFromByShard;
    final Map<Integer, TwitterTaskInfo> updateToByShard;

    JobUpdate(String jobKey, UpdateConfig updateConfig,
        Map<Integer, TwitterTaskInfo> updateFromByShard,
        Map<Integer, TwitterTaskInfo> updateToByShard) {
      this.jobKey = jobKey;
      this.updateConfig = updateConfig;
      this.updateFromByShard = updateFromByShard;
      this.updateToByShard = updateToByShard;
    }

    Map<Integer, TwitterTaskInfo> getFrom(boolean rollback) {
      return rollback ? updateToByShard : updateFromByShard;
    }

    Map<Integer, TwitterTaskInfo> getTo(boolean rollback) {
      return rollback ? updateFromByShard :updateToByShard;
    }
  }

  private final Function<JobUpdate, String> getUpdateJobKey = new Function<JobUpdate, String>() {
    @Override public String apply(JobUpdate update) {
      return update.jobKey;
    }
  };

  @VisibleForTesting synchronized String registerUpdate(UpdateConfig updateConfig,
      Map<Integer, TwitterTaskInfo> updateFromByShard,
      Map<Integer, TwitterTaskInfo> updateToByShard) throws UpdateException {
    checkNotNull(updateFromByShard);
    checkNotNull(updateToByShard);

    String oldJobKey = Iterables.getOnlyElement(ImmutableSet.copyOf(
        transform(updateFromByShard.values(), Tasks.INFO_TO_JOB_KEY)));
    String jobKey = Iterables.getOnlyElement(ImmutableSet.copyOf(
        transform(updateFromByShard.values(), Tasks.INFO_TO_JOB_KEY)));

    if (!oldJobKey.equals(jobKey)) {
      throw new UpdateException("An update cannot change a job key.");
    }
    if (updateToByShard.isEmpty()) {
      throw new UpdateException("Updated job must have at least one task.");
    }

    // Check if there is already an in-progress update for this job.
    if (isJobUpdating(jobKey)) {
      throw new UpdateException(String.format(
          "Existing update for job %s must be canceled before doing another update.", jobKey));
    }

    String updateToken = new StringBuilder()
        .append(jobKey)
        .append("-")
        .append(System.currentTimeMillis())
        .append("-")
        .append((UUID.randomUUID()))
        .toString();

    updatesInProgress.put(updateToken,
        new JobUpdate(jobKey, updateConfig, updateFromByShard, updateToByShard));

    return updateToken;
  }

  @Override
  public synchronized UpdateConfigResponse getUpdateConfig(String updateToken)
      throws UpdateException {
    checkNotBlank(updateToken);

    JobUpdate update = updatesInProgress.get(updateToken);
    if (update == null) {
      throw new UpdateException("Update token not recognized " + updateToken);
    }

    return new UpdateConfigResponse().setConfig(update.updateConfig)
        .setOldShards(update.updateFromByShard.keySet())
        .setNewShards(update.updateToByShard.keySet());
  }

  @Override
  public synchronized void updateFinished(String updateToken) throws UpdateException {
    checkNotBlank(updateToken);

    if (updatesInProgress.remove(updateToken) == null) {
      throw new UpdateException("Update token not recognized " + updateToken);
    }
  }

  @Override public void updateFinished(String owner, String jobName) throws UpdateException {
    checkNotBlank(owner);
    checkNotBlank(jobName);

    if (!Iterables.removeIf(transform(updatesInProgress.values(), getUpdateJobKey),
        Predicates.equalTo(jobKey(owner, jobName)))) {
      throw new UpdateException("No update in progress for job " + jobKey(owner, jobName));
    }
  }

  private boolean isJobUpdating(String jobKey) {
    return Iterables.any(transform(updatesInProgress.values(), getUpdateJobKey),
        Predicates.equalTo(jobKey));
  }

  @Override
  public synchronized Set<String> updateShards(String updateToken,
      final Set<Integer> restartShards, boolean rollback) throws UpdateException {
    checkNotBlank(updateToken);
    checkNotBlank(restartShards);

    LOG.info("Shard update requested with token " + updateToken + " for shards " + restartShards);
    JobUpdate update = updatesInProgress.get(updateToken);
    if (update == null) {
      throw new UpdateException("Update token not recognized: " + updateToken);
    }

    // Break down what we are updating to, an from what, based on rollback.
    final Map<Integer, TwitterTaskInfo> updateToByShard = update.getTo(rollback);
    final Map<Integer, TwitterTaskInfo> updateFromByShard = update.getFrom(rollback);

    if (!updateToByShard.keySet().containsAll(restartShards)) {
      throw new UpdateException(
          String.format("%s requested for shards, but not found in %s job: %s",
              rollback ? "Rollback" : "Update",
              rollback ? "old" : "new",
              Sets.difference(restartShards, updateToByShard.keySet())));
    }

    // Get the shards that a restart is being requested for.
    Query activeShardsQuery = new Query(
        new TaskQuery().setShardIds(restartShards).setJobKey(update.jobKey), Tasks.ACTIVE_FILTER);
    Set<TaskState> tasks = taskStore.fetch(activeShardsQuery);
    Preconditions.checkState(tasks.size() <= restartShards.size(),
        "Sanity check failed - too many tasks would be restarted.");

    // Mutate the stored task definitions.
    Set<String> taskIds = ImmutableSet.copyOf(transform(tasks, Tasks.STATE_TO_ID));
    LOG.info("Modifying tasks " + taskIds + " for " + (rollback ? "rollback" : "update"));
    final Set<ScheduledTask> tasksToUpdate = Sets.newHashSet();

    taskStore.mutate(Query.byId(taskIds), new Closure<TaskState>() {
      @Override public void execute(TaskState state) {
        ScheduleStatus originalStatus = state.task.getStatus();

        int shardId = Tasks.STATE_TO_SHARD_ID.apply(state);
        TwitterTaskInfo updatedTask = updateToByShard.get(shardId);

        ScheduledTask newTask = state.task.deepCopy();
        newTask.getAssignedTask().setTask(updatedTask);

        tasksToUpdate.add(newTask);

        changeTaskStatus(state.task, KILLED_BY_CLIENT);

        if (originalStatus != PENDING) {
          driver.killTask(Tasks.id(state));
        }
      }
    });

    // Delete the pending tasks that were updated.
    taskStore.remove(ImmutableSet.copyOf(transform(
        filter(tasks, Tasks.hasStatus(PENDING)), Tasks.STATE_TO_ID)));

    // Shard IDs that are being added in this update.  This will happen during a forward update
    // when tasks are being added as a part of the update, or during a rollback when tasks were
    // removed in the original update.
    final Set<Integer> shardIdsAdded = ImmutableSet.copyOf(
        Sets.difference(updateToByShard.keySet(), updateFromByShard.keySet()));

    final Set<Integer> shardIdsAddedOrInactive = Sets.union(shardIdsAdded,
        Sets.difference(restartShards, ImmutableSet.copyOf(
            transform(tasks, Tasks.STATE_TO_SHARD_ID))));

    Set<String> newTaskIds = Sets.newHashSet();

    // Add any tasks whose shards were inactive when the request was received.  A shard could end
    // up here if it is being added in the update, or if was dead when the request was received.
    Set<Integer> newTaskShardIds = Sets.intersection(restartShards, shardIdsAddedOrInactive);
    if (!newTaskShardIds.isEmpty()) {
      LOG.info("Launching tasks for shard IDs added in this update: " + newTaskShardIds);
      newTaskIds.addAll(launchTasks(ImmutableSet.copyOf(transform(newTaskShardIds,
          new Function<Integer, TwitterTaskInfo>() {
            @Override public TwitterTaskInfo apply(Integer shardId) {
              return updateToByShard.get(shardId);
            }
          }))));
    }

    newTaskIds.addAll(scheduleTaskCopies(tasksToUpdate));

    return newTaskIds;
  }

  @Override
  public synchronized Set<String> restartTasks(Set<String> taskIds) throws RestartException {
    checkNotBlank(taskIds);
    LOG.info("Restart requested for tasks " + taskIds);

    // TODO(wfarner): Change this (and the thrift interface) to query by shard ID in the context
    //    of a job instead of task ID.

    Query byId = Query.byId(taskIds);
    Set<TaskState> tasks = taskStore.fetch(byId);
    if (tasks.size() != taskIds.size()) {
      Set<String> unknownTasks = Sets.difference(taskIds,
          ImmutableSet.copyOf(transform(tasks, Tasks.STATE_TO_ID)));

      throw new RestartException("Restart requested for unknown tasks " + unknownTasks);
    }

    Set<String> jobKeys = ImmutableSet.copyOf(transform(tasks, Tasks.STATE_TO_JOB_KEY));
    if (jobKeys.size() != 1) {
      throw new RestartException("Task restart request cannot span multiple jobs: " + jobKeys);
    }
    if (isJobUpdating(Iterables.getOnlyElement(jobKeys))) {
      throw new RestartException(
          "Job update must complete or be canceled before restarting tasks.");
    }

    Iterable<TaskState> inactiveTasks = filter(tasks,
        Predicates.not(Tasks.ACTIVE_FILTER));
    if (!Iterables.isEmpty(inactiveTasks)) {
      LOG.warning("Restart request ignored for inactive tasks "
                  + transform(inactiveTasks, Tasks.STATE_TO_ID));
    }

    Query activeQuery = Query.and(byId, Tasks.ACTIVE_FILTER);
    Set<String> activeTaskIds = taskStore.fetchIds(activeQuery);
    taskStore.mutate(activeQuery, new Closure<TaskState>() {
      @Override public void execute(final TaskState state) {
        ScheduleStatus originalStatus = state.task.getStatus();
        changeTaskStatus(state.task, KILLED_BY_CLIENT);
        scheduleTaskCopies(Arrays.asList(new ScheduledTask(state.task)));

        if (originalStatus != PENDING) {
          driver.killTask(Tasks.id(state));
        }
      }
    });

    persist();

    return activeTaskIds;
  }

  @Override
  public synchronized JobUpdateResult updateJob(JobConfiguration updatedJob)
      throws ScheduleException, TaskDescriptionException {
    JobConfiguration populated = ConfigurationManager.validateAndPopulate(updatedJob);

    try {
      return Iterables.find(jobManagers, managerHasJob(populated)).updateJob(populated);
    } catch (NoSuchElementException e) {
      throw new ScheduleException("Job not found: " + jobKey(populated));
    }
  }

  // TODO(wfarner): Clean this up.
  private boolean stopped = false;
  @Override public synchronized void stop() {
    stopped = true;
  }

  private void persist() {
    if (stopped) {
      LOG.severe("Scheduler was stopped, ignoring persist request.");
      return;
    }

    LOG.info("Saving scheduler state.");
    NonVolatileSchedulerState state = new NonVolatileSchedulerState()
        .setFrameworkId(frameworkId.get())
        .setTasks(ImmutableList.copyOf(
            transform(taskStore.fetch(Query.GET_ALL), Tasks.STATE_TO_SCHEDULED)));
    Map<String, List<JobConfiguration>> moduleState = Maps.newHashMap();
    for (JobManager manager : jobManagers) {
      moduleState.put(manager.getUniqueKey(), Lists.newArrayList(manager.getJobs()));
    }
    state.setModuleJobs(moduleState);

    try {
      long startNanos = System.nanoTime();
      persistenceLayer.commit(state);
      vars.persistLatency.requestComplete((System.nanoTime() - startNanos) / 1000);
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

    // Apply defaults to backfill new fields.
    List<ScheduledTask> tasks = state.getTasks();
    for (ScheduledTask task : tasks) {
      ConfigurationManager.applyDefaultsIfUnset(task.getAssignedTask().getTask());
    }

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

  private final class Vars {
    final RequestStats persistLatency = new RequestStats("scheduler_persist");
    final AtomicLong resourceOffers = Stats.exportLong("scheduler_resource_offers");

    Vars() {
      for (final ScheduleStatus status : ScheduleStatus.values()) {
        Stats.export(new StatImpl<Integer>("task_store_" + status) {
          @Override public Integer read() {
            return taskStore.fetch(Query.byStatus(status)).size();
          }
        });
      }
    }
  }
  private final Vars vars = new Vars();
}
