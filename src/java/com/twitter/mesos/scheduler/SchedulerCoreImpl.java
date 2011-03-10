package com.twitter.mesos.scheduler;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveOffer;

import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.StateMachine;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.LiveTaskInfo;
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
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.StorageRole;
import com.twitter.mesos.scheduler.storage.StorageRole.Role;
import com.twitter.mesos.scheduler.storage.TaskStore;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.Tasks.INFO_TO_SHARD_ID;
import static com.twitter.mesos.Tasks.jobKey;
import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED_BY_CLIENT;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static com.twitter.mesos.scheduler.JobManager.JobUpdateResult.COMPLETED;
import static com.twitter.mesos.scheduler.JobManager.JobUpdateResult.JOB_UNCHANGED;
import static com.twitter.mesos.scheduler.JobManager.JobUpdateResult.UPDATER_LAUNCHED;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.CONSTRUCTED;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.INITIALIZED;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.STARTED;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.STOPPED;

/**
 * Implementation of the scheduler core.
 *
 * @author William Farner
 */
public class SchedulerCoreImpl implements SchedulerCore {

  private final Map<String, VolatileTaskState> taskStateById =
      new MapMaker().makeComputingMap(new Function<String, VolatileTaskState>() {
        @Override public VolatileTaskState apply(String taskId) {
          return new VolatileTaskState(taskId);
        }
      });

  private static final Logger LOG = Logger.getLogger(SchedulerCore.class.getName());

  // Schedulers that are responsible for triggering execution of jobs.
  private final ImmutableList<JobManager> jobManagers;

  // Handles all scheduler persistence
  private final Storage storage;

  // Kills the task with the id passed into execute.
  private Closure<String> killTask;

  // Filter to determine whether a task should be scheduled.
  private final SchedulingFilter schedulingFilter;

  // TODO(John Sirois): currently purely in-mem, persist this so that updates started by 1 scheduler
  // can be managed to completion by a replacement
  // Tracks updates that are in-progress, mapping from update token to update spec.
  @VisibleForTesting final Map<String, JobUpdate> updatesInProgress = Maps.newHashMap();

  private final Function<String, TwitterTaskInfo> updaterTaskBuilder;

  enum State {
    CONSTRUCTED,
    INITIALIZED,
    STARTED,
    STOPPED
  }

  private final StateMachine<State> stateMachine;

  @Inject
  public SchedulerCoreImpl(CronJobManager cronScheduler,
      ImmediateJobManager immediateScheduler,
      @StorageRole(Role.Primary) Storage storage,
      SchedulingFilter schedulingFilter,
      Function<String, TwitterTaskInfo> updaterTaskBuilder) {

    // The immediate scheduler will accept any job, so it's important that other schedulers are
    // placed first.
    this.jobManagers =
        ImmutableList.of(checkNotNull(cronScheduler), checkNotNull(immediateScheduler));

    this.storage = checkNotNull(storage);
    this.schedulingFilter = checkNotNull(schedulingFilter);
    this.updaterTaskBuilder = checkNotNull(updaterTaskBuilder);

   // TODO(John Sirois): Add a method to StateMachine or write a wrapper that allows for a read-locked
   // do-in-state assertion around a block of work.  Transition would then need to grab the write
   // lock.  Another approach is to force these transitions with:
   // SchedulerCoreFactory -> SchedulerCoreRunner -> SchedulerCore which remove all state sensitive
   // methods out of schedulerCore save for stop.
   stateMachine = StateMachine.<State>builder("scheduler-core")
       .initialState(CONSTRUCTED)
       .addState(CONSTRUCTED, INITIALIZED)
       .addState(INITIALIZED, STARTED)
       .addState(STARTED, STOPPED)
       .build();
  }

  @Override
  public String initialize() {
    checkLifecycleState(CONSTRUCTED);
    storage.start(new Work.NoResult.Quiet() {
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {

        taskStore.mutate(Query.GET_ALL, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask task) {
            // TODO(John Sirois): implement change detection in DbStorage mutate to make this more
            // efficient or else re-jigger where unset defaults get applied/handled
            ConfigurationManager.applyDefaultsIfUnset(task.getAssignedTask().getTask());
          }
        });
      }
    });

    String frameworkId = getFrameworkId();
    stateMachine.transition(INITIALIZED);
    return frameworkId;
  }

  private String getFrameworkId() {
    return storage.doInTransaction(new Work<String, RuntimeException>() {
      @Override public String apply(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) throws RuntimeException {
        return schedulerStore.fetchFrameworkId();
      }
    });
  }

  @Override
  public void start(Closure<String> killTask) {
    checkLifecycleState(INITIALIZED);
    this.killTask = Preconditions.checkNotNull(killTask);
    stateMachine.transition(STARTED);

    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) throws RuntimeException {

        for (JobManager jobManager : jobManagers) {
          for (JobConfiguration job : jobStore.fetchJobs(jobManager.getUniqueKey())) {
            try {
              jobManager.receiveJob(job);
            } catch (ScheduleException e) {
              LOG.log(Level.SEVERE, "While trying to restore state, scheduler module failed.", e);
            }
          }
        }
      }
    });
  }

  @Override
  public void registered(final String frameworkId) {
    checkStarted();
    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {
        schedulerStore.saveFrameworkId(frameworkId);
      }
    });
  }

  @Override
  public synchronized Set<TaskState> getTasks(final Query query) {
    checkStarted();
    return storage.doInTransaction(new Work.Quiet<Set<TaskState>>() {
      @Override public Set<TaskState> apply(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {
        ImmutableSortedSet<ScheduledTask> tasks = taskStore.fetch(query);
        return ImmutableSet.copyOf(Iterables.transform(tasks,
            new Function<ScheduledTask, TaskState>() {
              @Override public TaskState apply(ScheduledTask task) {
                VolatileTaskState volatileTaskState = taskStateById.get(Tasks.id(task));
                return new TaskState(task, volatileTaskState);
              }
            }));
      }
    });
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

  // TODO(William Farner): This is does not currently clear out tasks when a host is decommissioned.
  //    Figure out a solution that will work.  Might require mesos support for fetching the list
  //    of slaves.
  @Override
  public synchronized void updateRegisteredTasks(final RegisteredTaskUpdate update) {
    checkStarted();
    checkNotNull(update);
    checkNotBlank(update.getSlaveHost());
    checkNotNull(update.getTaskInfos());

    List<LiveTaskInfo> taskInfos = update.isSetTaskInfos() ? update.getTaskInfos()
        : Arrays.<LiveTaskInfo>asList();

    // Wrap with a mutable map so we can modify later.
    final Map<String, LiveTaskInfo> taskInfoMap = Maps.newHashMap(
        Maps.uniqueIndex(taskInfos, Tasks.LIVE_TO_ID));

    // TODO(William Farner): Have the scheduler only retain configurations for live jobs,
    //    and acquire all other state from slaves.
    //    This will allow the scheduler to only persist active tasks.

    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override
      protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {

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
        if (!recognizedTasks.isEmpty()) {
          taskStore.mutate(Query.byId(recognizedTasks),
              new Closure<ScheduledTask>() {
                @Override public void execute(ScheduledTask task) {
                  String taskId = Tasks.id(task);
                  LiveTaskInfo taskUpdate = taskInfoMap.get(taskId);
                  if (taskUpdate.getResources() != null) {
                    VolatileTaskState volatileTaskState = taskStateById.get(taskId);
                    volatileTaskState.resources =
                        new ResourceConsumption(taskUpdate.getResources());
                  }
                }
              });
        }

        Predicate<LiveTaskInfo> getTerminatedTasks = new Predicate<LiveTaskInfo>() {
          @Override public boolean apply(LiveTaskInfo update) {
            return !Tasks.isActive(update.getStatus());
          }
        };

        // Find any tasks that we believe to be running, but the slave reports as dead.
        Set<String> reportedDeadTasks = ImmutableSet.copyOf(
            transform(filter(taskInfoMap.values(), getTerminatedTasks), Tasks.LIVE_TO_ID));
        Set<String> deadTasks = reportedDeadTasks.isEmpty()
            ? ImmutableSet.<String>of()
            : taskStore.fetchIds(new Query(new TaskQuery().setTaskIds(reportedDeadTasks),
            Tasks.ACTIVE_FILTER));
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
        Predicate<ScheduledTask> isTaskReported = new Predicate<ScheduledTask>() {
          @Override public boolean apply(ScheduledTask task) {
            return recognizedTasks.contains(Tasks.id(task));
          }
        };
        Predicate<ScheduledTask> lastEventBeyondGracePeriod = new Predicate<ScheduledTask>() {
          @Override public boolean apply(ScheduledTask task) {
            long taskAgeMillis = System.currentTimeMillis()
                - Iterables.getLast(task.getTaskEvents()).getTimestamp();
            return taskAgeMillis > Amount.of(10, Time.MINUTES).as(Time.MILLISECONDS);
          }
        };

        TaskQuery slaveAssignedTaskQuery = new TaskQuery().setSlaveHost(update.getSlaveHost());
        Set<String> missingNotRunningTasks = taskStore.fetchIds(new Query(slaveAssignedTaskQuery,
            ImmutableList.<Predicate<ScheduledTask>>builder()
              .add(Predicates.not(isTaskReported))
              .add(Predicates.not(Tasks.hasStatus(RUNNING)))
              .add(lastEventBeyondGracePeriod)
              .build()));
        if (!missingNotRunningTasks.isEmpty()) {
          LOG.info("Removing non-running tasks no longer reported by slave " + update.getSlaveHost()
                   + ": " + missingNotRunningTasks);
          taskStore.remove(missingNotRunningTasks);
        }

        Set<String> missingRunningTasks = taskStore.fetchIds(new Query(slaveAssignedTaskQuery,
            Predicates.<ScheduledTask>and(
                Predicates.not(isTaskReported),
                Tasks.hasStatus(RUNNING))));
        if (!missingRunningTasks.isEmpty()) {
          LOG.info("Slave " + update.getSlaveHost() + " no longer reports running tasks: "
                   + missingRunningTasks + ", reporting as LOST.");

          setTaskStatus(Query.byId(missingRunningTasks), LOST);
        }
      }
    });
  }

  @Override
  public synchronized void createJob(JobConfiguration job) throws ScheduleException,
      ConfigurationManager.TaskDescriptionException {
    checkStarted();

    final JobConfiguration populated = ConfigurationManager.validateAndPopulate(job);

    // TODO(William Farner): Add a check to make sure the job name cannot conflict with the name format
    //    used for the updater (ending with ".updater")

    if (hasActiveJob(populated)) {
      throw new ScheduleException("Job already exists: " + jobKey(populated));
    }

    boolean accepted = false;
    for (final JobManager manager : jobManagers) {
      if (manager.receiveJob(populated)) {
        LOG.info("Job accepted by manager: " + manager.getUniqueKey());
        accepted = true;
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
    checkStarted();
    checkState(!hasRunningTasks(job));

    launchTasks(checkNotNull(job.getTaskConfigs()));
  }

  // TODO(William Farner): Kill function with side effects, it _will_ cause bugs.
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
    if (tasks.isEmpty()) {
      return ImmutableSet.of();
    }

    LOG.info("Launching " + tasks.size() + " tasks.");
    final Set<ScheduledTask> scheduledTasks = ImmutableSet.copyOf(transform(tasks, taskCreator));
    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {

        taskStore.add(scheduledTasks);
      }
    });

    return ImmutableSet.copyOf(transform(scheduledTasks, Tasks.SCHEDULED_TO_ID));
  }

  @Override
  public synchronized JobUpdateResult doJobUpdate(final JobConfiguration job)
      throws ScheduleException {
    checkStarted();

    LOG.info("Updating job " + jobKey(job));

    return storage.doInTransaction(new Work<JobUpdateResult, ScheduleException>() {
      @Override public JobUpdateResult apply(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) throws ScheduleException {

        // First, get comparable views of the start and end states.  Mapped by shard IDs.
        final Map<Integer, AssignedTask> existingTasks = Tasks
            .mapAssignedByShardId(transform(taskStore
                .fetch(Query.activeQuery(job)), Tasks.SCHEDULED_TO_ASSIGNED));
        final Map<Integer, TwitterTaskInfo> updatedTasks =
            Tasks.mapInfoByShardId(job.getTaskConfigs());

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

          // If we assign the existing values for all fields that (if changed) do not require
          // restart to the updated task, we can determine whether a restart is required.
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
        Set<Integer> shardIdsRemoved =
            Sets.difference(existingTasks.keySet(), updatedTasks.keySet());
        Set<Integer> shardIdsAdded = Sets.difference(updatedTasks.keySet(), existingTasks.keySet());

        if (shardIdsAdded.isEmpty()
            && shardIdsRemoved.isEmpty()
            && shardIdsMutatedInPlace.isEmpty()
            && tasksRequiringRestart.isEmpty()) {
          return JOB_UNCHANGED;
        }

        checkState((unmodifiedTasks.size() + shardIdsAdded.size() + shardIdsMutatedInPlace.size()
                    + tasksRequiringRestart.size()) == updatedTasks
            .size(), "Consistency check failed - incorrect number of modified tasks after update.");

        if (!tasksRequiringRestart.isEmpty()) {
          Map<Integer, TwitterTaskInfo> updateFrom =
              Tasks.mapInfoByShardId(transform(existingTasks.values(), Tasks.ASSIGNED_TO_INFO));

          try {
            launchUpdater(job.getOwner(), job.getName(), registerUpdate(job
                .getUpdateConfig(), updateFrom, updatedTasks));
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
        launchTasks(ImmutableSet.copyOf(filter(updatedTasks.values(), Predicates
            .compose(Predicates.in(shardIdsAdded), INFO_TO_SHARD_ID))));

        // Perform any in-place mutations.
        if (!shardIdsMutatedInPlace.isEmpty()) {
          Iterable<String> taskIds = transform(shardIdsMutatedInPlace, shardToExistingTaskId);
          taskStore.mutate(Query.byId(taskIds), new Closure<ScheduledTask>() {
            @Override public void execute(ScheduledTask task) {
              int shardId = task.getAssignedTask().getTask().getShardId();
              task.getAssignedTask().setTask(updatedTasks.get(shardId));
            }
          });
        }

        return COMPLETED;
      }
    });
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

  private Closure<ScheduledTask> taskLauncher(final String slaveId, final String slaveHost) {
    return new Closure<ScheduledTask>() {
      @Override public void execute(ScheduledTask task) {
        task.getAssignedTask().setSlaveId(slaveId).setSlaveHost(slaveHost);
        changeTaskStatus(task, STARTING);
      }
    };
  }

  @Override
  public synchronized TwitterTask offer(final SlaveOffer slaveOffer) throws ScheduleException {
    checkStarted();
    checkNotNull(slaveOffer);

    vars.resourceOffers.incrementAndGet();

    final TwitterTaskInfo offer;
    try {
      offer = ConfigurationManager.makeConcrete(slaveOffer);
    } catch (ConfigurationManager.TaskDescriptionException e) {
      LOG.log(Level.SEVERE, "Invalid slave offer", e);
      return null;
    }

    ScheduledTask task = storage.doInTransaction(new Work.Quiet<ScheduledTask>() {
      @Override public ScheduledTask apply(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {

        SortedSet<ScheduledTask> candidates = taskStore.fetch(
            Query.and(Query.byStatus(PENDING),
                schedulingFilter.makeFilter(offer, slaveOffer.getHostname())));

        if (candidates.isEmpty()) {
          return null;
        }

        LOG.info("Found " + candidates.size() + " candidates for offer.");

        // Choose the first (top) candidate and launch it.
        return Iterables.getOnlyElement(taskStore.mutate(
            Query.byId(Tasks.id(Iterables.get(candidates, 0))),
            taskLauncher(slaveOffer.getSlaveId().getValue(), slaveOffer.getHostname())));
      }
    });

    // There were no PENDING candidates
    if (task == null) {
      return null;
    }

    AssignedTask assignedTask = task.getAssignedTask();
    LOG.info(String.format("Offer on slave %s (id %s) is being assigned task for %s.",
        slaveOffer.getHostname(), assignedTask.getSlaveId(), jobKey(assignedTask)));

    List<Resource> resources = ImmutableList.of(
        Resources.makeResource(Resources.CPUS, task.getAssignedTask().getTask().getNumCpus()),
        Resources.makeResource(Resources.RAM_MB, task.getAssignedTask().getTask().getRamMb())
    );

    return new TwitterTask(assignedTask.getTaskId(), slaveOffer.getSlaveId().getValue(),
        assignedTask.getTask().getJobName() + "-" + assignedTask.getTaskId(), resources,
        assignedTask);
  }

  /**
   * Schedules {@code tasks}, which are expected to be copies of existing tasks.  The tasks provided
   * will be modified.
   *
   * @param tasks Copies of other tasks, to be scheduled.
   * @param taskStore The store to write the task copies to.
   * @return Task IDs of the rescheduled tasks.
   */
  private Set<String> scheduleTaskCopies(Iterable<ScheduledTask> tasks, TaskStore taskStore) {
    if (Iterables.isEmpty(tasks)) {
      return ImmutableSet.of();
    }

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

    taskStore.add(ImmutableSet.copyOf(tasks));

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
    task.addToTaskEvents(
        new TaskEvent().setTimestamp(System.currentTimeMillis()).setStatus(status));
    return task;
  }

  @Override
  public synchronized void setTaskStatus(Query rawQuery, final ScheduleStatus status) {
    checkStarted();
    checkNotNull(rawQuery);
    checkNotNull(status);

    // Only allow state transition from non-terminal state.
    final Query query = Query.and(rawQuery, Predicates.not(Tasks.TERMINATED_FILTER));

    final List<ScheduledTask> newTasks = Lists.newLinkedList();

    final Closure<ScheduledTask> mutateOperation;

    switch (status) {
      case PENDING:
      case STARTING:
      case RUNNING:
        // Simply assign the new state.
        mutateOperation = new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask task) {
            changeTaskStatus(task, status);
          }
        };

        break;

      case FINISHED:
        // Assign the FINISHED state to non-daemon tasks, move daemon tasks to PENDING.
        mutateOperation = new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask task) {
            if (task.getAssignedTask().getTask().isIsDaemon()) {
              LOG.info("Rescheduling daemon task " + Tasks.id(task));
              newTasks.add(new ScheduledTask(task));
            }

            changeTaskStatus(task, FINISHED);
          }
        };

        break;

      case FAILED:
        // Increment failure count, move to pending state, unless failure limit has been reached.
        mutateOperation = new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask task) {
            task.setFailureCount(task.getFailureCount() + 1);

            boolean failureLimitReached =
                (task.getAssignedTask().getTask().getMaxTaskFailures() != -1)
                && (task.getFailureCount()
                    >= task.getAssignedTask().getTask().getMaxTaskFailures());

            if (!failureLimitReached) {
              LOG.info("Rescheduling failed task below failure limit: " + Tasks.id(task));
              newTasks.add(new ScheduledTask(task));
            }

            changeTaskStatus(task, FAILED);
          }
        };

        break;

      case KILLED: // This can happen if the executor dies, or the task process itself is killed.
      case LOST:
      case NOT_FOUND:
        // Move to pending state.
        mutateOperation = new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask task) {
            LOG.info("Rescheduling " + status + " task: " + Tasks.id(task));
            newTasks.add(new ScheduledTask(task));
            changeTaskStatus(task, status);
          }
        };

        break;

      default:
        LOG.severe("Unknown schedule status " + status + " cannot be applied to query " + query);
        return;
    }

    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {

        taskStore.mutate(query, mutateOperation);

        if (!newTasks.isEmpty()) {
          scheduleTaskCopies(newTasks, taskStore);
        }
      }
    });
  }

  @Override
  public synchronized void killTasks(final Query query) throws ScheduleException {
    checkStarted();
    checkNotNull(query);

    LOG.info("Killing tasks matching " + query);

    storage.doInTransaction(new Work.NoResult<ScheduleException>() {
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) throws ScheduleException {

        // If this looks like a query for all tasks in a job, instruct the scheduler modules to
        // delete the job.
        boolean matchingScheduler = false;

        if (query.specifiesJobOnly()) {
          String jobKey = query.getJobKey();

          for (JobManager manager : jobManagers) {
            if (manager.deleteJob(jobKey)) {
              matchingScheduler = true;
            }
          }
        }

        // Don't change the state of terminated tasks.
        if (!matchingScheduler && taskStore.fetch(Query.and(query, Tasks.ACTIVE_FILTER))
            .isEmpty()) {
          throw new ScheduleException("No tasks matching query found.");
        }

        // First remove any pending tasks matching the query.
        taskStore.remove(Query.and(query, Tasks.hasStatus(PENDING)));

        Closure<ScheduledTask> mutate = new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask task) {
            changeTaskStatus(task, KILLED_BY_CLIENT);
            killTask.execute(Tasks.id(task));
          }
        };

        taskStore.mutate(Query.and(query, Tasks.ACTIVE_FILTER), mutate);
      }
    });
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

  @VisibleForTesting
  synchronized String registerUpdate(UpdateConfig updateConfig,
      Map<Integer, TwitterTaskInfo> updateFromByShard,
      Map<Integer, TwitterTaskInfo> updateToByShard) throws UpdateException {
    checkStarted();
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

  public synchronized UpdateConfigResponse getUpdateConfig(String updateToken)
      throws UpdateException {
    checkStarted();
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
    checkStarted();
    checkNotBlank(updateToken);

    if (updatesInProgress.remove(updateToken) == null) {
      throw new UpdateException("Update token not recognized " + updateToken);
    }
  }

  @Override
  public void updateFinished(String owner, String jobName) throws UpdateException {
    checkStarted();
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
      final Set<Integer> restartShards, final boolean rollback) throws UpdateException {
    checkStarted();
    checkNotBlank(updateToken);
    checkNotBlank(restartShards);

    LOG.info("Shard update requested with token " + updateToken + " for shards " + restartShards);
    final JobUpdate update = updatesInProgress.get(updateToken);
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

    return storage.doInTransaction(new Work.Quiet<Set<String>>() {
      @Override public Set<String> apply(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {

        // Get the shards that a restart is being requested for.
        Query activeShardsQuery = new Query(
            new TaskQuery().setShardIds(restartShards).setJobKey(update.jobKey),
            Tasks.ACTIVE_FILTER);
        Set<ScheduledTask> tasks = taskStore.fetch(activeShardsQuery);
        Preconditions.checkState(tasks.size() <= restartShards.size(),
            "Sanity check failed - too many tasks would be restarted.");

        // Mutate the stored task definitions.
        Set<String> taskIds = ImmutableSet.copyOf(transform(tasks, Tasks.SCHEDULED_TO_ID));
        LOG.info("Modifying tasks " + taskIds + " for " + (rollback ? "rollback" : "update"));
        final Set<ScheduledTask> tasksToUpdate = Sets.newHashSet();

        taskStore.mutate(Query.byId(taskIds), new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask task) {
            ScheduleStatus originalStatus = task.getStatus();

            int shardId = Tasks.SCHEDULED_TO_SHARD_ID.apply(task);
            TwitterTaskInfo updatedTask = updateToByShard.get(shardId);

            ScheduledTask newTask = task.deepCopy();
            newTask.getAssignedTask().setTask(updatedTask);

            tasksToUpdate.add(newTask);

            changeTaskStatus(task, KILLED_BY_CLIENT);

            if (originalStatus != PENDING) {
              killTask.execute(Tasks.id(task));
            }
          }
        });

        // Delete the pending tasks that were updated.
        taskStore.remove(ImmutableSet.copyOf(transform(
            filter(tasks, Tasks.hasStatus(PENDING)), Tasks.SCHEDULED_TO_ID)));

        // Shard IDs that are being added in this update.  This will happen during a forward update
        // when tasks are being added as a part of the update, or during a rollback when tasks were
        // removed in the original update.
        final Set<Integer> shardIdsAdded = ImmutableSet.copyOf(
            Sets.difference(updateToByShard.keySet(), updateFromByShard.keySet()));

        final Set<Integer> shardIdsAddedOrInactive = Sets.union(shardIdsAdded,
            Sets.difference(restartShards, ImmutableSet.copyOf(
                transform(tasks, Tasks.SCHEDULED_TO_SHARD_ID))));

        Set<String> newTaskIds = Sets.newHashSet();

        // Add any tasks whose shards were inactive when the request was received.  A shard could
        // end up here if it is being added in the update, or if was dead when the request was
        // received.
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

        newTaskIds.addAll(scheduleTaskCopies(tasksToUpdate, taskStore));

        return newTaskIds;
      }
    });
  }

  @Override
  public synchronized Set<String> restartTasks(final Set<String> taskIds) throws RestartException {
    checkStarted();
    checkNotBlank(taskIds);

    LOG.info("Restart requested for tasks " + taskIds);

    // TODO(William Farner): Change this (and the thrift interface) to query by shard ID in the context
    //    of a job instead of task ID.

    return storage.doInTransaction(new Work<Set<String>, RestartException>() {
      @Override public Set<String> apply(SchedulerStore schedulerStore, JobStore jobStore,
          final TaskStore taskStore) throws RestartException {

        Query byId = Query.byId(taskIds);
        Set<ScheduledTask> tasks = taskStore.fetch(byId);
        if (tasks.size() != taskIds.size()) {
          Set<String> unknownTasks = Sets.difference(taskIds, ImmutableSet
              .copyOf(transform(tasks, Tasks.SCHEDULED_TO_ID)));

          throw new RestartException("Restart requested for unknown tasks " + unknownTasks);
        }

        Set<String> jobKeys = ImmutableSet.copyOf(transform(tasks, Tasks.SCHEDULED_TO_JOB_KEY));
        if (jobKeys.size() != 1) {
          throw new RestartException("Task restart request cannot span multiple jobs: " + jobKeys);
        }
        if (isJobUpdating(Iterables.getOnlyElement(jobKeys))) {
          throw new RestartException("Job update must complete or be canceled before restarting tasks.");
        }

        Iterable<ScheduledTask> inactiveTasks = filter(tasks, Predicates.not(Tasks.ACTIVE_FILTER));
        if (!Iterables.isEmpty(inactiveTasks)) {
          LOG.warning("Restart request ignored for inactive tasks "
                      + transform(inactiveTasks, Tasks.SCHEDULED_TO_ID));
        }

        Query activeQuery = Query.and(byId, Tasks.ACTIVE_FILTER);
        Set<String> activeTaskIds = taskStore.fetchIds(activeQuery);
        taskStore.mutate(activeQuery, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask task) {
            ScheduleStatus originalStatus = task.getStatus();
            changeTaskStatus(task, KILLED_BY_CLIENT);
            scheduleTaskCopies(Arrays.asList(new ScheduledTask(task)), taskStore);

            if (originalStatus != PENDING) {
              killTask.execute(Tasks.id(task));
            }
          }
        });

        return activeTaskIds;
      }
    });
  }

  @Override
  public synchronized JobUpdateResult updateJob(JobConfiguration updatedJob)
      throws ScheduleException, TaskDescriptionException {
    checkStarted();

    JobConfiguration populated = ConfigurationManager.validateAndPopulate(updatedJob);

    try {
      return Iterables.find(jobManagers, managerHasJob(populated)).updateJob(populated);
    } catch (NoSuchElementException e) {
      throw new ScheduleException("Job not found: " + jobKey(populated));
    }
  }

  @Override
  public void stop() {
    checkStarted();
    storage.stop();
    stateMachine.transition(STOPPED);
  }

  private void checkStarted() {
    checkLifecycleState(STARTED);
  }

  private void checkLifecycleState(State state) {
    Preconditions.checkState(stateMachine.getState() == state);
  }

  private final class Vars {
    final AtomicLong resourceOffers = Stats.exportLong("scheduler_resource_offers");

    Vars() {
      for (final ScheduleStatus status : ScheduleStatus.values()) {
        Stats.export(new StatImpl<Integer>("task_store_" + status) {
          @Override public Integer read() {
            return storage.doInTransaction(new Work.Quiet<Integer>() {
              @Override public Integer apply(SchedulerStore schedulerStore, JobStore jobStore,
                  TaskStore taskStore) {

                // TODO(John Sirois): optimize this for DbStorage, currently burdened by un-needed
                // deserialization of ScheduledTask data - just need a select count
                return taskStore.fetch(Query.byStatus(status)).size();
              }
            });
          }
        });
      }
    }
  }
  private final Vars vars = new Vars();
}
