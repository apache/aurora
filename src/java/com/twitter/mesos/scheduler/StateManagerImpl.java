package com.twitter.mesos.scheduler;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Ranges;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.SlaveID;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Closure;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;
import com.twitter.common.util.StateMachine;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.gen.storage.JobUpdateConfiguration;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;
import com.twitter.mesos.scheduler.StateManagerVars.MutableState;
import com.twitter.mesos.scheduler.TransactionalStorage.SideEffect;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.Storage.Work.NoResult;
import com.twitter.mesos.scheduler.storage.TaskStore;
import com.twitter.mesos.scheduler.storage.UpdateStore;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.Tasks.jobKey;
import static com.twitter.mesos.gen.ScheduleStatus.INIT;
import static com.twitter.mesos.gen.ScheduleStatus.KILLING;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.UNKNOWN;

/**
 * Manager of all persistence-related operations for the scheduler.  Acts as a controller for
 * persisted state machine transitions, and their side-effects.
 */
public class StateManagerImpl implements StateManager {

  @VisibleForTesting
  @CmdLine(name = "transient_task_state_timeout",
      help = "The amount of time after which to treat a task stuck in a transient state as LOST.")
  static final Arg<Amount<Long, Time>> TRANSIENT_TASK_STATE_TIMEOUT =
      Arg.create(Amount.of(5L, Time.MINUTES));

  private static final Logger LOG = Logger.getLogger(StateManagerImpl.class.getName());

  private static final TaskQuery OUTSTANDING_TASK_QUERY = new TaskQuery().setStatuses(
      EnumSet.of(
          ScheduleStatus.ASSIGNED,
          ScheduleStatus.STARTING,
          ScheduleStatus.PREEMPTING,
          ScheduleStatus.RESTARTING,
          ScheduleStatus.KILLING,
          ScheduleStatus.UPDATING,
          ScheduleStatus.ROLLBACK));

  private static final Function<TaskUpdateConfiguration, Integer> GET_SHARD =
      new Function<TaskUpdateConfiguration, Integer>() {
        @Override public Integer apply(TaskUpdateConfiguration config) {
          return config.isSetOldConfig()
              ? config.getOldConfig().getShardId()
              : config.getNewConfig().getShardId();
        }
      };

  private static final Function<Protos.Attribute, String> ATTRIBUTE_NAME =
      new Function<org.apache.mesos.Protos.Attribute, String>() {
        @Override public String apply(Protos.Attribute attr) {
          return attr.getName();
        }
      };

  private static final Predicate<TaskUpdateConfiguration> SELECT_SHARDS_TO_KILL =
      new Predicate<TaskUpdateConfiguration>() {
        @Override public boolean apply(TaskUpdateConfiguration config) {
          return config.getNewConfig() == null;
        }
      };

  private static final Function<TaskUpdateConfiguration, Integer> GET_ORIGINAL_SHARD_ID =
    new Function<TaskUpdateConfiguration, Integer>() {
      @Override public Integer apply(TaskUpdateConfiguration config) {
        return config.getOldConfig().getShardId();
      }
    };

  private static final Function<Protos.Attribute, String> VALUE_CONVERTER =
      new Function<Protos.Attribute, String>() {
        @Override public String apply(Protos.Attribute attribute) {
          switch (attribute.getType()) {
            case SCALAR:
              return String.valueOf(attribute.getScalar().getValue());

            case TEXT:
              return attribute.getText().getValue();

            default:
              LOG.finest("Unrecognized attribute type:" + attribute.getType() + " , ignoring.");
              return null;
          }
        }
      };

  private static final AttributeConverter ATTRIBUTE_CONVERTER = new AttributeConverter() {
    @Override public Attribute apply(Entry<String, Collection<Protos.Attribute>> entry) {
      // Convert values and filter any that were ignored.
      Iterable<String> values = Iterables.filter(
          Iterables.transform(entry.getValue(), VALUE_CONVERTER), Predicates.notNull());

      return new Attribute(entry.getKey(), ImmutableSet.copyOf(values));
    }
  };

  private final AtomicLong shardSanityCheckFails = Stats.exportLong("shard_sanity_check_failures");

  private final TransactionalStorage transactionalStorage;

  // Enforces lifecycle of the manager, ensuring proper call order.
  private final StateMachine<State> managerState = StateMachine.<State>builder("state_manager")
      .initialState(State.CREATED)
      .addState(State.CREATED, State.INITIALIZED)
      .addState(State.INITIALIZED, State.STARTED)
      .addState(State.STARTED, State.STOPPED)
      .build();

  // Work queue to receive state machine side effect work.
  private final Queue<WorkEntry> workQueue = Lists.newLinkedList();

  // Adapt the work queue into a sink.
  private final TaskStateMachine.WorkSink workSink = new TaskStateMachine.WorkSink() {
      @Override public void addWork(WorkCommand work, TaskStateMachine stateMachine,
          Closure<ScheduledTask> mutation) {
        workQueue.add(new WorkEntry(work, stateMachine, mutation));
      }
    };

  private final Function<TwitterTaskInfo, ScheduledTask> taskCreator =
      new Function<TwitterTaskInfo, ScheduledTask>() {
        @Override public ScheduledTask apply(TwitterTaskInfo task) {
          return new ScheduledTask()
              .setStatus(INIT)
              .setAssignedTask(new AssignedTask().setTaskId(generateTaskId(task)).setTask(task));
        }
      };

  private final Function<ScheduledTask, Long> latestStateDuration;
  private final Predicate<ScheduledTask> taskTimedOut;

  private final Driver driver;
  private final Clock clock;

  /**
   * An item of work on the work queue.
   */
  private static class WorkEntry {
    private final WorkCommand command;
    private final TaskStateMachine stateMachine;
    private final Closure<ScheduledTask> mutation;

    WorkEntry(WorkCommand command, TaskStateMachine stateMachine,
        Closure<ScheduledTask> mutation) {
      this.command = command;
      this.stateMachine = stateMachine;
      this.mutation = mutation;
    }
  }

  @Inject
  StateManagerImpl(Storage storage, final Clock clock, MutableState mutableState, Driver driver) {
    checkNotNull(storage);
    this.clock = checkNotNull(clock);

    transactionalStorage = new TransactionalStorage(storage, mutableState,
        new Closure<StoreProvider>() {
          @Override public void execute(StoreProvider storeProvider) {
            processWorkQueueInTransaction(storeProvider);
          }
        });

    this.latestStateDuration = Functions.compose(
      new Function<Iterable<TaskEvent>, Long>() {
        @Override public Long apply(Iterable<TaskEvent> events) {
          return clock.nowMillis() - Iterables.getLast(events).getTimestamp();
        }
      }, Tasks.GET_TASK_EVENTS);

    this.taskTimedOut = Predicates.compose(
        Ranges.greaterThan(TRANSIENT_TASK_STATE_TIMEOUT.get().as(Time.MILLISECONDS)),
        latestStateDuration);

    this.driver = driver;

    Stats.exportSize("work_queue_depth", workQueue);
  }

  @VisibleForTesting
  Function<TwitterTaskInfo, ScheduledTask> getTaskCreator() {
    return taskCreator;
  }

  /**
   * State manager lifecycle state.
   */
  private enum State {
    CREATED,
    INITIALIZED,
    STARTED,
    STOPPED
  }

  /**
   * Prompts the state manager to prepare for possible activation in the leading scheduler process.
   */
  void prepare() {
    transactionalStorage.prepare();
  }

  /**
   * Initializes the state manager, by starting the storage and fetching the persisted framework ID.
   *
   * @return The persisted framework ID, or {@code null} if no framework ID exists in the store.
   */
  @Nullable
  synchronized String initialize() {
    managerState.transition(State.INITIALIZED);

    // Note: Do not attempt to mutate tasks here.  Any task mutations made here will be
    // overwritten if a snapshot is applied.

    transactionalStorage.start(new Work.NoResult.Quiet() {
      @Override protected void execute(final Storage.StoreProvider storeProvider) {
        for (ScheduledTask task : storeProvider.getTaskStore().fetchTasks(Query.GET_ALL)) {
          createStateMachine(task, task.getStatus());
        }
        transactionalStorage.addSideEffect(new SideEffect() {
          @Override public void mutate(MutableState state) {
            state.getVars().beginExporting();
          }
        });
      }
    });

    LOG.info("Storage initialization complete.");
    return getFrameworkId();
  }

  private String getFrameworkId() {
    managerState.checkState(ImmutableSet.of(State.INITIALIZED, State.STARTED));

    return transactionalStorage.doInTransaction(new Work<String, RuntimeException>() {
      @Override
      public String apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getSchedulerStore().fetchFrameworkId();
      }
    });
  }

  /**
   * Sets the framework ID that should be persisted.
   *
   * @param frameworkId Updated framework ID.
   */
  synchronized void setFrameworkId(final String frameworkId) {
    checkNotNull(frameworkId);

    managerState.checkState(ImmutableSet.of(State.INITIALIZED, State.STARTED));

    transactionalStorage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(Storage.StoreProvider storeProvider) {
        storeProvider.getSchedulerStore().saveFrameworkId(frameworkId);
      }
    });
  }

  private static Set<String> activeShards(TaskStore taskStore, String jobKey, int shardId) {
    TaskQuery query = new TaskQuery()
        .setStatuses(Tasks.ACTIVE_STATES)
        .setJobKey(jobKey)
        .setShardIds(ImmutableSet.of(shardId));
    return taskStore.fetchTaskIds(query);
  }

  /**
   * Instructs the state manager to start.
   */
  synchronized void start() {
    managerState.transition(State.STARTED);

    transactionalStorage.doInTransaction(new Work.NoResult.Quiet() {
      @Override
      protected void execute(final StoreProvider storeProvider) {
        for (String id : storeProvider.getJobStore().fetchManagerIds()) {
          for (JobConfiguration job : storeProvider.getJobStore().fetchJobs(id)) {
            ConfigurationManager.applyDefaultsIfUnset(job);
            storeProvider.getJobStore().saveAcceptedJob(id, job);
          }
        }
        LOG.info("Performing shard uniqueness sanity check.");
        storeProvider.getTaskStore().mutateTasks(Query.GET_ALL, new Closure<ScheduledTask>() {
          @Override
          public void execute(final ScheduledTask task) {
            ConfigurationManager.applyDefaultsIfUnset(task.getAssignedTask().getTask());

            // Perform a sanity check on the number of active shards.
            Set<String> activeTasksInShard = activeShards(
                storeProvider.getTaskStore(),
                Tasks.jobKey(task),
                Tasks.SCHEDULED_TO_SHARD_ID.apply(task));

            if (activeTasksInShard.size() > 1) {
              shardSanityCheckFails.incrementAndGet();
              LOG.severe("Active shard sanity check failed when loading " + Tasks.id(task)
                  + ", active tasks found: " + activeTasksInShard);

              // We want to keep exactly one task from this shard, so sort the IDs and keep the
              // highest (newest) in the hopes that it is legitimately running.
              if (!Tasks.id(task).equals(Iterables.getLast(Sets.newTreeSet(activeTasksInShard)))) {
                task.setStatus(ScheduleStatus.KILLED);
                task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.KILLED)
                    .setMessage("Killed duplicate shard."));
                // TODO(wfarner); Circle back if this is necessary.  Currently there's a race
                // condition between the time the scheduler is actually available without hitting
                // IllegalStateException (see DriverImpl).
                // driver.killTask(Tasks.id(task));

                transactionalStorage.addSideEffect(new SideEffect() {
                  @Override
                  public void mutate(MutableState state) {
                    state.getVars().adjustCount(
                        Tasks.jobKey(task),
                        task.getStatus(),
                        ScheduleStatus.KILLED);
                  }
                });
              } else {
                LOG.info("Retaining task " + Tasks.id(task));
              }
            } else {
              TaskEvent latestEvent = task.isSetTaskEvents()
                  ? Iterables.getLast(task.getTaskEvents(), null) : null;
              if ((latestEvent == null) || (latestEvent.getStatus() != task.getStatus())) {
                LOG.severe("Task " + Tasks.id(task) + " has no event for current status.");
                task.addToTaskEvents(new TaskEvent(clock.nowMillis(), task.getStatus())
                    .setMessage("Synthesized missing event."));
              }
            }
          }
        });
      }
    });
  }

  /**
   * Checks whether the state manager is currently in the STARTED state.
   *
   * @return {@code true} if in the STARTED state, {@code false} otherwise.
   */
  public synchronized boolean isStarted() {
    return managerState.getState() == State.STARTED;
  }

  /**
   * Instructs the state manager to stop, and shut down the backing storage.
   */
  synchronized void stop() {
    managerState.transition(State.STOPPED);

    transactionalStorage.stop();
  }

  /**
   * Inserts new tasks into the store.
   *
   * @param tasks Tasks to insert.
   * @return Generated task IDs for the tasks inserted.
   */
  synchronized Set<String> insertTasks(Set<TwitterTaskInfo> tasks) {
    checkNotNull(tasks);

    final Set<ScheduledTask> scheduledTasks = ImmutableSet.copyOf(transform(tasks, taskCreator));

    transactionalStorage.doInTransaction(new Work.NoResult.Quiet() {
      @Override
      protected void execute(Storage.StoreProvider storeProvider) {
        storeProvider.getTaskStore().saveTasks(scheduledTasks);

        for (ScheduledTask task : scheduledTasks) {
          createStateMachine(task).updateState(PENDING);
        }
      }
    });

    return Tasks.ids(scheduledTasks);
  }

  /**
   * Thrown when an update fails.
   */
  static class UpdateException extends Exception {
    public UpdateException(String msg) {
      super(msg);
    }
    public UpdateException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  /**
   * Registers a new update.
   *
   * @param role Role to register an update for.
   * @param job Job to register an update for.
   * @param updatedTasks Updated Task information to be registered.
   * @throws UpdateException If no active tasks are found for the job, or if an update for the job
   *     is already in progress.
   * @return A unique string identifying the update.
   */
  synchronized String registerUpdate(final String role, final String job,
      final Set<TwitterTaskInfo> updatedTasks) throws UpdateException {

    checkNotBlank(role);
    checkNotBlank(job);
    checkNotBlank(updatedTasks);

    return transactionalStorage.doInTransaction(new Work<String, UpdateException>() {
      @Override
      public String apply(Storage.StoreProvider storeProvider) throws UpdateException {
        String jobKey = Tasks.jobKey(role, job);
        Set<TwitterTaskInfo> existingTasks = ImmutableSet.copyOf(Iterables.transform(
            storeProvider.getTaskStore().fetchTasks(Query.activeQuery(jobKey)),
            Tasks.SCHEDULED_TO_INFO));

        if (existingTasks.isEmpty()) {
          throw new UpdateException("No active tasks found for job" + jobKey);
        }

        UpdateStore updateStore = storeProvider.getUpdateStore();
        if (fetchShardUpdateConfig(updateStore, role, job, 0).isPresent()) {
          throw new UpdateException("Update already in progress for " + jobKey);
        }

        Map<Integer, TwitterTaskInfo> oldShards = Maps.uniqueIndex(existingTasks,
            Tasks.INFO_TO_SHARD_ID);
        Map<Integer, TwitterTaskInfo> newShards = Maps.uniqueIndex(updatedTasks,
            Tasks.INFO_TO_SHARD_ID);

        ImmutableSet.Builder<TaskUpdateConfiguration> shardConfigBuilder = ImmutableSet.builder();
        for (int shard : Sets.union(oldShards.keySet(), newShards.keySet())) {
          shardConfigBuilder.add(
              new TaskUpdateConfiguration(oldShards.get(shard), newShards.get(shard)));
        }

        String updateToken = UUID.randomUUID().toString();
        updateStore.saveJobUpdateConfig(
            new JobUpdateConfiguration(role, job, updateToken, shardConfigBuilder.build()));
        return updateToken;
      }
    });
  }

  private Optional<TaskUpdateConfiguration> fetchShardUpdateConfig(UpdateStore updateStore,
      String role, String job, int shard) {

    Optional<JobUpdateConfiguration> optional = updateStore.fetchJobUpdateConfig(role, job);
    return optional.isPresent()
        ? fetchShardUpdateConfig(optional.get(), shard)
        : Optional.<TaskUpdateConfiguration>absent();
  }

  private Optional<TaskUpdateConfiguration> fetchShardUpdateConfig(JobUpdateConfiguration config,
      int shard) {

    Set<TaskUpdateConfiguration> matches = fetchShardUpdateConfigs(config, ImmutableSet.of(shard));
    return Optional.fromNullable(Iterables.getOnlyElement(matches, null));
  }

  private Set<TaskUpdateConfiguration> fetchShardUpdateConfigs(JobUpdateConfiguration config,
      Set<Integer> shards) {

    return ImmutableSet.copyOf(Iterables.filter(config.getConfigs(),
        Predicates.compose(Predicates.in(shards), GET_SHARD)));
  }

  /**
   * Terminates an in-progress update.
   *
   * @param role Role owning the update to finish.
   * @param job Job to finish updating.
   * @param updateToken Token associated with the update.  If present, the token must match the
   *     the stored token for the update.
   * @param result The result of the update.
   * @param throwIfMissing If {@code true}, this throws UpdateException when the update is missing.
   * @return {@code true} if the update was finished, false if nonexistent.
   * @throws UpdateException If an update is not in-progress for the job, or the non-null token
   *     does not match the stored token.
   */
  synchronized boolean finishUpdate(final String role, final String job,
      final Optional<String> updateToken, final UpdateResult result, final boolean throwIfMissing)
      throws UpdateException {
    checkNotBlank(role);
    checkNotBlank(job);

    return transactionalStorage.doInTransaction(new Work<Boolean, UpdateException>() {
      @Override public Boolean apply(StoreProvider storeProvider) throws UpdateException {
        UpdateStore updateStore = storeProvider.getUpdateStore();

        String jobKey = Tasks.jobKey(role, job);

        // Since we store all shards in a job with the same token, we can just check shard 0,
        // which is always guaranteed to exist for a job.
        Optional<JobUpdateConfiguration> jobConfig = updateStore.fetchJobUpdateConfig(role, job);
        if (!jobConfig.isPresent()) {
          if (throwIfMissing) {
            throw new UpdateException("Update does not exist for " + jobKey);
          }
          return false;
        }

        if (updateToken.isPresent()
            && !updateToken.get().equals(jobConfig.get().getUpdateToken())) {
          throw new UpdateException("Invalid update token for " + jobKey);
        }

        if (result == UpdateResult.SUCCESS) {
          for (Integer shard : fetchShardsToKill(jobConfig.get())) {
            changeState(Query.liveShard(jobKey, shard), KILLING, "Removed during update.");
          }
        }

        updateStore.removeShardUpdateConfigs(role, job);
        return true;
      }
    });
  }

  private Set<Integer> fetchShardsToKill(JobUpdateConfiguration jobConfig) {
    return ImmutableSet.copyOf(Iterables.transform(Iterables.filter(
        jobConfig.getConfigs(), SELECT_SHARDS_TO_KILL),
        GET_ORIGINAL_SHARD_ID));
  }

  /**
   * Typedef to make anonymous implementation more concise.
   */
  private abstract static class AttributeConverter
      implements Function<Entry<String, Collection<Protos.Attribute>>, Attribute> { }

  /**
   * Persists the attributes associated with a host.
   *
   * @param slaveHost Host to save attributes for.
   * @param attributes Attributes associated with the host.
   */
  synchronized void saveAttributesFromOffer(final String slaveHost,
      List<Protos.Attribute> attributes) {

    MorePreconditions.checkNotBlank(slaveHost);
    Preconditions.checkNotNull(attributes);

    // Group by attribute name.
    final Multimap<String, Protos.Attribute> valuesByName =
        Multimaps.index(attributes, ATTRIBUTE_NAME);

    // Convert groups into individual attributes.
    final Set<Attribute> attrs = Sets.newHashSet(
        Iterables.transform(valuesByName.asMap().entrySet(), ATTRIBUTE_CONVERTER));

    transactionalStorage.execute(new NoResult.Quiet() {
      @Override protected void execute(StoreProvider storeProvider) throws RuntimeException {
        storeProvider.getAttributeStore().saveHostAttributes(new HostAttributes(slaveHost, attrs));
      }
    });
  }

  /**
   * An entity that may modify state of tasks by ID.
   */
  interface StateChanger {

    /**
     * Changes the state of tasks.
     *
     * @param taskIds IDs of the tasks to modify.
     * @param state New state to apply to the tasks.
     * @param auditMessage Audit message to associate with the transition.
     */
    void changeState(Set<String> taskIds, ScheduleStatus state, String auditMessage);
  }

  /**
   * A mutation performed on the results of a query.
   */
  interface StateMutation<E extends Exception> {
    void execute(Set<ScheduledTask> tasks, StateChanger changer) throws E;

    /**
     * A state mutation that does not throw a checked exception.
     */
    interface Quiet extends StateMutation<RuntimeException> { }
  }

  /**
   * Performs an operation on the state based on a fixed query.
   *
   * @param query The query to perform, whose tasks will be made available to the callback via
   *    the provided {@link StateChanger}.
   * @param operation Operation to be performed.
   * @param <E> Type of exception thrown by the state change.
   * @throws E If the operation fails.
   */
  synchronized <E extends Exception> void taskOperation(final TaskQuery query,
      final StateMutation<E> operation) throws E {
    checkNotNull(operation);

    transactionalStorage.doInTransaction(new NoResult<E>() {
      @Override protected void execute(Storage.StoreProvider storeProvider) throws E {
        final TaskStore taskStore = storeProvider.getTaskStore();
        Set<ScheduledTask> tasks = (query == null) ? null : taskStore.fetchTasks(query);

        operation.execute(tasks, new StateChanger() {
          @Override public void changeState(Set<String> taskIds, ScheduleStatus state,
              String auditMessage) {
            changeStateInTransaction(taskIds, stateUpdaterWithAuditMessage(state, auditMessage));
          }
        });
      }
    });
  }

  /**
   * Performs a simple state change, transitioning all tasks matching a query to the given
   * state.
   * No audit message will be applied with the transition.
   *
   * @param query Query to perform, the results of which will be modified.
   * @param newState State to move the resulting tasks into.
   * @return the number of successful state changes.
   */
  @VisibleForTesting
  public synchronized int changeState(TaskQuery query, ScheduleStatus newState) {
    return changeState(query, stateUpdater(newState));
  }

  /**
   * Performs a simple state change, transitioning all tasks matching a query to the given
   * state and applying the given audit message.
   *
   * @param query Query to perform, the results of which will be modified.
   * @param newState State to move the resulting tasks into.
   * @param auditMessage Audit message to apply along with the state change.
   * @return the number of successful state changes.
   */
  synchronized int changeState(TaskQuery query, ScheduleStatus newState,
      @Nullable String auditMessage) {
    return changeState(query, stateUpdaterWithAuditMessage(newState, auditMessage));
  }

  /**
   * Assigns a task to a specific slave.
   * This will modify the task record to reflect the host assignment and return the updated record.
   *
   * @param taskId ID of the task to mutate.
   * @param slaveHost Host name that the task is being assigned to.
   * @param slaveId ID of the slave that the task is being assigned to.
   * @param assignedPorts Ports on the host that are being assigned to the task.
   * @return The updated task record, or {@code null} if the task was not found.
   */
  synchronized AssignedTask assignTask(String taskId, String slaveHost, SlaveID slaveId,
      Set<Integer> assignedPorts) {
    checkNotBlank(taskId);
    checkNotBlank(slaveHost);
    checkNotNull(assignedPorts);

    final AtomicReference<AssignedTask> returnValue = new AtomicReference<AssignedTask>();
    changeState(Query.byId(taskId), assignHost(slaveHost, slaveId, returnValue, assignedPorts));

    return returnValue.get();
  }

  /**
   * Fetches all tasks that match a query.
   *
   * @param query Query to perform.
   * @return A read-only view of the tasks matching the query.
   */
  public synchronized Set<ScheduledTask> fetchTasks(final TaskQuery query) {
    checkNotNull(query);
    managerState.checkState(ImmutableSet.of(State.INITIALIZED, State.STARTED));

    return transactionalStorage.doInTransaction(new Work.Quiet<Set<ScheduledTask>>() {
      @Override public Set<ScheduledTask> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getTaskStore().fetchTasks(query);
      }
    });
  }

  @VisibleForTesting
  final LoadingCache<ScheduleStatus, AtomicLong> maxOutstandingTimes =
      CacheBuilder.newBuilder().build(CacheLoader.from(
          new Function<ScheduleStatus, AtomicLong>() {
            @Override public AtomicLong apply(ScheduleStatus status) {
              return Stats.exportLong("scheduler_max_" + status + "_waiting_ms");
            }
          }));

  /**
   * Scans any outstanding tasks and attempts to kill any tasks that have timed out; additionally
   * reports stats on the maximum time a task has spent in each outstanding state.
   */
  public synchronized void scanOutstandingTasks() {
    managerState.checkState(State.STARTED);

    Set<ScheduledTask> outstandingTasks = fetchTasks(OUTSTANDING_TASK_QUERY);

    Multimap<ScheduleStatus, Long> stateDurations =
        Multimaps.transformValues(Multimaps.index(outstandingTasks, Tasks.GET_STATUS),
        latestStateDuration);
    Ordering<Long> ordering = Ordering.natural();

    for (ScheduleStatus status : OUTSTANDING_TASK_QUERY.getStatuses()) {
      long highest = Iterables.getLast(ordering.sortedCopy(stateDurations.get(status)), 0L);
      maxOutstandingTimes.getUnchecked(status).set(highest);
    }

    if (outstandingTasks.isEmpty()) {
      return;
    }

    LOG.info("Checking " + outstandingTasks.size() + " outstanding tasks.");
    Iterable<ScheduledTask> missingTasks = Iterables.filter(outstandingTasks, taskTimedOut);

    // Kill any timed out tasks.
    Set<String> missingTaskIds = Tasks.ids(missingTasks);
    for (String missingTaskId : missingTaskIds) {
      LOG.info("Attempting to kill missing task " + missingTaskId);
      driver.killTask(missingTaskId);
    }
    changeState(Query.byId(missingTaskIds), ScheduleStatus.LOST, "Task timed out.");
  }

  /**
   * Fetches the updated configuration of a shard.
   *
   * @param role Job owner.
   * @param job Job to fetch updated configs for.
   * @param shards Shards within a job to fetch.
   * @return The task information of the shard.
   */
  synchronized Set<TwitterTaskInfo> fetchUpdatedTaskConfigs(final String role, final String job,
      final Set<Integer> shards) {
    checkNotNull(role);
    checkNotNull(job);
    checkNotBlank(shards);
    managerState.checkState(ImmutableSet.of(State.INITIALIZED, State.STARTED));

    Set<TaskUpdateConfiguration> configs = transactionalStorage.doInTransaction(
        new Work.Quiet<Set<TaskUpdateConfiguration>>() {
          @Override public Set<TaskUpdateConfiguration> apply(StoreProvider storeProvider) {
            Optional<JobUpdateConfiguration> config =
                storeProvider.getUpdateStore().fetchJobUpdateConfig(role, job);
            return config.isPresent()
                ? fetchShardUpdateConfigs(config.get(), shards)
                : ImmutableSet.<TaskUpdateConfiguration>of();
          }
        });

    return ImmutableSet.copyOf(Iterables.transform(configs, Shards.GET_NEW_CONFIG));
  }

  /**
   * Instructs the state manager to abandon records of the provided tasks.  This is essentially
   * simulating an executor notifying the state manager of missing tasks.
   * Tasks will be deleted in this process.
   *
   * @param taskIds IDs of tasks to abandon.
   * @throws IllegalStateException If the manager is not in a state to service abandon requests.
   */
  synchronized void abandonTasks(final Set<String> taskIds) throws IllegalStateException {
    MorePreconditions.checkNotBlank(taskIds);
    managerState.checkState(State.STARTED);

    transactionalStorage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider storeProvider) {
        for (TaskStateMachine stateMachine : getStateMachines(taskIds).values()) {
          stateMachine.updateState(ScheduleStatus.UNKNOWN, "Dead executor.");
        }

        // Need to process the work queue first to ensure the tasks can be state changed prior
        // to deletion.
        processWorkQueueInTransaction(storeProvider);
        deleteTasks(taskIds);
      }
    });
  }

  private int changeStateInTransaction(
      Set<String> taskIds, Function<TaskStateMachine, Boolean> stateChange) {
    int count = 0;
    for (TaskStateMachine stateMachine : getStateMachines(taskIds).values()) {
      if (stateChange.apply(stateMachine)) {
        ++count;
      }
    }
    return count;
  }

  private int changeState(
      final TaskQuery query, final Function<TaskStateMachine, Boolean> stateChange) {
    return transactionalStorage.doInTransaction(new Work.Quiet<Integer>() {
      @Override public Integer apply(StoreProvider storeProvider) {
        return changeStateInTransaction(
            storeProvider.getTaskStore().fetchTaskIds(query), stateChange);
      }
    });
  }

  private static Function<TaskStateMachine, Boolean> stateUpdater(final ScheduleStatus state) {
    return new Function<TaskStateMachine, Boolean>() {
      @Override public Boolean apply(TaskStateMachine stateMachine) {
        return stateMachine.updateState(state);
      }
    };
  }

  private static Function<TaskStateMachine, Boolean> stateUpdaterWithAuditMessage(
      final ScheduleStatus state, @Nullable final String auditMessage) {
    return new Function<TaskStateMachine, Boolean>() {
      @Override public Boolean apply(TaskStateMachine stateMachine) {
        return stateMachine.updateState(state, auditMessage);
      }
    };
  }

  @ThermosJank
  private Function<TaskStateMachine, Boolean> assignHost(
      final String slaveHost, final SlaveID slaveId,
      final AtomicReference<AssignedTask> taskReference, final Set<Integer> assignedPorts) {
    final Closure<ScheduledTask> mutation = new Closure<ScheduledTask>() {
      @Override public void execute(ScheduledTask task) {
        AssignedTask assigned;
        TwitterTaskInfo info = task.getAssignedTask().getTask();
        // Length check is an artifact of thrift 0.5.0 NPE workaround from ConfigurationManager.
        // See MESOS-370.
        if (info.isSetThermosConfig() && (info.getThermosConfig().length > 0)) {
          assigned = task.getAssignedTask();
          assigned.setAssignedPorts(CommandLineExpander.getNameMappedPorts(
              assigned.getTask().getRequestedPorts(), assignedPorts));
        } else {
          assigned = CommandLineExpander.expand(task.getAssignedTask(), assignedPorts);
        }

        task.setAssignedTask(assigned);
        assigned.setSlaveHost(slaveHost)
            .setSlaveId(slaveId.getValue());
        Preconditions.checkState(
            taskReference.compareAndSet(null, assigned),
            "More than one result was found for an identity query.");
      }
    };

    return new Function<TaskStateMachine, Boolean>() {
      @Override public Boolean apply(final TaskStateMachine stateMachine) {
        return stateUpdaterWithMutation(ScheduleStatus.ASSIGNED, mutation).apply(stateMachine);
      }
    };
  }

  private static Function<TaskStateMachine, Boolean> stateUpdaterWithMutation(
      final ScheduleStatus state, final Closure<ScheduledTask> mutation) {
    return new Function<TaskStateMachine, Boolean>() {
      @Override public Boolean apply(TaskStateMachine stateMachine) {
        return stateMachine.updateState(state, mutation);
      }
    };
  }

  // Supplier that checks if there is an active update for a job.
  private Supplier<Boolean> taskUpdateChecker(final String role, final String job) {
    return new Supplier<Boolean>() {
      @Override public Boolean get() {
        return transactionalStorage.doInTransaction(new Work.Quiet<Boolean>() {
          @Override public Boolean apply(Storage.StoreProvider storeProvider) {
            return storeProvider.getUpdateStore().fetchJobUpdateConfig(role, job).isPresent();
          }
        });
      }
    };
  }

  /**
   * Creates a new task ID that is permanently unique (not guaranteed, but highly confident),
   * and by default sorts in chronological order.
   *
   * @param task Task that an ID is being generated for.
   * @return New task ID.
   */
  private String generateTaskId(TwitterTaskInfo task) {
    String sep = "-";
    return new StringBuilder()
        .append(clock.nowMillis())               // Allows chronological sorting.
        .append(sep)
        .append(jobKey(task))                    // Identification and collision prevention.
        .append(sep)
        .append(task.getShardId())               // Collision prevention within job.
        .append(sep)
        .append(UUID.randomUUID())               // Just-in-case collision prevention.
        .toString().replaceAll("[^\\w-]", sep);  // Constrain character set.
  }

  private void processWorkQueueInTransaction(StoreProvider storeProvider) {
    managerState.checkState(ImmutableSet.of(State.INITIALIZED, State.STARTED));
    for (final WorkEntry work : Iterables.consumingIterable(workQueue)) {
      final TaskStateMachine stateMachine = work.stateMachine;

      if (work.command == WorkCommand.KILL) {
        driver.killTask(stateMachine.getTaskId());
      } else {
        TaskStore taskStore = storeProvider.getTaskStore();
        String taskId = stateMachine.getTaskId();
        TaskQuery idQuery = Query.byId(taskId);

        switch (work.command) {
          case RESCHEDULE:
            ScheduledTask task =
                Iterables.getOnlyElement(taskStore.fetchTasks(idQuery)).deepCopy();
            task.getAssignedTask().unsetSlaveId();
            task.getAssignedTask().unsetSlaveHost();
            task.getAssignedTask().unsetAssignedPorts();
            ConfigurationManager.resetStartCommand(task.getAssignedTask().getTask());
            task.unsetTaskEvents();
            task.setAncestorId(taskId);
            String newTaskId = generateTaskId(task.getAssignedTask().getTask());
            task.getAssignedTask().setTaskId(newTaskId);

            LOG.info("Task being rescheduled: " + taskId);

            taskStore.saveTasks(ImmutableSet.of(task));

            createStateMachine(task).updateState(PENDING, "Rescheduled");
            break;

          case UPDATE:
          case ROLLBACK:
            maybeRescheduleForUpdate(storeProvider, taskId,
                work.command == WorkCommand.ROLLBACK);
            break;

          case UPDATE_STATE:
            taskStore.mutateTasks(idQuery, new Closure<ScheduledTask>() {
              @Override public void execute(ScheduledTask task) {
                task.setStatus(stateMachine.getState());
                work.mutation.execute(task);
              }
            });
            transactionalStorage.addSideEffect(new SideEffect() {
              @Override public void mutate(MutableState state) {
                state.getVars()
                    .adjustCount(stateMachine.getJobKey(), stateMachine.getPreviousState(),
                        stateMachine.getState());
              }
            });
            break;

          case DELETE:
            deleteTasks(ImmutableSet.of(taskId));
            break;

          case INCREMENT_FAILURES:
            taskStore.mutateTasks(idQuery, new Closure<ScheduledTask>() {
              @Override public void execute(ScheduledTask task) {
                task.setFailureCount(task.getFailureCount() + 1);
              }
            });
            break;

          default:
            LOG.severe("Unrecognized work command type " + work.command);
        }
      }
    }
  }

  /**
   * Deletes records of tasks from the task store.
   * This will not perform any state checking or state transitions, but will immediately remove
   * the tasks from the store.  It will also silently ignore attempts to delete task IDs that do
   * not exist.
   *
   * @param taskIds IDs of tasks to delete.
   */
  public synchronized void deleteTasks(final Set<String> taskIds) {
    transactionalStorage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(final StoreProvider storeProvider) {
        final TaskStore taskStore = storeProvider.getTaskStore();
        final Iterable<ScheduledTask> tasks = taskStore.fetchTasks(Query.byId(taskIds));

        transactionalStorage.addSideEffect(new SideEffect() {
          @Override public void mutate(MutableState state) {
            for (ScheduledTask task : tasks) {
              state.getVars().decrementCount(Tasks.jobKey(task), task.getStatus());
            }
          }
        });

        taskStore.removeTasks(taskIds);
      }
    });
  }

  private void maybeRescheduleForUpdate(
      StoreProvider storeProvider, String taskId, boolean rollingBack) {
    TaskStore taskStore = storeProvider.getTaskStore();

    TwitterTaskInfo oldConfig = Tasks.SCHEDULED_TO_INFO.apply(
        Iterables.getOnlyElement(taskStore.fetchTasks(Query.byId(taskId))));

    Optional<TaskUpdateConfiguration> optional = fetchShardUpdateConfig(
        storeProvider.getUpdateStore(),
        oldConfig.getOwner().getRole(),
        oldConfig.getJobName(),
        oldConfig.getShardId());

    // TODO(Sathya): Figure out a way to handle race condition when finish update is called
    //     before ROLLBACK

    if (!optional.isPresent()) {
      LOG.warning("No update configuration found for key " + Tasks.jobKey(oldConfig)
          + " shard " + oldConfig.getShardId() + " : Assuming update has finished.");
      return;
    }

    TaskUpdateConfiguration updateConfig = optional.get();
    TwitterTaskInfo newConfig =
        rollingBack ? updateConfig.getOldConfig() : updateConfig.getNewConfig();
    if (newConfig == null) {
      // The updated configuration removed the shard, nothing to reschedule.
      return;
    }

    ConfigurationManager.resetStartCommand(newConfig);
    ScheduledTask newTask = taskCreator.apply(newConfig).setAncestorId(taskId);
    taskStore.saveTasks(ImmutableSet.of(newTask));
    createStateMachine(newTask)
        .updateState(PENDING, "Rescheduled after " + (rollingBack ? "rollback." : "update."));
  }

  private Map<String, TaskStateMachine> getStateMachines(final Set<String> taskIds) {
    return transactionalStorage.doInTransaction(new Work.Quiet<Map<String, TaskStateMachine>>() {
      @Override public Map<String, TaskStateMachine> apply(StoreProvider storeProvider) {
        Set<ScheduledTask> tasks = storeProvider.getTaskStore().fetchTasks(Query.byId(taskIds));
        Map<String, ScheduledTask> existingTasks = Maps.uniqueIndex(
            tasks,
            new Function<ScheduledTask, String>() {
              @Override public String apply(ScheduledTask input) {
                return input.getAssignedTask().getTaskId();
              }
            });

        ImmutableMap.Builder<String, TaskStateMachine> builder = ImmutableMap.builder();
        for (String taskId : taskIds) {
          // Pass null get() values through.
          builder.put(taskId, getStateMachine(taskId, existingTasks.get(taskId)));
        }
        return builder.build();
      }
    });
  }

  private TaskStateMachine getStateMachine(String taskId, ScheduledTask task) {
    if (task != null) {
      String role = task.getAssignedTask().getTask().getOwner().getRole();
      String job = task.getAssignedTask().getTask().getJobName();

      return new TaskStateMachine(
          Tasks.id(task),
          Tasks.jobKey(task),
          task,
          taskUpdateChecker(role, job),
          workSink,
          taskTimedOut,
          clock,
          task.getStatus());
    }

    // The task is unknown, not present in storage.
    TaskStateMachine stateMachine = new TaskStateMachine(
        taskId,
        null,
        // The task is unknown, so there is no matching task to fetch.
        null,
        // Since the task doesn't exist, its job cannot be updating.
        Suppliers.ofInstance(false),
        workSink,
        taskTimedOut,
        clock,
        INIT);
    stateMachine.updateState(UNKNOWN);
    return stateMachine;
  }

  private TaskStateMachine createStateMachine(ScheduledTask task) {
    return createStateMachine(task, INIT);
  }

  private TaskStateMachine createStateMachine(
      final ScheduledTask task, final ScheduleStatus initialState) {
    final String taskId = Tasks.id(task);
    String role = task.getAssignedTask().getTask().getOwner().getRole();
    String job = task.getAssignedTask().getTask().getJobName();

    final String jobKey = Tasks.jobKey(task);
    TaskStateMachine stateMachine = new TaskStateMachine(
        taskId,
        jobKey,
        Iterables.getOnlyElement(fetchTasks(Query.byId(taskId))),
        taskUpdateChecker(role, job),
        workSink,
        taskTimedOut,
        clock,
        initialState);

    transactionalStorage.addSideEffect(new SideEffect() {
      @Override public void mutate(MutableState state) {
        state.getVars().incrementCount(jobKey, initialState);
      }
    });

    return stateMachine;
  }

}
