package com.twitter.mesos.scheduler;

import java.util.EnumSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.commons.lang.StringUtils;
import org.apache.mesos.Protos.SlaveID;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Closures;
import com.twitter.common.base.Command;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.collections.Pair;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;
import com.twitter.common.util.StateMachine;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;
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
import static com.twitter.mesos.scheduler.storage.UpdateStore.ShardUpdateConfiguration;

/**
 * Manager of all persistence-related operations for the scheduler.  Acts as a controller for
 * persisted state machine transitions, and their side-effects.
 *
 * @author William Farner
 */
class StateManager {

  private static final Logger LOG = Logger.getLogger(StateManager.class.getName());

  @VisibleForTesting
  @CmdLine(name = "missing_task_grace_period",
      help = "The amount of time after which to treat an ASSIGNED task as LOST.")
  static final Arg<Amount<Long, Time>> MISSING_TASK_GRACE_PERIOD =
      Arg.create(Amount.of(1L, Time.MINUTES));

  // State of the manager instance.
  private enum State {
    CREATED,
    INITIALIZED,
    STARTED,
    STOPPED
  }

  // Enforces lifecycle of the manager, ensuring proper call order.
  private final StateMachine<State> managerState = StateMachine.<State>builder("state_manager")
      .initialState(State.CREATED)
      .addState(State.CREATED, State.INITIALIZED)
      .addState(State.INITIALIZED, State.STARTED)
      .addState(State.STARTED, State.STOPPED)
      .build();

  // An item of work on the work queue.
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

  // Work queue to receive state machine side effect work.
  private final Queue<WorkEntry> workQueue = Lists.newLinkedList();

  // Adapt the work queue into a sink.
  private final TaskStateMachine.WorkSink workSink = new TaskStateMachine.WorkSink() {
      @Override public void addWork(WorkCommand work, TaskStateMachine stateMachine,
          Closure<ScheduledTask> mutation) {
        workQueue.add(new WorkEntry(work, stateMachine, mutation));
      }
    };

  private final Cache<String, TaskStateMachine> taskStateMachines =
      CacheBuilder.newBuilder().build(new CacheLoader<String, TaskStateMachine>() {
        @Override public TaskStateMachine load(String taskId) {
          return new TaskStateMachine(taskId,
              null,
              // The task is unknown, so there is no matching task to fetch.
              Suppliers.<ScheduledTask>ofInstance(null),
              // Since the task doesn't exist, its job cannot be updating.
              Suppliers.ofInstance(false),
              workSink,
              taskTimeoutFilter,
              clock,
              INIT)
              .updateState(UNKNOWN);
        }
      }
  );

  @VisibleForTesting
  final Function<TwitterTaskInfo, ScheduledTask> taskCreator =
      new Function<TwitterTaskInfo, ScheduledTask>() {
        @Override public ScheduledTask apply(TwitterTaskInfo task) {
          return new ScheduledTask()
              .setStatus(INIT)
              .setAssignedTask(new AssignedTask().setTaskId(generateTaskId(task)).setTask(task));
        }
      };

  private final Predicate<Iterable<TaskEvent>> taskTimeoutFilter;

  // Handles all scheduler persistence
  private final Storage storage;

  // Kills the task with the id passed into execute.
  private Closure<String> killTask;
  private final Clock clock;

  @Inject
  StateManager(Storage storage, final Clock clock) {
    this.storage = checkNotNull(storage);
    this.clock = checkNotNull(clock);
    this.taskTimeoutFilter = new Predicate<Iterable<TaskEvent>>() {
      @Override public boolean apply(Iterable<TaskEvent> events) {
        TaskEvent lastEvent = Iterables.getLast(events, null);
        if (lastEvent == null) {
          return true;
        } else {
          long lastEventAgeMillis = clock.nowMillis() - lastEvent.getTimestamp();
          return lastEventAgeMillis > MISSING_TASK_GRACE_PERIOD.get().as(Time.MILLISECONDS);
        }
      }
    };

    this.killTask = new Closure<String>() {
      @Override public void execute(String taskId) {
        LOG.log(Level.SEVERE, "Attempted to kill task " + taskId + " before registered.");
      }
    };

    Stats.exportSize("work_queue_depth", workQueue);
  }

  /**
   * Prompts the state manager to prepare for possible activation in the leading scheduler process.
   */
  void prepare() {
    storage.prepare();
  }

  /**
   * Initializes the state manager, by starting the storage and fetching the persisted framework ID.
   *
   * @return The persisted framework ID, or {@code null} if no framework ID exists in the store.
   */
  @Nullable
  synchronized String initialize() {
    managerState.transition(State.INITIALIZED);

    storage.start(new Work.NoResult.Quiet() {
      @Override protected void execute(Storage.StoreProvider storeProvider) {
        storeProvider.getTaskStore().mutateTasks(Query.GET_ALL, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask task) {
            ConfigurationManager.applyDefaultsIfUnset(task.getAssignedTask().getTask());
            createStateMachine(task, task.getStatus());
          }
        });
      }
    });

    LOG.info("Storage initialization complete.");
    return getFrameworkId();
  }

  private String getFrameworkId() {
    managerState.checkState(ImmutableSet.of(State.INITIALIZED, State.STARTED));

    return storage.doInTransaction(new Work<String, RuntimeException>() {
      @Override public String apply(Storage.StoreProvider storeProvider) {
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

    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(Storage.StoreProvider storeProvider) {
        storeProvider.getSchedulerStore().saveFrameworkId(frameworkId);
      }
    });
  }

  /**
   * Instructs the state manager to start, providing a callback that can be used to kill active
   * tasks.
   *
   * @param killTask Task killer callback.
   */
  synchronized void start(Closure<String> killTask) {
    managerState.transition(State.STARTED);

    this.killTask = checkNotNull(killTask);
  }

  /**
   * Instructs the state manager to stop, and shut down the backing storage.
   */
  synchronized void stop() {
    managerState.transition(State.STOPPED);

    storage.stop();
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

    transactionalWork(new Work.NoResult.Quiet() {
      @Override protected void execute(Storage.StoreProvider storeProvider) {
        storeProvider.getTaskStore().saveTasks(scheduledTasks);

        for (ScheduledTask task : scheduledTasks) {
          createStateMachine(task).updateState(PENDING);
        }
      }
    });

    return ImmutableSet.copyOf(Iterables.transform(scheduledTasks, Tasks.SCHEDULED_TO_ID));
  }

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

    return storage.doInTransaction(new Work<String, UpdateException>() {
      @Override public String apply(Storage.StoreProvider storeProvider) throws UpdateException {
        String jobKey = Tasks.jobKey(role, job);
        TaskStore taskStore = storeProvider.getTaskStore();
        Set<TwitterTaskInfo> existingTasks =
            ImmutableSet.copyOf(Iterables.transform(taskStore.fetchTasks(Query.activeQuery(jobKey)),
                Tasks.SCHEDULED_TO_INFO));

        if (existingTasks.isEmpty()) {
          throw new UpdateException("No active tasks found for job" + jobKey);
        }

        UpdateStore updateStore = storeProvider.getUpdateStore();
        if (updateStore.fetchShardUpdateConfig(role, job, 0) != null) {
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
        updateStore.saveShardUpdateConfigs(role, job, updateToken, shardConfigBuilder.build());
        return updateToken;
      }
    });
  }

  /**
   * Terminates an in-progress update.
   *
   * @param role Role owning the update to finish.
   * @param job Job to finish updating.
   * @param updateToken Token associated with the update.  If not present, the token must match the
   *     the stored token for the update.
   * @param result The result of the update.
   * @throws UpdateException If an update is not in-progress for the job, or the non-null token
   *     does not match the stored token.
   */
  synchronized void finishUpdate(final String role, final String job,
      final Optional<String> updateToken, final UpdateResult result) throws UpdateException {
    checkNotBlank(role);
    checkNotBlank(job);

    storage.doInTransaction(new NoResult<UpdateException>() {
      @Override protected void execute(Storage.StoreProvider storeProvider) throws UpdateException {
        UpdateStore updateStore = storeProvider.getUpdateStore();

        String jobKey = Tasks.jobKey(role, job);

        // Since we store all shards in a job with the same token, we can just check shard 0,
        // which is always guaranteed to exist for a job.
        UpdateStore.ShardUpdateConfiguration updateConfig =
            updateStore.fetchShardUpdateConfig(role, job, 0);
        if (updateConfig == null) {
          throw new UpdateException("Update does not exist for " + jobKey);
        }

        if ((updateToken.isPresent()) && !updateToken.get().equals(updateConfig.getUpdateToken())) {
          throw new UpdateException("Invalid update token for " + jobKey);
        }

        if (result == UpdateResult.SUCCESS) {
          for (Integer shard : fetchShardsToKill(role, job, updateStore)) {
            changeState(Query.liveShard(jobKey, shard), KILLING, "Removed during update.");
          }
        }

        updateStore.removeShardUpdateConfigs(role, job);
      }
    });
  }

  private static final Predicate<ShardUpdateConfiguration> SELECT_SHARDS_TO_KILL =
      new Predicate<ShardUpdateConfiguration>() {
        @Override public boolean apply(ShardUpdateConfiguration config) {
          return config.getNewConfig() == null;
        }
      };

  private final Function<ShardUpdateConfiguration, Integer> GET_ORIGINAL_SHARD_ID =
    new Function<ShardUpdateConfiguration, Integer>() {
      @Override public Integer apply(ShardUpdateConfiguration config) {
        return config.getOldConfig().getShardId();
      }
    };

  private Set<Integer> fetchShardsToKill(String role, String job, UpdateStore updateStore) {
    return ImmutableSet.copyOf(Iterables.transform(Iterables.filter(
        updateStore.fetchShardUpdateConfigs(role, job), SELECT_SHARDS_TO_KILL),
        GET_ORIGINAL_SHARD_ID));
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

    /**
     * Changes the state of tasks, using an empty audit message.
     *
     * @param taskIds IDs of the tasks to modify.
     * @param state New state to apply to the tasks.
     */
    void changeState(Set<String> taskIds, ScheduleStatus state);
  }

  /**
   * A mutation performed on the results of a query.
   */
  interface StateMutation<E extends Exception> {
    void execute(Set<ScheduledTask> tasks, StateChanger changer) throws E;

    /**
     * A state mutation that does not throw a checked exception.
     */
    interface Quiet extends StateMutation<RuntimeException> {}
  }

  /**
   * Performs an operation on the state based on a fixed query.
   *
   * @param taskQuery The query to perform, whose tasks will be made available to the callback via
   *    the provided {@link StateChanger}.  If the query is {@code null}, no initial query will be
   *    performed, and the {@code tasks} argument to
   *    {@link StateMutation#execute(Set, StateChanger)} will be {@code null}.
   * @param operation Operation to be performed.
   * @param <E> Type of exception thrown by the state change.
   * @throws E If the operation fails.
   */
  synchronized <E extends Exception> void taskOperation(@Nullable final Query taskQuery,
      final StateMutation<E> operation) throws E {
    checkNotNull(operation);

    transactionalWork(new NoResult<E>() {
      @Override
      protected void execute(Storage.StoreProvider storeProvider) throws E {
        Set<ScheduledTask> tasks = (taskQuery == null)
            ? null : storeProvider.getTaskStore().fetchTasks(taskQuery);
        operation.execute(tasks, new StateChanger() {
          @Override
          public void changeState(Set<String> taskIds, ScheduleStatus state,
              String auditMessage) {
            changeStateInTransaction(taskIds,
                stateUpdaterWithAuditMessage(state, auditMessage));
          }

          @Override
          public void changeState(Set<String> taskIds, ScheduleStatus state) {
            changeStateInTransaction(taskIds, stateUpdater(state));
          }
        });
      }
    });
  }

  /**
   * Convenience method to {@link #taskOperation(Query, StateMutation)} with a {@code null} query.
   *
   * @param operation Operation to be performed.
   * @param <E> Type of exception thrown by the state change.
   * @throws E If the operation fails.
   */
  synchronized <E extends Exception> void taskOperation(StateMutation<E> operation) throws E {
    taskOperation(null, operation);
  }

  /**
   * Performs a simple state change, transitioning all tasks matching a query to the given
   * state.
   * No audit message will be applied with the transition.
   *
   * @param taskQuery Query to perform, the results of which will be modified.
   * @param newState State to move the resulting tasks into.
   */
  synchronized void changeState(Query taskQuery, ScheduleStatus newState) {
    changeState(taskQuery, stateUpdater(newState));
  }

  /**
   * Performs a simple state change, transitioning all tasks matching a query to the given
   * state and applying the given audit message.
   *
   * @param taskQuery Query to perform, the results of which will be modified.
   * @param newState State to move the resulting tasks into.
   * @param auditMessage Audit message to apply along with the state change.
   */
  synchronized void changeState(Query taskQuery, ScheduleStatus newState,
      @Nullable String auditMessage) {
    changeState(taskQuery, stateUpdaterWithAuditMessage(newState, auditMessage));
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
  synchronized Set<ScheduledTask> fetchTasks(final Query query) {
    checkNotNull(query);
    managerState.checkState(ImmutableSet.of(State.INITIALIZED, State.STARTED));

    return storage.doInTransaction(new Work.Quiet<Set<ScheduledTask>>() {
      @Override public Set<ScheduledTask> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getTaskStore().fetchTasks(query);
      }
    });
  }

  private static final Query OUTSTANDING_TASK_QUERY = new Query(new TaskQuery().setStatuses(
      EnumSet.of(
          ScheduleStatus.ASSIGNED,
          ScheduleStatus.STARTING,
          ScheduleStatus.PREEMPTING,
          ScheduleStatus.RESTARTING,
          ScheduleStatus.KILLING)));

  /**
   * Scans any outstanding tasks and attempts to kill any tasks that have timed out.
   */
  synchronized void scanOutstandingTasks() {
    managerState.checkState(State.STARTED);

    Set<ScheduledTask> outstandingTasks = fetchTasks(OUTSTANDING_TASK_QUERY);
    if (outstandingTasks.isEmpty()) {
      return;
    }
    LOG.info("Checking " + outstandingTasks.size() + " outstanding tasks.");
    Predicate<ScheduledTask> missingTaskFilter =
        Predicates.compose(taskTimeoutFilter, Tasks.GET_TASK_EVENTS);
    Iterable<ScheduledTask> missingTasks = Iterables.filter(outstandingTasks, missingTaskFilter);

    // Kill any timed out tasks.  This assumes that the mesos core will send a TASK_LOST status
    // update if we attempt to kill any tasks that the core has no knowledge of.
    for (String missingTaskId : Iterables.transform(missingTasks, Tasks.SCHEDULED_TO_ID)) {
      LOG.info("Attempting to kill missing task " + missingTaskId);
      killTask.execute(missingTaskId);
    }
  }

  /**
   * Fetches the IDs of all tasks that match a query.
   *
   * @param query Query to perform.
   * @return The IDs of all tasks matching the query.
   */
  synchronized Set<String> fetchTaskIds(final Query query) {
    checkNotNull(query);
    managerState.checkState(ImmutableSet.of(State.INITIALIZED, State.STARTED));

    return storage.doInTransaction(new Work.Quiet<Set<String>>() {
      @Override
      public Set<String> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getTaskStore().fetchTaskIds(query);
      }
    });
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

    Set<ShardUpdateConfiguration> configs =
        storage.doInTransaction(new Work.Quiet<Set<ShardUpdateConfiguration>>() {
      @Override public Set<ShardUpdateConfiguration> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getUpdateStore().fetchShardUpdateConfigs(role, job, shards);
      }
    });

    return ImmutableSet.copyOf(Iterables.transform(configs, Shards.GET_NEW_CONFIG));
  }

  private static final Function<TaskStateMachine, String> GET_TASK_ID =
      new Function<TaskStateMachine, String>() {
        @Override public String apply(TaskStateMachine stateMachine) {
          return stateMachine.getTaskId();
        }
      };

  private static final Function<TaskStateMachine, String> GET_HOST =
      new Function<TaskStateMachine, String>() {
        @Override public String apply(TaskStateMachine stateMachine) {
          return stateMachine.getSlaveHost();
        }
      };

  private static final Predicate<TaskStateMachine> HAS_HOST =
      Predicates.compose(Predicates.notNull(), GET_HOST);

  /**
   * Generates a mapping from slave hostname to task IDs, including only tasks that have been
   * assigned to a host.
   *
   * @return Map from slave hosts to task IDs.
   */
  synchronized Multimap<String, String> getHostAssignedTasks() {
    return Multimaps.transformValues(
        Multimaps.index(Iterables.filter(taskStateMachines.asMap().values(), HAS_HOST), GET_HOST),
        GET_TASK_ID);
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

    processWorkQueueAfter(new Command() {
      @Override public void execute() {
        for (String taskId : taskIds) {
          TaskStateMachine stateMachine = taskStateMachines.getUnchecked(taskId);
          if (stateMachine != null) {
            stateMachine.updateState(ScheduleStatus.UNKNOWN, "Dead executor.");
          } else {
            LOG.severe("Unknown task ID " + taskId);
          }
        }
      }
    });

    storage.doInTransaction(
        new Work.NoResult.Quiet() {
          @Override protected void execute(StoreProvider storeProvider) {
            deleteTasks(storeProvider.getTaskStore(), taskIds);
          }
        }
    );
  }

  private <T, E extends Exception> void transactionalWork(final Work<T, E> work) throws E {
    processWorkQueueAfter(new ExceptionalCommand<E>() {
      @Override public void execute() throws E {
        storage.doInTransaction(work);
      }
    });
  }

  private void changeStateInTransaction(Set<String> taskIds,
      final Closure<TaskStateMachine> stateChange) {
    for (String taskId : taskIds) {
      stateChange.execute(taskStateMachines.getUnchecked(taskId));
    }
  }

  private void changeState(final Query taskQuery, final Closure<TaskStateMachine> stateChange) {
    transactionalWork(new Work.NoResult.Quiet() {
      @Override protected void execute(Storage.StoreProvider storeProvider) {
        changeStateInTransaction(storeProvider.getTaskStore().fetchTaskIds(taskQuery), stateChange);
      }
    });
  }

  private static Closure<TaskStateMachine> stateUpdater(final ScheduleStatus state) {
    return new Closure<TaskStateMachine>() {
      @Override public void execute(TaskStateMachine stateMachine) {
        stateMachine.updateState(state);
      }
    };
  }

  private static Closure<TaskStateMachine> stateUpdaterWithAuditMessage(final ScheduleStatus state,
      final @Nullable String auditMessage) {
    return new Closure<TaskStateMachine>() {
      @Override public void execute(TaskStateMachine stateMachine) {
        stateMachine.updateState(state, auditMessage);
      }
    };
  }

  @ThermosJank
  @SuppressWarnings("unchecked")
  private static Closure<TaskStateMachine> assignHost(final String slaveHost,
      final SlaveID slaveId, final AtomicReference<AssignedTask> taskReference,
      final Set<Integer> assignedPorts) {
    Closure<ScheduledTask> mutation = new Closure<ScheduledTask>() {
      @Override public void execute(ScheduledTask task) {
        AssignedTask assigned;
        if (task.getAssignedTask().getTask().isSetThermosConfig()) {
          assigned = task.getAssignedTask();
        } else {
          assigned = CommandLineExpander.expand(task.getAssignedTask(),
              assignedPorts);
        }

        task.setAssignedTask(assigned);
        assigned.setSlaveHost(slaveHost)
            .setSlaveId(slaveId.getValue());
        Preconditions.checkState(
            taskReference.compareAndSet(null, assigned),
            "More than one result was found for an identity query.");
      }
    };

    return Closures.combine(
        stateUpdaterWithMutation(ScheduleStatus.ASSIGNED, mutation),
        new Closure<TaskStateMachine>() {
          @Override public void execute(TaskStateMachine stateMachine) {
            stateMachine.setSlaveHost(slaveHost);
          }
        });
  }

  private static Closure<TaskStateMachine> stateUpdaterWithMutation(final ScheduleStatus state,
      final Closure<ScheduledTask> mutation) {
    return new Closure<TaskStateMachine>() {
      @Override public void execute(TaskStateMachine stateMachine) {
        stateMachine.updateState(state, mutation);
      }
    };
  }

  private Supplier<ScheduledTask> taskSupplier(final String taskId) {
    return new Supplier<ScheduledTask>() {
      @Override public ScheduledTask get() {
        return Iterables.getOnlyElement(fetchTasks(Query.byId(taskId)));
      }
    };
  }

  // Supplier that checks if there is an active update for a job.
  private Supplier<Boolean> taskUpdateChecker(final String role, final String job) {
    return new Supplier<Boolean>() {
      @Override public Boolean get() {
        return storage.doInTransaction(new Work.Quiet<Boolean>() {
          @Override public Boolean apply(Storage.StoreProvider storeProvider) {
            return storeProvider.getUpdateStore().fetchShardUpdateConfig(role, job, 0) != null;
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
    return new StringBuilder()
        .append(clock.nowMillis())               // Allows chronological sorting.
        .append("-")
        .append(jobKey(task))                    // Identification and collision prevention.
        .append("-")
        .append(task.getShardId())               // Collision prevention within job.
        .append("-")
        .append(UUID.randomUUID())               // Just-in-case collision prevention.
        .toString().replaceAll("[^\\w-]", "-");  // Constrain character set.
  }

  private <E extends Exception> void processWorkQueueAfter(ExceptionalCommand<E> command) throws E {
    managerState.checkState(State.STARTED);
    command.execute();

    for (final WorkEntry work : Iterables.consumingIterable(workQueue)) {
      final TaskStateMachine stateMachine = work.stateMachine;

      if (work.command == WorkCommand.KILL) {
        killTask.execute(stateMachine.getTaskId());
      } else {
        storage.doInTransaction(new Work.NoResult.Quiet() {
          @Override protected void execute(Storage.StoreProvider storeProvider) {
            TaskStore taskStore = storeProvider.getTaskStore();
            String taskId = stateMachine.getTaskId();
            Query idQuery = Query.byId(taskId);

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
                vars.adjustCount(stateMachine.getJobKey(), stateMachine.getPreviousState(),
                    stateMachine.getState());
                break;

              case DELETE:
                deleteTasks(taskStore, ImmutableSet.of(taskId));
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
        });
      }
    }
  }

  private void deleteTasks(TaskStore taskStore, Set<String> taskIds) {
    for (ScheduledTask task
        : taskStore.fetchTasks(new Query(new TaskQuery().setTaskIds(taskIds)))) {
      vars.decrementCount(Tasks.jobKey(task), task.getStatus());
    }

    taskStore.removeTasks(taskIds);
    taskStateMachines.asMap().keySet().removeAll(taskIds);
  }

  private void maybeRescheduleForUpdate(Storage.StoreProvider storeProvider, String taskId,
      boolean rollingBack) {
    TaskStore taskStore = storeProvider.getTaskStore();

    TwitterTaskInfo oldConfig = Tasks.SCHEDULED_TO_INFO.apply(
        Iterables.getOnlyElement(taskStore.fetchTasks(Query.byId(taskId))));

    UpdateStore.ShardUpdateConfiguration updateConfig = storeProvider.getUpdateStore()
        .fetchShardUpdateConfig(oldConfig.getOwner().getRole(), oldConfig.getJobName(),
            oldConfig.getShardId());

    // TODO(Sathya): Figure out a way to handle race condition when finish update is called
    //     before ROLLBACK

    if (updateConfig == null) {
      LOG.warning("No update configuration found for key " + Tasks.jobKey(oldConfig)
          + " shard " + oldConfig.getShardId() + " : Assuming update has finished.");
      return;
    }

    TwitterTaskInfo newConfig =
        rollingBack ? updateConfig.getOldConfig() : updateConfig.getNewConfig();
    if (newConfig == null) {
      // The updated configuration removed the shard, nothing to reschedule.
      return;
    }

    ScheduledTask newTask = taskCreator.apply(newConfig).setAncestorId(taskId);
    taskStore.saveTasks(ImmutableSet.of(newTask));
    createStateMachine(newTask)
        .updateState(PENDING, "Rescheduled after " + (rollingBack ? "rollback." : "update."));
  }

  private TaskStateMachine createStateMachine(ScheduledTask task) {
    return createStateMachine(task, INIT);
  }

  private TaskStateMachine createStateMachine(ScheduledTask task, ScheduleStatus initialState) {
    String taskId = Tasks.id(task);
    String role = task.getAssignedTask().getTask().getOwner().getRole();
    String job = task.getAssignedTask().getTask().getJobName();

    String jobKey = Tasks.jobKey(task);
    vars.incrementCount(jobKey, initialState);
    TaskStateMachine stateMachine = new TaskStateMachine(
        taskId,
        jobKey,
        taskSupplier(taskId),
        taskUpdateChecker(role, job),
        workSink,
        taskTimeoutFilter,
        clock,
        initialState);

    String slaveHost = task.getAssignedTask().getSlaveHost();
    if (!StringUtils.isBlank(slaveHost)) {
      stateMachine.setSlaveHost(slaveHost);
    }

    taskStateMachines.asMap().put(taskId, stateMachine);
    return stateMachine;
  }

  private final class Vars {
    private final Cache<Pair<String, ScheduleStatus>, AtomicLong> countersByJobKeyAndStatus =
        CacheBuilder.newBuilder().build(new CacheLoader<Pair<String,ScheduleStatus>, AtomicLong>() {
              @Override public AtomicLong load(Pair<String, ScheduleStatus> jobAndStatus) {
                String jobKey = jobAndStatus.getFirst();
                ScheduleStatus status = jobAndStatus.getSecond();
                return Stats.exportLong("job_" + jobKey + "_tasks_" + status.name());
              }
            }
        );

    private final Cache<ScheduleStatus, AtomicLong> countersByStatus =
        CacheBuilder.newBuilder().build(new CacheLoader<ScheduleStatus, AtomicLong>() {
              @Override public AtomicLong load(ScheduleStatus status) {
                return Stats.exportLong("task_store_" + status);
              }
            }
        );

    Vars() {
      // Initialize by-status counters.
      for (ScheduleStatus status : ScheduleStatus.values()) {
        countersByStatus.getUnchecked(status);
      }

      Stats.exportSize("num_task_state_machines", taskStateMachines);
    }

    void incrementCount(String jobKey, ScheduleStatus status) {
      countersByStatus.getUnchecked(status).incrementAndGet();
      getCounter(jobKey, status).incrementAndGet();
    }

    void decrementCount(String jobKey, ScheduleStatus status) {
      countersByStatus.getUnchecked(status).decrementAndGet();
      getCounter(jobKey, status).decrementAndGet();
    }

    void adjustCount(String jobKey, ScheduleStatus oldStatus, ScheduleStatus newStatus) {
      decrementCount(jobKey, oldStatus);
      incrementCount(jobKey, newStatus);
    }

    AtomicLong getCounter(String jobKey, ScheduleStatus status) {
      return countersByJobKeyAndStatus.getUnchecked(Pair.of(jobKey, status));
    }
  }
  private final Vars vars = new Vars();

  @VisibleForTesting
  long getTaskCounter(String jobKey, ScheduleStatus status) {
    return vars.getCounter(jobKey, status).get();
  }
}
