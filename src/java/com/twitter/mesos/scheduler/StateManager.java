package com.twitter.mesos.scheduler;

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Closure;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;
import com.twitter.common.util.StateMachine;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.Storage.Work.NoResult;
import com.twitter.mesos.scheduler.storage.StorageRole;
import com.twitter.mesos.scheduler.storage.StorageRole.Role;
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

  private final Map<String, TaskStateMachine> taskStateMachines = new MapMaker().makeComputingMap(
      new Function<String, TaskStateMachine>() {
        @Override public TaskStateMachine apply(String taskId) {
          return new TaskStateMachine(taskId,
              // The task is unknown, so there is no matching task to fetch.
              Suppliers.<ScheduledTask>ofInstance(null),
              // Since the task doesn't exist, its job cannot be updating.
              Suppliers.ofInstance(false),
              workSink,
              MISSING_TASK_GRACE_PERIOD.get())
              .updateState(UNKNOWN);
        }
      }
  );

  private final Function<TwitterTaskInfo, ScheduledTask> taskCreator =
      new Function<TwitterTaskInfo, ScheduledTask>() {
        @Override public ScheduledTask apply(TwitterTaskInfo task) {
          return new ScheduledTask()
              .setStatus(INIT)
              .setAssignedTask(new AssignedTask().setTaskId(generateTaskId(task)).setTask(task));
        }
      };

  // Handles all scheduler persistence
  private final Storage storage;

  // Kills the task with the id passed into execute.
  private Closure<String> killTask;
  private final Clock clock;

  @Inject
  StateManager(@StorageRole(Role.Primary) Storage storage, Clock clock) {
    this.clock = checkNotNull(clock);
    this.storage = checkNotNull(storage);

    Stats.export(new StatImpl<Integer>("work_queue_depth") {
      @Override public Integer read() {
        return workQueue.size();
      }
    });
  }

  private static Predicate<TaskStateMachine> hasStatus(final ScheduleStatus status) {
    return new Predicate<TaskStateMachine>() {
      @Override public boolean apply(TaskStateMachine stateMachine) {
        return stateMachine.getState() == status;
      }
    };
  }

  /**
   * Initializes the state manager, by starting the storage and fetching the persisted framework ID.
   *
   * @return The persisted framework ID, or {@code null} if no framework ID exists in the store.
   */
  @Nullable
  String initialize() {
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

    // Export count of tasks in each state.
    for (final ScheduleStatus status : ScheduleStatus.values()) {
      Stats.export(new StatImpl<Integer>("task_store_" + status) {
        @Override public Integer read() {
          return Iterables.size(Iterables.filter(taskStateMachines.values(), hasStatus(status)));
        }
      });
    }

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
  void setFrameworkId(final String frameworkId) {
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
  void start(Closure<String> killTask) {
    managerState.transition(State.STARTED);

    this.killTask = checkNotNull(killTask);
  }

  /**
   * Instructs the state manager to stop, and shut down the backing storage.
   */
  void stop() {
    managerState.transition(State.STOPPED);

    storage.stop();
  }

  /**
   * Inserts new tasks into the store.
   *
   * @param tasks Tasks to insert.
   * @return Generated task IDs for the tasks inserted.
   */
  Set<String> insertTasks(Set<TwitterTaskInfo> tasks) {
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
   * @param jobKey Key of the job update to update.
   * @param updatedTasks Updated Task information to be registered.
   * @throws UpdateException If no active tasks are found for the job, or if an update for the job
   *     is already in progress.
   * @return A unique string identifying the update.
   */
  String registerUpdate(final String jobKey, final Set<TwitterTaskInfo> updatedTasks)
      throws UpdateException {
    checkNotBlank(jobKey);
    checkNotBlank(updatedTasks);

    return storage.doInTransaction(new Work<String, UpdateException>() {
      @Override public String apply(Storage.StoreProvider storeProvider) throws UpdateException {
        TaskStore taskStore = storeProvider.getTaskStore();
        Set<TwitterTaskInfo> existingTasks =
            ImmutableSet.copyOf(Iterables.transform(taskStore.fetchTasks(Query.activeQuery(jobKey)),
                Tasks.SCHEDULED_TO_INFO));

        if (existingTasks.isEmpty()) {
          throw new UpdateException("No active tasks found for job" + jobKey);
        }

        UpdateStore updateStore = storeProvider.getUpdateStore();
        if (updateStore.fetchShardUpdateConfig(jobKey, 0) != null) {
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
        updateStore.saveShardUpdateConfigs(jobKey, updateToken, shardConfigBuilder.build());
        return updateToken;
      }
    });
  }

  /**
   * Terminates an in-progress update.
   *
   * @param jobKey Key of the job update to terminate.
   * @param updateToken Token associated with the update.  If non-null, the token must match the
   *     the stored token for the update.
   * @param result The result of the update.
   * @throws UpdateException If an update is not in-progress for the job, or the non-null token
   *     does not match the stored token.
   */
  void finishUpdate(final String jobKey, @Nullable final String updateToken,
      final UpdateResult result) throws UpdateException{
    checkNotBlank(jobKey);

    storage.doInTransaction(new NoResult<UpdateException>() {
      @Override protected void execute(Storage.StoreProvider storeProvider) throws UpdateException {
        UpdateStore updateStore = storeProvider.getUpdateStore();

        // Since we store all shards in a job with the same token, we can just check shard 0,
        // which is always guaranteed to exist for a job.
        UpdateStore.ShardUpdateConfiguration updateConfig =
            updateStore.fetchShardUpdateConfig(jobKey, 0);
        if (updateConfig == null) {
          throw new UpdateException("Update does not exist for " + jobKey);
        }

        if ((updateToken != null) && !updateToken.equals(updateConfig.getUpdateToken())) {
          throw new UpdateException("Invalid update token for " + jobKey);
        }

        if (result == UpdateResult.SUCCESS) {
          for (Integer shard : fetchShardsToKill(jobKey, updateStore)) {
            changeState(Query.liveShard(jobKey, shard), KILLING, "Removed during update.");
          }
        }

        updateStore.removeShardUpdateConfigs(jobKey);
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

  private Set<Integer> fetchShardsToKill(String jobKey, UpdateStore updateStore) {
    return ImmutableSet.copyOf(Iterables.transform(Iterables.filter(
        updateStore.fetchShardUpdateConfigs(jobKey), SELECT_SHARDS_TO_KILL),
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
  <E extends Exception> void taskOperation(@Nullable final Query taskQuery,
      final StateMutation<E> operation) throws E {
    checkNotNull(operation);

    transactionalWork(new NoResult<E>() {
      @Override protected void execute(Storage.StoreProvider storeProvider) throws E {
        Set<ScheduledTask> tasks = (taskQuery == null)
            ? null : storeProvider.getTaskStore().fetchTasks(taskQuery);
        operation.execute(tasks, new StateChanger() {
          @Override public void changeState(Set<String> taskIds, ScheduleStatus state,
              String auditMessage) {
            changeStateInTransaction(taskIds,
                stateUpdaterWithAuditMessage(state, auditMessage));
          }

          @Override public void changeState(Set<String> taskIds, ScheduleStatus state) {
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
  <E extends Exception> void taskOperation(StateMutation<E> operation) throws E {
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
  void changeState(Query taskQuery, ScheduleStatus newState) {
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
  void changeState(Query taskQuery, ScheduleStatus newState, String auditMessage) {
    changeState(taskQuery, stateUpdaterWithAuditMessage(newState, auditMessage));
  }

  /**
   * Performs a state change on at most one task.
   * If a result of {@code taskQuery} is found, {@code mutation} will be called with the result,
   * and the return value from {@code mutation} will be returned, or {@code null} if no result was
   * found.
   * If multiple results are found from {@code taskQuery}, {@link IllegalStateException} will be
   * thrown.
   *
   * @param taskQuery Query to perform, the results of which will be modified.
   * @param newState State to move the resulting tasks into.
   * @param mutation Mutate operation to execute on the query result, if one exists.
   * @param <T> The function return type.
   * @return The return value from {@code mutation}.
   */
  <T> T changeState(Query taskQuery, ScheduleStatus newState,
      final Function<ScheduledTask, T> mutation) {
    checkNotNull(taskQuery);
    checkNotNull(newState);
    checkNotNull(mutation);

    final AtomicReference<T> returnValue = new AtomicReference<T>();
    changeState(taskQuery, stateUpdaterWithMutation(newState, new Closure<ScheduledTask>() {
      @Override public void execute(ScheduledTask task) {
        Preconditions.checkState(
            returnValue.compareAndSet(null, mutation.apply(task)),
            "More than one result was found for an identity query.");
      }
    }));

    return returnValue.get();
  }

  /**
   * Fetches all tasks that match a query.
   *
   * @param query Query to perform.
   * @return A read-only view of the tasks matching the query.
   */
  Set<ScheduledTask> fetchTasks(final Query query) {
    checkNotNull(query);
    managerState.checkState(ImmutableSet.of(State.INITIALIZED, State.STARTED));

    return storage.doInTransaction(new Work.Quiet<Set<ScheduledTask>>() {
      @Override public Set<ScheduledTask> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getTaskStore().fetchTasks(query);
      }
    });
  }

  /**
   * Fetches the IDs of all tasks that match a query.
   *
   * @param query Query to perform.
   * @return The IDs of all tasks matching the query.
   */
  Set<String> fetchTaskIds(final Query query) {
    checkNotNull(query);
    managerState.checkState(ImmutableSet.of(State.INITIALIZED, State.STARTED));

    return storage.doInTransaction(new Work.Quiet<Set<String>>() {
      @Override
      public Set<String> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getTaskStore().fetchTaskIds(query);
      }
    });
  }

  private static final Function<ShardUpdateConfiguration, TwitterTaskInfo> GET_NEW_CONFIG =
      new Function<ShardUpdateConfiguration, TwitterTaskInfo>() {
        @Override public TwitterTaskInfo apply(ShardUpdateConfiguration updateConfig) {
          return updateConfig.getNewConfig();
        }
      };

  /**
   * Fetches the updated configuration of a shard.
   *
   * @param jobKey Key of the job to fetch.
   * @param shards Shards within a job to fetch.
   * @return The task information of the shard.
   */
  Set<TwitterTaskInfo> fetchUpdatedTaskConfigs(final String jobKey, final Set<Integer> shards) {
    checkNotNull(jobKey);
    checkNotBlank(shards);

    Set<ShardUpdateConfiguration> configs =
        storage.doInTransaction(new Work.Quiet<Set<ShardUpdateConfiguration>>() {
      @Override public Set<ShardUpdateConfiguration> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getUpdateStore().fetchShardUpdateConfigs(jobKey, shards);
      }
    });

    return ImmutableSet.copyOf(Iterables.transform(configs, GET_NEW_CONFIG));
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
      stateChange.execute(taskStateMachines.get(taskId));
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
      final String auditMessage) {
    return new Closure<TaskStateMachine>() {
      @Override public void execute(TaskStateMachine stateMachine) {
        stateMachine.updateState(state, auditMessage);
      }
    };
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
  private Supplier<Boolean> taskUpdateChecker(final String jobKey) {
    return new Supplier<Boolean>() {
      @Override public Boolean get() {
        return storage.doInTransaction(new Work.Quiet<Boolean>() {
          @Override public Boolean apply(Storage.StoreProvider storeProvider) {
            return storeProvider.getUpdateStore().fetchShardUpdateConfig(jobKey, 0) != null;
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
                break;

              case DELETE:
                taskStore.removeTasks(ImmutableSet.of(taskId));
                taskStateMachines.remove(taskId);
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

  private void maybeRescheduleForUpdate(Storage.StoreProvider storeProvider, String taskId,
      boolean rollingBack) {
    TaskStore taskStore = storeProvider.getTaskStore();

    TwitterTaskInfo oldConfig = Tasks.SCHEDULED_TO_INFO.apply(
        Iterables.getOnlyElement(taskStore.fetchTasks(Query.byId(taskId))));

    UpdateStore.ShardUpdateConfiguration updateConfig = storeProvider.getUpdateStore()
        .fetchShardUpdateConfig(Tasks.jobKey(oldConfig), oldConfig.getShardId());

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
    return createStateMachine(task, null);
  }

  // Maintain a mapping from job key to task ID.  When a job key is first inserted,
  // a stat per ScheduleStatus for the job is exported.
  private final Multimap<String, String> jobKeysToTaskIds = HashMultimap.create();
  private void exportCounters(final String jobKey, String taskId) {
    if (!jobKeysToTaskIds.containsKey(jobKey)) {
      // Export count of tasks in each state for the job.
      for (final ScheduleStatus status : ScheduleStatus.values()) {
        Stats.export(new StatImpl<Integer>("job_" + jobKey + "_tasks_" + status.name()) {
          @Override public Integer read() {
            Map<String, TaskStateMachine> jobStateMachines =
                Maps.filterKeys(taskStateMachines, Predicates.in(jobKeysToTaskIds.get(jobKey)));
            return Iterables.size(Iterables.filter(jobStateMachines.values(), hasStatus(status)));
          }
        });
      }
    }
    jobKeysToTaskIds.put(jobKey, taskId);
  }

  private TaskStateMachine createStateMachine(ScheduledTask task,
      @Nullable ScheduleStatus initialState) {
    String taskId = Tasks.id(task);
    String jobKey = Tasks.jobKey(task);
    TaskStateMachine stateMachine =
        initialState == null
            ? new TaskStateMachine(
                taskId,
                taskSupplier(taskId),
                taskUpdateChecker(jobKey),
                workSink,
                MISSING_TASK_GRACE_PERIOD.get())
            : new TaskStateMachine(
                taskId,
                taskSupplier(taskId),
                taskUpdateChecker(jobKey),
                workSink,
                MISSING_TASK_GRACE_PERIOD.get(),
                initialState);

    exportCounters(jobKey, taskId);
    taskStateMachines.put(taskId, stateMachine);
    return stateMachine;
  }
}
