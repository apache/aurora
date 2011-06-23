package com.twitter.mesos.scheduler;

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.inject.Inject;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Closure;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;
import com.twitter.common.util.StateMachine;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.Storage.Work.NoResult;
import com.twitter.mesos.scheduler.storage.StorageRole;
import com.twitter.mesos.scheduler.storage.StorageRole.Role;
import com.twitter.mesos.scheduler.storage.TaskStore;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.twitter.mesos.Tasks.jobKey;
import static com.twitter.mesos.gen.ScheduleStatus.INIT;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.UNKNOWN;

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
          return new TaskStateMachine(taskId, Suppliers.<ScheduledTask>ofInstance(null), workSink,
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
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {

        taskStore.mutate(Query.GET_ALL, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask task) {
            ConfigurationManager.applyDefaultsIfUnset(task.getAssignedTask().getTask());

            String taskId = Tasks.id(task);
            taskStateMachines.put(taskId,
                new TaskStateMachine(taskId, taskSupplier(taskId),
                    workSink, MISSING_TASK_GRACE_PERIOD.get(), task.getStatus()));
          }
        });
      }
    });
    LOG.info("Initialization of DbStorage complete.");
    return getFrameworkId();
  }

  private String getFrameworkId() {
    managerState.checkState(ImmutableSet.of(State.INITIALIZED, State.STARTED));

    return storage.doInTransaction(new Work<String, RuntimeException>() {
      @Override public String apply(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) throws RuntimeException {
        return schedulerStore.fetchFrameworkId();
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
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {
        schedulerStore.saveFrameworkId(frameworkId);
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
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {
        taskStore.add(scheduledTasks);

        for (ScheduledTask task : scheduledTasks) {
          String taskId = Tasks.id(task);

          taskStateMachines.put(taskId,
              new TaskStateMachine(taskId, taskSupplier(taskId),
                  workSink, MISSING_TASK_GRACE_PERIOD.get())
                  .updateState(PENDING));
        }
      }
    });

    return ImmutableSet.copyOf(Iterables.transform(scheduledTasks, Tasks.SCHEDULED_TO_ID));
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
      @Override protected void execute(
          SchedulerStore schedulerStore,
          JobStore jobStore,
          final TaskStore taskStore) throws E {

        operation.execute(taskQuery == null ? null : taskStore.fetch(taskQuery),
            new StateChanger() {
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
      @Override public Set<ScheduledTask> apply(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {
        return taskStore.fetch(query);
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
      public Set<String> apply(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {
        return taskStore.fetchIds(query);
      }
    });
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
      TaskStateMachine sm = taskStateMachines.get(taskId);
      Preconditions.checkState(sm != null, "State machine not found for task ID " + taskId);
      stateChange.execute(sm);
    }
  }

  private void changeState(final Query taskQuery, final Closure<TaskStateMachine> stateChange) {
    transactionalWork(new Work.NoResult.Quiet() {
      @Override protected void execute(
          SchedulerStore schedulerStore,
          JobStore jobStore,
          TaskStore taskStore) {

        changeStateInTransaction(taskStore.fetchIds(taskQuery), stateChange);
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
          @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
              final TaskStore taskStore) {

            switch (work.command) {
              case RESCHEDULE:
                ScheduledTask task = Iterables.getOnlyElement(
                    taskStore.fetch(Query.byId(stateMachine.getTaskId()))).deepCopy();
                task.getAssignedTask().unsetSlaveId();
                task.getAssignedTask().unsetSlaveHost();
                task.unsetTaskEvents();
                task.setAncestorId(Tasks.id(task));
                String taskId = generateTaskId(task.getAssignedTask().getTask());
                task.getAssignedTask().setTaskId(taskId);

                LOG.info("Task being rescheduled: " + stateMachine.getTaskId());

                taskStore.add(ImmutableSet.of(task));

                taskStateMachines.put(taskId,
                    new TaskStateMachine(taskId, taskSupplier(taskId),
                        workSink, MISSING_TASK_GRACE_PERIOD.get())
                        .updateState(PENDING, "Rescheduled"));
                break;

              case UPDATE_STATE:
                taskStore.mutate(Query.byId(stateMachine.getTaskId()),
                    new Closure<ScheduledTask>() {
                      @Override public void execute(ScheduledTask task) {
                        task.setStatus(stateMachine.getState());
                        work.mutation.execute(task);
                      }
                    });
                break;

              case DELETE:
                taskStore.remove(ImmutableSet.of(stateMachine.getTaskId()));
                break;

              case INCREMENT_FAILURES:
                taskStore.mutate(Query.byId(stateMachine.getTaskId()),
                    new Closure<ScheduledTask>() {
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
}
