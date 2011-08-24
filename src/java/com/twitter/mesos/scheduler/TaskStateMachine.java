package com.twitter.mesos.scheduler;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.commons.lang.builder.HashCodeBuilder;

import com.twitter.common.base.Closure;
import com.twitter.common.base.Closures;
import com.twitter.common.base.Command;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;
import com.twitter.common.util.StateMachine;
import com.twitter.common.util.StateMachine.Transition;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskEvent;


import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.INIT;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLING;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.PREEMPTING;
import static com.twitter.mesos.gen.ScheduleStatus.RESTARTING;
import static com.twitter.mesos.gen.ScheduleStatus.ROLLBACK;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static com.twitter.mesos.gen.ScheduleStatus.UNKNOWN;
import static com.twitter.mesos.gen.ScheduleStatus.UPDATING;

/**
 * State machine for a mesos task.
 * This enforces the lifecycle of a task, and triggers the actions that should be taken in response
 * to different state transitions.  These responses are externally communicated by populating a
 * provided work queue.
 *
 * TODO(William Farner): Introduce an interface to allow state machines to be dealt with
 *     abstractly from the consumption side.
 *
 * @author William Farner
 */
public class TaskStateMachine {

  private static final Logger LOG = Logger.getLogger(TaskStateMachine.class.getName());

  private static final AtomicLong ILLEGAL_TRANSITIONS =
      Stats.exportLong("scheduler_illegal_task_state_transitions");

  private final String taskId;
  private final WorkSink workSink;
  private final StateMachine<State> stateMachine;
  private final Clock clock;

  /**
   * Composes a schedule status and a state change argument.  Only the ScheduleStatuses in two
   * States must be equal for them to be considered equal.
   */
  private static class State {
    ScheduleStatus state;
    Closure<ScheduledTask> mutation;

    static State create(ScheduleStatus status) {
      return create(status, null);
    }

    static State create(ScheduleStatus status, @Nullable Closure<ScheduledTask> mutation) {
      State state = new State();
      state.state = status;
      state.mutation = mutation;
      return state;
    }

    static State[] array(ScheduleStatus status, ScheduleStatus... statuses) {
      ImmutableList.Builder<State> builder = ImmutableList.builder();
      builder.add(State.create(status));
      for (ScheduleStatus curStatus : statuses) {
        builder.add(State.create(curStatus));
      }
      List<State> states = builder.build();
      return states.toArray(new State[states.size()]);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof State)) {
        return false;
      }

      if (o == this) {
        return true;
      }

      State other = (State) o;
      return state == other.state;
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder()
          .append(state)
          .toHashCode();
    }

    @Override
    public String toString() {
      return state.toString();
    }
  }

  /**
   * A write-only work acceptor.
   */
  public interface WorkSink {
    /**
     * Appends external work that must be performed for a state machine transition to be fully
     * complete.
     *
     * @param work Description of the work to be performed.
     * @param stateMachine The state machine that the work is associated with.
     * @param mutation Mutate operation to perform along with the state transition.
     */
    void addWork(WorkCommand work, TaskStateMachine stateMachine, Closure<ScheduledTask> mutation);
  }

  /**
   * Creates a new task state machine using the default system clock.
   *
   * @param taskId ID of the task managed by this state machine.
   * @param taskReader Interface to provide read-only access to the task that this state machine
   *     manages.
   * @param isJobUpdating Supplier to test whether the task's job is currently in a rolling update.
   * @param workSink Work sink to receive transition response actions.
   * @param missingTaskGracePeriod Amount of time to allow a task to be in ASSIGNED state before
   *     considering an {@code UNKNOWN} transition to be a lost task.
   */
  public TaskStateMachine(String taskId, Supplier<ScheduledTask> taskReader,
      Supplier<Boolean> isJobUpdating, WorkSink workSink,
      Amount<Long, Time> missingTaskGracePeriod) {
    this(taskId, taskReader, isJobUpdating, workSink, missingTaskGracePeriod,
        Clock.SYSTEM_CLOCK, INIT);
  }

  public TaskStateMachine(String taskId, Supplier<ScheduledTask> taskReader,
      Supplier<Boolean> isJobUpdating, WorkSink workSink,
      Amount<Long, Time> missingTaskGracePeriod, ScheduleStatus initialState) {
    this(taskId, taskReader, isJobUpdating, workSink, missingTaskGracePeriod,
        Clock.SYSTEM_CLOCK, initialState);
  }

  /**
   * Creates a new task state machine.
   *
   * @param taskId ID of the task managed by this state machine.
   * @param taskReader Interface to provide read-only access to the task that this state machine
   *     manages.
   * @param isJobUpdating Supplier to test whether the task's job is currently in a rolling update.
   * @param workSink Work sink to receive transition response actions
   * @param missingTaskGracePeriod Amount of time to allow a task to be in ASSIGNED state before
   *     considering an {@code UNKNOWN} transition to be a lost task.
   * @param clock Clock to use for reading the current time.
   * @param initialState The state to begin the state machine at.  All legal transitions will be
   *     added, but this allows the state machine to 'skip' states, for instance when a task is
   *     loaded from a persistent store.
   */
  public TaskStateMachine(
      final String taskId,
      final Supplier<ScheduledTask> taskReader,
      final Supplier<Boolean> isJobUpdating,
      final WorkSink workSink,
      final Amount<Long, Time> missingTaskGracePeriod,
      final Clock clock,
      final ScheduleStatus initialState) {

    this.taskId = MorePreconditions.checkNotBlank(taskId);
    checkNotNull(taskReader);
    this.workSink = checkNotNull(workSink);
    checkNotNull(missingTaskGracePeriod);
    this.clock = checkNotNull(clock);
    checkNotNull(initialState);

    @SuppressWarnings("unchecked")
    Closure<Transition<State>> manageTerminatedTasks = Closures.combine(
        // Kill a task that we believe to be terminated when an attempt is made to revive.
        Closures.filter(Transition.<State>to(State.array(ASSIGNED, STARTING, RUNNING)),
            addWorkClosure(WorkCommand.KILL)),
        // Remove a terminated task that is remotely removed.
        Closures.filter(Transition.<State>to(State.create(UNKNOWN)),
            addWorkClosure(WorkCommand.DELETE)));

    final Closure<Transition<State>> restart = new Closure<Transition<State>>() {
      @Override public void execute(Transition<State> transition) {
        addWork(WorkCommand.RESCHEDULE, transition.getTo().mutation);
      }
    };

    @SuppressWarnings("unchecked")
    final Closure<Transition<State>> manageRestartingTask = Closures.combine(
        manageTerminatedTasks,
        Closures.filter(Transition.to(State.create(KILLED)), restart));

    // To be called on a task transitioning into the FINISHED state.
    final Command rescheduleIfDaemon = new Command() {
      @Override public void execute() {
        if (taskReader.get().getAssignedTask().getTask().isIsDaemon()) {
          addWork(WorkCommand.RESCHEDULE);
        }
      }
    };

    // To be called on a task transitioning into the FAILED state.
    final Command incrementFailuresMaybeReschedule = new Command() {
      @Override public void execute() {
        ScheduledTask task = taskReader.get();
        addWork(WorkCommand.INCREMENT_FAILURES);

        // Max failures is ignored for daemon task.
        boolean isDaemon = task.getAssignedTask().getTask().isIsDaemon();

        // Max failures is ignored when set to -1.
        int maxFailures = task.getAssignedTask().getTask().getMaxTaskFailures();
        if (isDaemon || (maxFailures == -1) || (task.getFailureCount() < (maxFailures - 1))) {
          addWork(WorkCommand.RESCHEDULE);
        } else {
          LOG.info("Task " + getTaskId() + " reached failure limit, not rescheduling");
        }
      }
    };

    final Command initiateUpdateSequence = new Command() {
      @Override public void execute() throws IllegalStateException {
        Preconditions.checkState(isJobUpdating.get(),
            "No active update found for task " + taskId + ", which is trying to update/rollback.");
        addWork(WorkCommand.KILL);
      }
    };

    stateMachine = StateMachine.<State>builder("Task-" + taskId)
        .logTransitions()
        .initialState(State.create(initialState))
        .addState(
            State.create(INIT),
            State.array(PENDING, UNKNOWN))
        .addState(
            new Closure<Transition<State>>() {
              @Override public void execute(Transition<State> transition) {
                switch (transition.getTo().state) {
                  case KILLING:
                    addWork(WorkCommand.DELETE);
                    break;

                  case UPDATING:
                    addWork(WorkCommand.UPDATE);
                    addWork(WorkCommand.DELETE);
                    break;

                  case ROLLBACK:
                    addWork(WorkCommand.ROLLBACK);
                    addWork(WorkCommand.DELETE);
                }
              }
            },
            State.create(PENDING),
            State.array(ASSIGNED, UPDATING, ROLLBACK, KILLING))
        .addState(
            new Closure<Transition<State>>() {
              @Override
              public void execute(Transition<State> transition) {
                switch (transition.getTo().state) {
                  case FINISHED:
                    rescheduleIfDaemon.execute();
                    break;

                  case PREEMPTING:
                    addWork(WorkCommand.KILL);
                    break;

                  case FAILED:
                    incrementFailuresMaybeReschedule.execute();
                    break;

                  case RESTARTING:
                    addWork(WorkCommand.KILL);
                    break;

                  case UPDATING:
                  case ROLLBACK:
                    initiateUpdateSequence.execute();
                    break;

                  case KILLED:
                    addWork(WorkCommand.RESCHEDULE);
                    break;

                  case KILLING:
                    addWork(WorkCommand.KILL);
                    break;

                  case LOST:
                    addWork(WorkCommand.RESCHEDULE);
                    break;

                  case UNKNOWN:
                    // Determine if we have been waiting too long on this task.
                    if (isTaskTimedOut(taskReader.get().getTaskEvents(), missingTaskGracePeriod)) {
                      updateState(LOST);
                    }
                }
              }
            },
            State.create(ASSIGNED),
            State.array(STARTING, RUNNING, FINISHED, FAILED, RESTARTING, UPDATING, ROLLBACK, KILLED,
                KILLING, LOST, PREEMPTING))
        .addState(
            new Closure<Transition<State>>() {
              @Override
              public void execute(Transition<State> transition) {
                switch (transition.getTo().state) {
                  case FINISHED:
                    rescheduleIfDaemon.execute();
                    break;

                  case RESTARTING:
                    addWork(WorkCommand.KILL);
                    break;

                  case UPDATING:
                  case ROLLBACK:
                    initiateUpdateSequence.execute();
                    break;

                  case PREEMPTING:
                    addWork(WorkCommand.KILL);
                    break;

                  case FAILED:
                    incrementFailuresMaybeReschedule.execute();
                    break;

                  case KILLED:
                    addWork(WorkCommand.RESCHEDULE);
                    break;

                  case KILLING:
                    addWork(WorkCommand.KILL);
                    break;

                  case LOST:
                    addWork(WorkCommand.RESCHEDULE);
                    break;

                  case UNKNOWN:
                    // The slave previously acknowledged that it had the task, and now stopped
                    // reporting it.
                    updateState(LOST);
                }
              }
            },
            State.create(STARTING),
            State.array(RUNNING, FINISHED, FAILED, RESTARTING, UPDATING, KILLING, KILLED,
                ROLLBACK, LOST, PREEMPTING))
        .addState(
            new Closure<Transition<State>>() {
              @Override
              public void execute(Transition<State> transition) {
                switch (transition.getTo().state) {
                  case FINISHED:
                    rescheduleIfDaemon.execute();
                    break;

                  case PREEMPTING:
                    addWork(WorkCommand.KILL);
                    break;

                  case RESTARTING:
                    addWork(WorkCommand.KILL);
                    break;

                  case UPDATING:
                  case ROLLBACK:
                    initiateUpdateSequence.execute();
                    break;

                  case FAILED:
                    incrementFailuresMaybeReschedule.execute();
                    break;

                  case KILLED:
                    addWork(WorkCommand.RESCHEDULE);
                    break;

                  case KILLING:
                    addWork(WorkCommand.KILL);
                    break;

                  case LOST:
                    addWork(WorkCommand.RESCHEDULE);
                    break;

                  case UNKNOWN:
                    updateState(LOST);
                }
              }
            },
            State.create(RUNNING),
            State.array(FINISHED, RESTARTING, UPDATING, FAILED, KILLING, KILLED, ROLLBACK,
                LOST, PREEMPTING))
        .addState(
            manageTerminatedTasks,
            State.create(FINISHED),
            State.create(UNKNOWN))
        .addState(
            new Closure<Transition<State>>() {
              @Override public void execute(Transition<State> transition) {
                switch (transition.getTo().state) {
                  case ASSIGNED:
                  case STARTING:
                  case RUNNING:
                    addWork(WorkCommand.KILL);
                    break;

                  case LOST:
                  case KILLED:
                    addWork(WorkCommand.RESCHEDULE);
                    break;

                  case UNKNOWN:
                    updateState(LOST);
                }
              }
            },
            State.create(PREEMPTING),
            State.array(FINISHED, FAILED, KILLING, KILLED, LOST))
        .addState(
            manageRestartingTask,
            State.create(RESTARTING),
            State.array(KILLED, UNKNOWN))
        .addState(
            manageUpdatingTask(false),
            State.create(UPDATING),
            State.array(ROLLBACK, FINISHED, FAILED, KILLING, KILLED, LOST, UNKNOWN))
        .addState(
            manageUpdatingTask(true),
            State.create(ROLLBACK),
            State.array(FINISHED, FAILED, KILLING, KILLED, LOST))
        .addState(
            manageTerminatedTasks,
            State.create(FAILED),
            State.create(UNKNOWN))
        .addState(
            manageTerminatedTasks,
            State.create(KILLED),
            State.create(UNKNOWN))
        .addState(
            manageTerminatedTasks,
            State.create(KILLING),
            State.array(FINISHED, FAILED, KILLED, LOST, UNKNOWN))
        .addState(
            manageTerminatedTasks,
            State.create(LOST),
            State.create(UNKNOWN))
        .addState(
            manageTerminatedTasks,
            State.create(UNKNOWN))
        // Since we want this action to be performed last in the transition sequence, the callback
        // must be the last chained transition callback.
        .onAnyTransition(
            new Closure<Transition<State>>() {
              @Override
              public void execute(final Transition<State> transition) {
                ScheduleStatus from = transition.getFrom().state;
                ScheduleStatus to = transition.getTo().state;

                if (transition.isValidStateChange() && (to != UNKNOWN)
                    // Prevent an update when killing a pending task, since the task is deleted
                    // prior to the update.
                    && !((from == PENDING) && (to == KILLING))) {
                  addWork(WorkCommand.UPDATE_STATE, transition.getTo().mutation);
                } else if (!transition.isAllowed()) {
                  LOG.log(Level.SEVERE, "Illegal state transition attempted: " + transition);
                  ILLEGAL_TRANSITIONS.incrementAndGet();
                }
              }
            }
        )
        .throwOnBadTransition(false)
        .build();
  }

  private boolean isTaskTimedOut(Iterable<TaskEvent> taskEvents,
      Amount<Long, Time> missingTaskGracePeriod) {
    if (taskEvents == null || Iterables.isEmpty(taskEvents)) {
      LOG.warning("Task " + taskId + " had no task events, assuming timed out.");
      return true;
    } else {
      long lastEventAgeMillis = clock.nowMillis() - Iterables.getLast(taskEvents).getTimestamp();
      return lastEventAgeMillis > missingTaskGracePeriod.as(Time.MILLISECONDS);
    }
  }

  private Closure<Transition<State>> manageUpdatingTask(final boolean rollback) {
    return new Closure<Transition<State>>() {
      @Override public void execute(Transition<State> transition) {
        switch (transition.getTo().state) {
          case ASSIGNED:
          case STARTING:
          case KILLING:
          case ROLLBACK:
          case RUNNING:
            addWork(WorkCommand.KILL);
            break;
          case FINISHED:
          case FAILED:
          case KILLED:
          case LOST:
            addWork(rollback ? WorkCommand.ROLLBACK : WorkCommand.UPDATE);
            break;

          case UNKNOWN:
            // TODO(wfarner): DELETE isn't the best thing to do here, since we lose track of the
            // task, but moving to LOST will cause it to be rescheduled.  Need to rethink.
            addWork(WorkCommand.DELETE);
            addWork(rollback ? WorkCommand.ROLLBACK : WorkCommand.UPDATE);

        }
      }
    };
  }

  private Closure<Transition<State>> addWorkClosure(final WorkCommand work) {
    return new Closure<Transition<State>>() {
      @Override public void execute(Transition<State> item) {
        addWork(work);
      }
    };
  }

  private void addWork(WorkCommand work) {
    addWork(work, Closures.<ScheduledTask>noop());
  }

  private void addWork(WorkCommand work, Closure<ScheduledTask> mutation) {
    LOG.info("Adding work command " + work + " for " + this);
    workSink.addWork(work, TaskStateMachine.this, mutation);
  }

  /**
   * Same as {@link #updateState(ScheduleStatus, Closure)}, but uses a noop mutation.
   *
   * @param status Status to apply to the task.
   * @return A reference to the state machine.
   */
  public synchronized TaskStateMachine updateState(ScheduleStatus status) {
    return updateState(status, Closures.<ScheduledTask>noop());
  }

  /**
   * Same as {@link #updateState(ScheduleStatus, Closure, String)}, but uses a noop mutation.
   *
   * @param status Status to apply to the task.
   * @param auditMessage The (optional) audit message to associate with the transition.
   * @return A reference to the state machine.
   */
  public synchronized TaskStateMachine updateState(ScheduleStatus status,
      @Nullable String auditMessage) {
    return updateState(status, Closures.<ScheduledTask>noop(), auditMessage);
  }

  /**
   * Same as {@link #updateState(ScheduleStatus, Closure, String)}, but omits the audit message.
   *
   * @param status Status to apply to the task.
   * @param mutation Mutate operation to perform while updating the task.
   * @return A reference to the state machine.
   */
  public synchronized TaskStateMachine updateState(ScheduleStatus status,
      Closure<ScheduledTask> mutation) {
    return updateState(status, mutation, null);
  }

  /**
   * Attempt to transition the state machine to the provided state.
   * At the time this method returns, any work commands required to satisfy the state transition
   * will be appended to the work queue.
   *
   * @param status Status to apply to the task.
   * @param auditMessage The (optional) audit message to associate with the transition.
   * @param mutation Mutate operation to perform while updating the task.
   * @return A reference to the state machine.
   */
  public synchronized TaskStateMachine updateState(final ScheduleStatus status,
      Closure<ScheduledTask> mutation,
      @Nullable final String auditMessage) {
    checkNotNull(status);
    checkNotNull(mutation);

    // Don't bother applying noop state changes.  If we end up modifying task state without a
    // state transition (e.g. storing resource consumption of a running task, for example), we need
    // to find a different way to suppress noop transitions.
    if (stateMachine.getState().state != status) {
      @SuppressWarnings("unchecked")
      Closure<ScheduledTask> operation = Closures.combine(mutation,
          new Closure<ScheduledTask>() {
            @Override public void execute(ScheduledTask task) {
              task.addToTaskEvents(new TaskEvent(clock.nowMillis(), status, auditMessage));
            }
          });
      stateMachine.transition(State.create(status, operation));
    }

    return this;
  }

  /**
   * Fetch the current state from the state machine.
   *
   * @return The current state.
   */
  public synchronized ScheduleStatus getState() {
    return stateMachine.getState().state;
  }

  /**
   * Gets the ID for the task that this state machine manages.
   *
   * @return The state machine's task ID.
   */
  public String getTaskId() {
    return taskId;
  }

  @Override
  public String toString() {
    return getTaskId();
  }
}
