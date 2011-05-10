package com.twitter.mesos.scheduler;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;

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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.INIT;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED_BY_CLIENT;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static com.twitter.mesos.gen.ScheduleStatus.UNKNOWN;

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
  private final StateMachine<ScheduleStatus> stateMachine;

  /**
   * A write-only work acceptor.
   */
  static interface WorkSink {
    /**
     * Appends external work that must be performed for a state machine transition to be fully
     * complete.
     *
     * @param work Description of the work to be performed.
     * @param stateMachine  The state machine that is adding the work.
     */
    void addWork(WorkItem work, TaskStateMachine stateMachine);
  }

  /**
   * Creates a new task state machine using the default system clock.
   *
   * @param taskId ID of the task managed by this state machine.
   * @param taskReader Interface to provide read-only access to the task that this state machine
   *     manages.
   * @param workSink Work sink to receive transition response actions.
   * @param missingTaskGracePeriod Amount of time to allow a task to be in ASSIGNED state before
   *     considering an {@code UNKNOWN} transition to be a lost task.
   */
  public TaskStateMachine(String taskId, Supplier<ScheduledTask> taskReader, WorkSink workSink,
      Amount<Long, Time> missingTaskGracePeriod) {
    this(taskId, taskReader, workSink, missingTaskGracePeriod, Clock.SYSTEM_CLOCK);
  }

  /**
   * Creates a new task state machine.
   *
   * @param taskId ID of the task managed by this state machine.
   * @param taskReader Interface to provide read-only access to the task that this state machine
   *     manages.
   * @param workSink Work sink to receive transition response actions
   * @param missingTaskGracePeriod Amount of time to allow a task to be in ASSIGNED state before
   *     considering an {@code UNKNOWN} transition to be a lost task.
   * @param clock Clock to use for reading the current time.
   */
  public TaskStateMachine(
      String taskId,
      final Supplier<ScheduledTask> taskReader,
      final WorkSink workSink,
      final Amount<Long, Time> missingTaskGracePeriod,
      final Clock clock) {

    this.taskId = MorePreconditions.checkNotBlank(taskId);
    checkNotNull(taskReader);
    this.workSink = checkNotNull(workSink);
    checkNotNull(missingTaskGracePeriod);
    checkNotNull(clock);

    Closure<Transition<ScheduleStatus>> manageTerminatedTasks = Closures.combine(
        // Kill a task that we believe to be terminated when an attempt is made to revive.
        Closures.filter(Transition.to(ASSIGNED, STARTING, RUNNING), addWorkClosure(WorkItem.KILL)),
        // Remove a terminated task that is remotely removed.
        Closures.filter(Transition.to(UNKNOWN), addWorkClosure(WorkItem.DELETE)));

    // To be called on a task transitioning into the FINISHED state.
    final Command rescheduleIfDaemon = new Command() {
      @Override public void execute() {
        if (taskReader.get().getAssignedTask().getTask().isIsDaemon()) {
          addWork(WorkItem.RESCHEDULE);
        }
      }
    };

    // To be called on a task transitioning into the FAILED state.
    final Command maybeReschedule = new Command() {
      @Override public void execute() {
        ScheduledTask task = taskReader.get();
        addWork(WorkItem.INCREMENT_FAILURES);

        if (task.getFailureCount() < task.getAssignedTask().getTask().getMaxTaskFailures()) {
          addWork(WorkItem.RESCHEDULE);
        }
      }
    };

    stateMachine = StateMachine.<ScheduleStatus>builder("Task-" + taskId)
        .logTransitions()
        .initialState(INIT)
        .addState(
            Closures.filter(Transition.to(PENDING), addWorkClosure(WorkItem.CREATE_TASK)),
            INIT,
            PENDING, UNKNOWN)
        .addState(
            PENDING,
            ASSIGNED, KILLED_BY_CLIENT)
        .addState(
            new Closure<Transition<ScheduleStatus>>() {
              @Override public void execute(Transition<ScheduleStatus> transition) {
                switch (transition.getTo()) {
                  case FAILED:
                    maybeReschedule.execute();
                    break;

                  case KILLED:
                    rescheduleIfDaemon.execute();
                    break;

                  case KILLED_BY_CLIENT:
                    addWork(WorkItem.KILL);
                    break;

                  case LOST:
                    addWork(WorkItem.RESCHEDULE);
                    break;

                  case UNKNOWN:
                      // Have we been waiting too long on this task?
                    long lastEventAgeMillis = clock.nowMillis()
                        - Iterables.getLast(taskReader.get().getTaskEvents()).getTimestamp();
                    if (lastEventAgeMillis > missingTaskGracePeriod.as(Time.MILLISECONDS)) {
                      addWork(WorkItem.KILL);
                      addWork(WorkItem.RESCHEDULE);
                    }
                }
              }
            },
            ASSIGNED,
            STARTING, KILLED, KILLED_BY_CLIENT, FAILED, LOST)
        .addState(
            new Closure<Transition<ScheduleStatus>>() {
              @Override public void execute(Transition<ScheduleStatus> transition) {
                switch (transition.getTo()) {
                  case FINISHED:
                    rescheduleIfDaemon.execute();
                    break;

                  case FAILED:
                    maybeReschedule.execute();
                    break;

                  case KILLED:
                    rescheduleIfDaemon.execute();
                    break;

                  case KILLED_BY_CLIENT:
                    addWork(WorkItem.KILL);
                    break;

                  case LOST:
                  case UNKNOWN:
                    addWork(WorkItem.RESCHEDULE);
                }
              }
            },
            STARTING,
            RUNNING, FINISHED, FAILED, KILLED, KILLED_BY_CLIENT, LOST)
        .addState(
            new Closure<Transition<ScheduleStatus>>() {
              @Override public void execute(Transition<ScheduleStatus> transition) {
                switch (transition.getTo()) {
                  case FINISHED:
                    rescheduleIfDaemon.execute();
                    break;

                  case FAILED:
                    maybeReschedule.execute();
                    break;

                  case KILLED:
                    rescheduleIfDaemon.execute();
                    break;

                  case KILLED_BY_CLIENT:
                    addWork(WorkItem.KILL);
                    break;

                  case LOST:
                  case UNKNOWN:
                    addWork(WorkItem.RESCHEDULE);
                }
              }
            },
            RUNNING,
            FINISHED, FAILED, KILLED, KILLED_BY_CLIENT, LOST)
        .addState(
            manageTerminatedTasks,
            FINISHED,
            UNKNOWN)
        .addState(
            manageTerminatedTasks,
            FAILED,
            UNKNOWN)
        .addState(
            manageTerminatedTasks,
            KILLED,
            UNKNOWN)
        .addState(
            manageTerminatedTasks,
            KILLED_BY_CLIENT,
            UNKNOWN)
        .addState(
            manageTerminatedTasks,
            LOST,
            UNKNOWN)
        .addState(
            manageTerminatedTasks,
            UNKNOWN)
        // Since we want this action to be performed last in the transition sequence, the callback
        // must be the last chained transition callback.
        .onAnyTransition(
            new Closure<Transition<ScheduleStatus>>() {
              @Override
              public void execute(final Transition<ScheduleStatus> transition) {
                if (transition.isValidStateChange() && transition.getTo() != UNKNOWN) {
                  addWork(WorkItem.UPDATE_STATE);
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

  private Closure<Transition<ScheduleStatus>> addWorkClosure(final WorkItem work) {
    return new Closure<Transition<ScheduleStatus>>() {
      @Override public void execute(Transition<ScheduleStatus> item) {
        addWork(work);
      }
    };
  }

  private void addWork(final WorkItem work) {
    LOG.info("Adding work item " + work + " for " + this);
    workSink.addWork(work, TaskStateMachine.this);
  }

  /**
   * Attempt to transition the state machine to the provided state.
   * At the time this method returns, any work items required to satisfy the state transition will
   * be appended to the work queue.
   *
   * @param status Status to apply to the task.
   */
  public synchronized void setState(ScheduleStatus status) {
    checkNotNull(status);
    stateMachine.transition(status);
  }

  /**
   * Fetch the current state from the state machine.
   *
   * @return The current state.
   */
  public synchronized ScheduleStatus getState() {
    return stateMachine.getState();
  }

  @Override
  public String toString() {
    return taskId;
  }
}
