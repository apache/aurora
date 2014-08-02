/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.updater;

import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.entities.ITaskEvent;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.updater.InstanceUpdater.Result.FAILED;
import static org.apache.aurora.scheduler.updater.InstanceUpdater.Result.SUCCESS;

/**
 * In part of a job update, this manages the update of an individual instance. This includes
 * deciding how to effect an update from a possibly-absent old configuration to a possibly-absent
 * new configuration, and detecting whether a replaced instance becomes unstable.
 *
 * TODO(wfarner): This probably needs to be parameterized so that it may be reused for rollbacks.
 */
class InstanceUpdater {
  private static final Logger LOG = Logger.getLogger(InstanceUpdater.class.getName());

  private final Optional<ITaskConfig> desiredState;
  private final int toleratedFailures;
  private final Amount<Long, Time> minRunningTime;
  private final Amount<Long, Time> maxNonRunningTime;
  private final Clock clock;

  /**
   * Keep an optional controller reference so that we may discard the reference after we've
   * advertised completion through {@link TaskController#updateCompleted(Result) updateCompleted}.
   * This gives us a signal to no-op (when this is reset to absent), and ensures we can't send any
   * control signals from that point on.
   */
  private Optional<TaskController> controllerRef = Optional.absent();

  private int observedFailures = 0;

  InstanceUpdater(
      Optional<ITaskConfig> desiredState,
      int toleratedFailures,
      Amount<Long, Time> minRunningTime,
      Amount<Long, Time> maxNonRunningTime,
      Clock clock,
      final TaskController controller) {

    this.desiredState = requireNonNull(desiredState);
    this.toleratedFailures = toleratedFailures;
    this.minRunningTime = requireNonNull(minRunningTime);
    this.maxNonRunningTime = requireNonNull(maxNonRunningTime);
    this.clock = requireNonNull(clock);
    this.controllerRef = Optional.of(controller);
  }

  private long millisSince(ITaskEvent event) {
    return clock.nowMillis() - event.getTimestamp();
  }

  private boolean appearsStable(IScheduledTask task) {
    return millisSince(Tasks.getLatestEvent(task)) >= minRunningTime.as(Time.MILLISECONDS);
  }

  private boolean appearsStuck(IScheduledTask task) {
    // Walk task events backwards to find the first event, or first non-running event.
    ITaskEvent earliestNonRunningEvent = task.getTaskEvents().get(0);
    for (ITaskEvent event : Lists.reverse(task.getTaskEvents())) {
      if (event.getStatus() == RUNNING) {
        break;
      } else {
        earliestNonRunningEvent = event;
      }
    }

    return millisSince(earliestNonRunningEvent) >= maxNonRunningTime.as(Time.MILLISECONDS);
  }

  private boolean permanentlyKilled(IScheduledTask task) {
    return Iterables.any(
        task.getTaskEvents(),
        Predicates.compose(Predicates.equalTo(ScheduleStatus.KILLING), Tasks.TASK_EVENT_TO_STATUS));
  }

  private static boolean isKillable(ScheduleStatus status) {
    return Tasks.isActive(status) && status != ScheduleStatus.KILLING;
  }

  private void completed(Result status) {
    controllerRef.get().updateCompleted(status);
    controllerRef = Optional.absent();
  }

  /**
   * Evaluates the state differences between the originally-provided {@code desiredState} and the
   * provided {@code actualState}, and invokes any necessary actions on the provided
   * {@link TaskController}.
   * <p>
   * This function should be idempotent, with the exception of an internal failure counter that
   * increments when an updating task exits, or an active but not
   * {@link ScheduleStatus#RUNNING RUNNING} task takes too long to start.
   *
   * <p>
   * It is the reponsibility of the caller to ensure that the {@code actualState} is the latest
   * value.  Note: the caller should avoid calling this when a terminal task is moving to another
   * terminal state.  It should also suppress deletion events for tasks that have been replaced by
   * an active task.
   *
   * @param actualState The actual observed state of the task.
   */
  synchronized void evaluate(Optional<IScheduledTask> actualState) {
    if (!controllerRef.isPresent()) {
      // Avoid any further action if a result was already given.
      return;
    }

    TaskController controller = controllerRef.get();

    boolean desiredPresent = desiredState.isPresent();
    boolean actualPresent = actualState.isPresent();

    if (desiredPresent && actualPresent) {
      // The update is changing the task configuration.
      handleActualAndDesiredPresent(actualState.get());
    } else if (desiredPresent) {
      // The update is introducing a new instance.
      controller.addReplacement();
    } else if (actualPresent) {
      // The update is removing an instance.
      if (isKillable(actualState.get().getStatus())) {
        controller.killTask();
      }
    } else {
      // No-op update.
      completed(SUCCESS);
    }
  }

  private boolean addFailureAndCheckIfFailed() {
    LOG.info("Observed updated task failure.");
    observedFailures++;
    if (observedFailures > toleratedFailures) {
      completed(FAILED);
      return true;
    }
    return false;
  }

  private void handleActualAndDesiredPresent(IScheduledTask actualState) {
    Preconditions.checkState(desiredState.isPresent());
    Preconditions.checkArgument(!actualState.getTaskEvents().isEmpty());

    TaskController controller = controllerRef.get();

    ScheduleStatus status = actualState.getStatus();
    if (desiredState.get().equals(actualState.getAssignedTask().getTask())) {
      // The desired task is in the system.
      if (status == RUNNING) {
        // The desired task is running.
        if (appearsStable(actualState)) {
          // Stably running, our work here is done.
          completed(SUCCESS);
        } else {
          // Not running long enough to consider stable, check again later.
          controller.reevaluteAfterRunningLimit();
        }
      } else if (Tasks.isTerminated(status)) {
        // The desired task has terminated, this is a failure.
        addFailureAndCheckIfFailed();
      } else if (appearsStuck(actualState)) {
        // The task is not running, but not terminated, and appears to have been in this state
        // long enough that we should intervene.
        if (!addFailureAndCheckIfFailed()) {
          controller.killTask();
        }
      } else {
        // The task is in a transient state on the way into or out of running, check back later.
        controller.reevaluteAfterRunningLimit();
      }
    } else {
      // This is not the configuration that we would like to run.
      if (isKillable(status)) {
        // Task is active, kill it.
        controller.killTask();
      } else if (Tasks.isTerminated(status) && permanentlyKilled(actualState)) {
        // The old task has exited, it is now safe to add the new one.
        controller.addReplacement();
      }
    }
  }

  enum Result {
    SUCCESS,
    FAILED
  }
}
