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
package org.apache.aurora.scheduler.async;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatsProvider;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.state.StateManager;

import static java.util.Objects.requireNonNull;

/**
 * Observes task transitions and identifies tasks that are 'stuck' in a transient state.  Stuck
 * tasks will be transitioned to the LOST state.
 */
class TaskTimeout extends AbstractIdleService implements EventSubscriber {
  private static final Logger LOG = Logger.getLogger(TaskTimeout.class.getName());

  @VisibleForTesting
  static final Amount<Long, Time> NOT_STARTED_RETRY = Amount.of(5L, Time.SECONDS);

  @VisibleForTesting
  static final String TIMED_OUT_TASKS_COUNTER = "timed_out_tasks";

  @VisibleForTesting
  static final Optional<String> TIMEOUT_MESSAGE = Optional.of("Task timed out");

  @VisibleForTesting
  static final Set<ScheduleStatus> TRANSIENT_STATES = EnumSet.of(
      ScheduleStatus.ASSIGNED,
      ScheduleStatus.PREEMPTING,
      ScheduleStatus.RESTARTING,
      ScheduleStatus.KILLING,
      ScheduleStatus.DRAINING);

  private final ScheduledExecutorService executor;
  private final StateManager stateManager;
  private final Amount<Long, Time> timeout;
  private final AtomicLong timedOutTasks;

  @Inject
  TaskTimeout(
      ScheduledExecutorService executor,
      StateManager stateManager,
      Amount<Long, Time> timeout,
      StatsProvider statsProvider) {

    this.executor = requireNonNull(executor);
    this.stateManager = requireNonNull(stateManager);
    this.timeout = requireNonNull(timeout);
    this.timedOutTasks = statsProvider.makeCounter(TIMED_OUT_TASKS_COUNTER);
  }

  private static boolean isTransient(ScheduleStatus status) {
    return TRANSIENT_STATES.contains(status);
  }

  @Override
  protected void startUp() {
    // No work to do here for startup, however we leverage the state tracking in
    // AbstractIdleService.
  }

  @Override
  protected void shutDown() {
    // Nothing to do for shutting down.
  }

  @Subscribe
  public void recordStateChange(TaskStateChange change) {
    final String taskId = change.getTaskId();
    final ScheduleStatus newState = change.getNewState();
    if (isTransient(newState)) {
      executor.schedule(
          new Runnable() {
            @Override
            public void run() {
              if (isRunning()) {
                // This query acts as a CAS by including the state that we expect the task to be in
                // if the timeout is still valid.  Ideally, the future would have already been
                // canceled, but in the event of a state transition race, including transientState
                // prevents an unintended task timeout.
                // Note: This requires LOST transitions trigger Driver.killTask.
                if (stateManager.changeState(
                    taskId,
                    Optional.of(newState),
                    ScheduleStatus.LOST,
                    TIMEOUT_MESSAGE)) {

                  LOG.info("Timeout reached for task " + taskId + ":" + taskId);
                  timedOutTasks.incrementAndGet();
                }
              } else {
                // Our service is not yet started.  We don't want to lose track of the task, so
                // we will try again later.
                LOG.fine("Retrying timeout of task " + taskId + " in " + NOT_STARTED_RETRY);
                executor.schedule(
                    this,
                    NOT_STARTED_RETRY.getValue(),
                    NOT_STARTED_RETRY.getUnit().getTimeUnit());
              }
            }
          },
          timeout.getValue(),
          timeout.getUnit().getTimeUnit());
    }
  }
}
