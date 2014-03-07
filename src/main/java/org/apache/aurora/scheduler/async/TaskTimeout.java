/**
 * Copyright 2013 Apache Software Foundation
 *
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.eventbus.Subscribe;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatsProvider;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.state.StateManager;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Observes task transitions and identifies tasks that are 'stuck' in a transient state.  Stuck
 * tasks will be transitioned to the LOST state.
 */
class TaskTimeout implements EventSubscriber {
  private static final Logger LOG = Logger.getLogger(TaskTimeout.class.getName());

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
  private final long timeoutMillis;
  private final AtomicLong timedOutTasks;

  @Inject
  TaskTimeout(
      ScheduledExecutorService executor,
      StateManager stateManager,
      Amount<Long, Time> timeout,
      StatsProvider statsProvider) {

    this.executor = checkNotNull(executor);
    this.stateManager = checkNotNull(stateManager);
    this.timeoutMillis = timeout.as(Time.MILLISECONDS);
    this.timedOutTasks = statsProvider.makeCounter(TIMED_OUT_TASKS_COUNTER);
  }

  private static boolean isTransient(ScheduleStatus status) {
    return TRANSIENT_STATES.contains(status);
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
              // This query acts as a CAS by including the state that we expect the task to be in if
              // the timeout is still valid.  Ideally, the future would have already been canceled,
              // but in the event of a state transition race, including transientState prevents an
              // unintended task timeout.
              // Note: This requires LOST transitions trigger Driver.killTask.
              if (stateManager.changeState(
                  taskId,
                  Optional.of(newState),
                  ScheduleStatus.LOST,
                  TIMEOUT_MESSAGE)) {

                LOG.info("Timeout reached for task " + taskId + ":" + taskId);
                timedOutTasks.incrementAndGet();
              }
            }
          },
          timeoutMillis,
          TimeUnit.MILLISECONDS);
    }
  }
}
