/**
 * Copyright 2014 Apache Software Foundation
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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import com.google.common.base.Optional;
import com.google.common.eventbus.Subscribe;
import com.twitter.common.stats.SlidingStats;
import com.twitter.common.util.Clock;

import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.state.StateManager;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.THROTTLED;

/**
 * A holding area for tasks that have been throttled.  Tasks entering the
 * {@link org.apache.aurora.gen.ScheduleStatus#THROTTLED} state will be transitioned to
 * {@link org.apache.aurora.gen.ScheduleStatus#PENDING} after the penalty period (as dictated by
 * {@link RescheduleCalculator} has expired.
 */
class TaskThrottler implements EventSubscriber {

  private final RescheduleCalculator rescheduleCalculator;
  private final Clock clock;
  private final ScheduledExecutorService executor;
  private final StateManager stateManager;

  private final SlidingStats throttleStats = new SlidingStats("task_throttle", "ms");

  @Inject
  TaskThrottler(
      RescheduleCalculator rescheduleCalculator,
      Clock clock,
      ScheduledExecutorService executor,
      StateManager stateManager) {

    this.rescheduleCalculator = checkNotNull(rescheduleCalculator);
    this.clock = checkNotNull(clock);
    this.executor = checkNotNull(executor);
    this.stateManager = checkNotNull(stateManager);
  }

  @Subscribe
  public void taskChangedState(final TaskStateChange stateChange) {
    if (stateChange.getNewState() == THROTTLED) {
      long readyAtMs = Tasks.getLatestEvent(stateChange.getTask()).getTimestamp()
          + rescheduleCalculator.getFlappingPenaltyMs(stateChange.getTask());
      long delayMs = Math.max(0, readyAtMs - clock.nowMillis());
      throttleStats.accumulate(delayMs);
      executor.schedule(
          new Runnable() {
            @Override
            public void run() {
              stateManager.changeState(
                  stateChange.getTaskId(),
                  Optional.of(THROTTLED),
                  PENDING,
                  Optional.<String>absent());
            }
          },
          delayMs,
          TimeUnit.MILLISECONDS);
    }
  }
}
