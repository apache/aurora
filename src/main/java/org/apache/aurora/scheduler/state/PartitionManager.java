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
package org.apache.aurora.scheduler.state;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;

import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult.Quiet;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Subscribes to transitions into PARTITIONED task state and, if applicable, sets a timer to
 * move the task to LOST.
 */
public class PartitionManager implements PubsubEvent.EventSubscriber {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);

  private final ScheduledExecutorService executor;
  private final StateManager stateManager;
  private final Storage storage;
  private final Clock clock;

  @VisibleForTesting
  static final String TRANSITION_MESSAGE = "Task partitioned for too long.";

  @VisibleForTesting
  PartitionManager(
      Storage storage,
      StateManager stateManager,
      Clock clock,
      ScheduledExecutorService executor) {

    this.executor = requireNonNull(executor);
    this.stateManager = requireNonNull(stateManager);
    this.storage = requireNonNull(storage);
    this.clock = requireNonNull(clock);
  }

  @Inject
  PartitionManager(Storage storage, StateManager stateManager, Clock clock) {
    this(
      storage,
      stateManager,
      clock,
      AsyncUtil.singleThreadLoggingScheduledExecutor("PartitionManager", LOG));
  }

  private long getLastTransitionSecsAgo(IScheduledTask task) {
    return Duration.ofMillis(
        clock.nowMillis() - Tasks.getLatestEvent(task).getTimestamp()).getSeconds();
  }

  /**
   * Schedules a delayed task to move tasks from PARTITIONED -> LOST based on task partition policy.
   * @param stateChange The state change event.
   */
  @Subscribe
  public void handle(TaskStateChange stateChange) {
    ITaskConfig config = stateChange.getTask().getAssignedTask().getTask();
    String taskId = Tasks.id(stateChange.getTask());
    // Partition Policy can be null, in which case its equivalent to reschedule with 0s delay.
    if (stateChange.getNewState().equals(ScheduleStatus.PARTITIONED)
        && (!config.isSetPartitionPolicy() || config.getPartitionPolicy().isReschedule())) {
      long delay = config.isSetPartitionPolicy() ? config.getPartitionPolicy().getDelaySecs() : 0;
      // We're recovering from a failover, so modify the delay based on last event time.
      if (!stateChange.isTransition()) {
        delay = Math.max(0, delay - getLastTransitionSecsAgo(stateChange.getTask()));
      }
      LOG.info("Partitioned task {} will be rescheduled in {} secs", taskId, delay);
      // Use the timestamp to verify the task state hasn't changed when we execute after the delay.
      long lastTimestamp = Tasks.getLatestEvent(stateChange.getTask()).getTimestamp();
      executor.schedule(() -> storage.write((Quiet) storeProvider -> {
        Optional<IScheduledTask> maybeTask = storeProvider.getTaskStore().fetchTask(taskId);
        if (maybeTask.isPresent()
            && Tasks.getLatestEvent(maybeTask.get()).getTimestamp() == lastTimestamp) {
          stateManager.changeState(
              storeProvider,
              stateChange.getTask().getAssignedTask().getTaskId(),
              Optional.of(ScheduleStatus.PARTITIONED),
              ScheduleStatus.LOST,
              Optional.of(TRANSITION_MESSAGE));
        }
      }), delay, TimeUnit.SECONDS);
    } else if (stateChange.getNewState().equals(ScheduleStatus.PARTITIONED)) {
      LOG.info("Partitioned task {} will not be rescheduled.", taskId);
    }
  }
}
