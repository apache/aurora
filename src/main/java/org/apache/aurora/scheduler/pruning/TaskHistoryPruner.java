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
package org.apache.aurora.scheduler.pruning;

import java.util.Set;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;

import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.BatchWorker;
import org.apache.aurora.scheduler.SchedulerModule.TaskEventBatchWorker;
import org.apache.aurora.scheduler.async.AsyncModule.AsyncExecutor;
import org.apache.aurora.scheduler.async.DelayExecutor;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.base.AsyncUtil.shutdownOnError;
import static org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import static org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;

/**
 * Prunes tasks in a job based on per-job history and an inactive time threshold by observing tasks
 * transitioning into one of the inactive states.
 */
public class TaskHistoryPruner implements EventSubscriber {
  private static final Logger LOG = LoggerFactory.getLogger(TaskHistoryPruner.class);
  private static final String FATAL_ERROR_FORMAT =
      "Unexpected problem pruning task history for %s. Triggering shutdown";

  private final DelayExecutor executor;
  private final StateManager stateManager;
  private final Clock clock;
  private final HistoryPrunnerSettings settings;
  private final Storage storage;
  private final Lifecycle lifecycle;
  private final TaskEventBatchWorker batchWorker;

  private final Predicate<IScheduledTask> safeToDelete = new Predicate<IScheduledTask>() {
    @Override
    public boolean apply(IScheduledTask task) {
      return Tasks.getLatestEvent(task).getTimestamp()
          <= clock.nowMillis() - settings.minRetentionThresholdMillis;
    }
  };

  static class HistoryPrunnerSettings {
    private final long pruneThresholdMillis;
    private final long minRetentionThresholdMillis;
    private final int perJobHistoryGoal;

    HistoryPrunnerSettings(
        Amount<Long, Time> inactivePruneThreshold,
        Amount<Long, Time> minRetentionThreshold,
        int perJobHistoryGoal) {

      this.pruneThresholdMillis = inactivePruneThreshold.as(Time.MILLISECONDS);
      this.minRetentionThresholdMillis = minRetentionThreshold.as(Time.MILLISECONDS);
      this.perJobHistoryGoal = perJobHistoryGoal;
    }
  }

  @Inject
  TaskHistoryPruner(
      @AsyncExecutor DelayExecutor executor,
      StateManager stateManager,
      Clock clock,
      HistoryPrunnerSettings settings,
      Storage storage,
      Lifecycle lifecycle,
      TaskEventBatchWorker batchWorker) {

    this.executor = requireNonNull(executor);
    this.stateManager = requireNonNull(stateManager);
    this.clock = requireNonNull(clock);
    this.settings = requireNonNull(settings);
    this.storage = requireNonNull(storage);
    this.lifecycle = requireNonNull(lifecycle);
    this.batchWorker = requireNonNull(batchWorker);
  }

  @VisibleForTesting
  long calculateTimeout(long taskEventTimestampMillis) {
    return Math.max(
        settings.minRetentionThresholdMillis,
        settings.pruneThresholdMillis - Math.max(0, clock.nowMillis() - taskEventTimestampMillis));
  }

  /**
   * When triggered, records an inactive task state change.
   *
   * @param change Event when a task changes state.
   */
  @Subscribe
  public void recordStateChange(TaskStateChange change) {
    if (Tasks.isTerminated(change.getNewState())) {
      long timeoutBasis = change.isTransition()
          ? clock.nowMillis()
          : Iterables.getLast(change.getTask().getTaskEvents()).getTimestamp();
      registerInactiveTask(
          Tasks.getJob(change.getTask()),
          change.getTaskId(),
          calculateTimeout(timeoutBasis));
    }
  }

  private void deleteTasks(final Set<String> taskIds) {
    LOG.info("Pruning inactive tasks " + taskIds);
    batchWorker.execute(storeProvider -> {
      stateManager.deleteTasks(storeProvider, taskIds);
      return BatchWorker.NO_RESULT;
    });
  }

  @VisibleForTesting
  static Query.Builder jobHistoryQuery(IJobKey jobKey) {
    return Query.jobScoped(jobKey).byStatus(apiConstants.TERMINAL_STATES);
  }

  private void registerInactiveTask(
      final IJobKey jobKey,
      final String taskId,
      long timeRemaining) {

    LOG.debug("Prune task {} in {} ms.", taskId, timeRemaining);

    executor.execute(
        shutdownOnError(
            lifecycle,
            LOG,
            String.format(FATAL_ERROR_FORMAT, "task: " + taskId),
            () -> {
              LOG.info("Pruning expired inactive task " + taskId);
              deleteTasks(ImmutableSet.of(taskId));
            }),
        Amount.of(timeRemaining, Time.MILLISECONDS));

    executor.execute(
        shutdownOnError(
            lifecycle,
            LOG,
            String.format(FATAL_ERROR_FORMAT, "job: " + jobKey),
            () -> {
              Iterable<IScheduledTask> inactiveTasks =
                  Storage.Util.fetchTasks(storage, jobHistoryQuery(jobKey));
              int numInactiveTasks = Iterables.size(inactiveTasks);
              int tasksToPrune = numInactiveTasks - settings.perJobHistoryGoal;
              if (tasksToPrune > 0 && numInactiveTasks > settings.perJobHistoryGoal) {
                Set<String> toPrune = FluentIterable
                    .from(Tasks.LATEST_ACTIVITY.sortedCopy(inactiveTasks))
                    .filter(safeToDelete)
                    .limit(tasksToPrune)
                    .transform(Tasks::id)
                    .toSet();
                if (!toPrune.isEmpty()) {
                  deleteTasks(toPrune);
                }
              }
            }));
  }
}
