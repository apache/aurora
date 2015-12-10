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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;

import static java.util.Objects.requireNonNull;

/**
 * Prunes per-job update history on a periodic basis.
 */
class JobUpdateHistoryPruner extends AbstractIdleService {
  private static final Logger LOG = Logger.getLogger(JobUpdateHistoryPruner.class.getName());

  private final Clock clock;
  private final ScheduledExecutorService executor;
  private final Storage storage;
  private final HistoryPrunerSettings settings;

  static class HistoryPrunerSettings {
    private final Amount<Long, Time> pruneInterval;
    private final Amount<Long, Time> maxHistorySize;
    private final int maxUpdatesPerJob;

    HistoryPrunerSettings(
        Amount<Long, Time> pruneInterval,
        Amount<Long, Time> maxHistorySize,
        int maxUpdatesPerJob) {

      this.pruneInterval = requireNonNull(pruneInterval);
      this.maxHistorySize = requireNonNull(maxHistorySize);
      this.maxUpdatesPerJob = maxUpdatesPerJob;
    }
  }

  @Inject
  JobUpdateHistoryPruner(
      Clock clock,
      ScheduledExecutorService executor,
      Storage storage,
      HistoryPrunerSettings settings) {

    this.clock = requireNonNull(clock);
    this.executor = requireNonNull(executor);
    this.storage = requireNonNull(storage);
    this.settings = requireNonNull(settings);
  }

  @Override
  protected void startUp() {
    executor.scheduleAtFixedRate(
        () -> storage.write((NoResult.Quiet) storeProvider -> {
          Set<IJobUpdateKey> prunedUpdates = storeProvider.getJobUpdateStore().pruneHistory(
              settings.maxUpdatesPerJob,
              clock.nowMillis() - settings.maxHistorySize.as(Time.MILLISECONDS));

          LOG.info(prunedUpdates.isEmpty()
              ? "No job update history to prune."
              : "Pruned job update history: " + Joiner.on(",").join(prunedUpdates));
        }),
        settings.pruneInterval.as(Time.MILLISECONDS),
        settings.pruneInterval.as(Time.MILLISECONDS),
        TimeUnit.MILLISECONDS);
  }

  @Override
  protected void shutDown() {
    // Nothing to do - await VM shutdown.
  }
}
