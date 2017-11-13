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
package org.apache.aurora.scheduler.reconciliation;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.BackoffStrategy;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.async.AsyncModule.AsyncExecutor;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Watches for task transitions into {@link ScheduleStatus#KILLING KILLING} and periodically
 * retries {@link Driver#killTask(String)} until the task transitions.
 */
public class KillRetry implements EventSubscriber {
  private static final Logger LOG = LoggerFactory.getLogger(KillRetry.class);

  @VisibleForTesting
  static final String RETRIES_COUNTER = "task_kill_retries";

  private final Driver driver;
  private final Storage storage;
  private final ScheduledExecutorService executor;
  private final BackoffStrategy backoffStrategy;
  private final AtomicLong killRetries;

  @Inject
  KillRetry(
      Driver driver,
      Storage storage,
      @AsyncExecutor ScheduledExecutorService executor,
      BackoffStrategy backoffStrategy,
      StatsProvider statsProvider) {

    this.driver = requireNonNull(driver);
    this.storage = requireNonNull(storage);
    this.executor = requireNonNull(executor);
    this.backoffStrategy = requireNonNull(backoffStrategy);
    killRetries = statsProvider.makeCounter(RETRIES_COUNTER);
  }

  @Subscribe
  public void taskChangedState(TaskStateChange stateChange) {
    if (stateChange.getNewState() == ScheduleStatus.KILLING) {
      new KillAttempt(stateChange.getTaskId()).tryLater();
    }
  }

  private class KillAttempt implements Runnable {
    private final String taskId;
    private final AtomicLong retryInMs = new AtomicLong();

    KillAttempt(String taskId) {
      this.taskId = taskId;
    }

    void tryLater() {
      retryInMs.set(backoffStrategy.calculateBackoffMs(retryInMs.get()));
      executor.schedule(this, retryInMs.get(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
      Query.Builder query = Query.taskScoped(taskId).byStatus(ScheduleStatus.KILLING);
      if (!Iterables.isEmpty(Storage.Util.fetchTasks(storage, query))) {
        LOG.info("Task " + taskId + " not yet killed, retrying.");

        // Kill did not yet take effect, try again.
        driver.killTask(taskId);
        killRetries.incrementAndGet();
        tryLater();
      }
    }
  }
}
