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
package org.apache.aurora.scheduler.storage;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;

import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.TaskStore.Mutable.TaskMutation;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
/**
 * Utility class to contain and perform storage backfill operations.
 */
public final class StorageBackfill {

  private static final Logger LOG = Logger.getLogger(StorageBackfill.class.getName());

  private static final AtomicLong SHARD_SANITY_CHECK_FAILS =
      Stats.exportLong("shard_sanity_check_failures");

  private static final AtomicLong BACKFILLED_TASK_CONFIG_KEYS =
      Stats.exportLong("task_config_keys_backfilled");

  private static final AtomicLong BACKFILLED_JOB_CONFIG_KEYS =
      Stats.exportLong("job_store_task_config_keys_backfilled");

  private StorageBackfill() {
    // Utility class.
  }

  private static void backfillJobDefaults(JobStore.Mutable jobStore) {
    for (String id : jobStore.fetchManagerIds()) {
      for (JobConfiguration job : IJobConfiguration.toBuildersList(jobStore.fetchJobs(id))) {
        populateJobKey(job.getTaskConfig(), BACKFILLED_JOB_CONFIG_KEYS);
        ConfigurationManager.applyDefaultsIfUnset(job);
        jobStore.saveAcceptedJob(id, IJobConfiguration.build(job));
      }
    }
  }

  private static void guaranteeShardUniqueness(
      ScheduledTask task,
      TaskStore.Mutable taskStore,
      Clock clock) {

    if (Tasks.isActive(task.getStatus())) {
      // Perform a sanity check on the number of active shards.
      Query.Builder query = Query.instanceScoped(
          IJobKey.build(task.getAssignedTask().getTask().getJob()),
          task.getAssignedTask().getInstanceId())
          .active();
      Set<String> activeTasksInShard = FluentIterable.from(taskStore.fetchTasks(query))
          .transform(Tasks.SCHEDULED_TO_ID)
          .toSet();

      if (activeTasksInShard.size() > 1) {
        SHARD_SANITY_CHECK_FAILS.incrementAndGet();
        LOG.severe("Active shard sanity check failed when loading " + Tasks.id(task)
            + ", active tasks found: " + activeTasksInShard);

        // We want to keep exactly one task from this shard, so sort the IDs and keep the
        // highest (newest) in the hopes that it is legitimately running.
        String newestTask = Iterables.getLast(Sets.newTreeSet(activeTasksInShard));
        if (Tasks.id(task).equals(newestTask)) {
          LOG.info("Retaining task " + Tasks.id(task));
        } else {
          task.setStatus(ScheduleStatus.KILLED);
          task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.KILLED)
              .setMessage("Killed duplicate shard."));
          // TODO(wfarner); Circle back if this is necessary.  Currently there's a race
          // condition between the time the scheduler is actually available without hitting
          // IllegalStateException (see DriverImpl).
          // driver.killTask(Tasks.id(task));
        }
      }
    }
  }

  private static void rewriteSandboxDeletedState(ScheduledTask task) {
    if (task.getStatus() == ScheduleStatus.SANDBOX_DELETED) {
      List<TaskEvent> events = task.getTaskEvents();
      ScheduleStatus previousStatus = events.get(events.size() - 2).getStatus();

      // Set the status to the previous event and drop the last event.
      task.setStatus(previousStatus);
      events.remove(events.size() - 1);

      LOG.info("Rewriting SANDBOX_DELETED status to " + previousStatus + " for " + Tasks.id(task));
    }
  }

  private static void populateJobKey(TaskConfig config, AtomicLong counter) {
    if (!config.isSetJob() || !JobKeys.isValid(IJobKey.build(config.getJob()))) {
      config.setJob(new JobKey()
          .setRole(config.getOwner().getRole())
          .setEnvironment(config.getEnvironment())
          .setName(config.getJobName()));

      counter.incrementAndGet();
    }
  }

  /**
   * Backfills the storage to make it match any assumptions that may have changed since
   * the structs were first written.
   *
   * @param storeProvider Storage provider.
   * @param clock Clock, used for timestamping backfilled task events.
   */
  public static void backfill(final MutableStoreProvider storeProvider, final Clock clock) {
    backfillJobDefaults(storeProvider.getJobStore());

    // Backfilling job keys has to be done in a separate transaction to ensure follow up scoped
    // Query calls work against upgraded MemTaskStore, which does not support deprecated fields.
    LOG.info("Backfilling task config job keys.");
    storeProvider.getUnsafeTaskStore().mutateTasks(Query.unscoped(), new TaskMutation() {
      @Override
      public IScheduledTask apply(final IScheduledTask task) {
        ScheduledTask builder = task.newBuilder();
        populateJobKey(builder.getAssignedTask().getTask(), BACKFILLED_TASK_CONFIG_KEYS);
        return IScheduledTask.build(builder);
      }
    });

    LOG.info("Performing shard uniqueness sanity check.");
    storeProvider.getUnsafeTaskStore().mutateTasks(Query.unscoped(), new TaskMutation() {
      @Override
      public IScheduledTask apply(final IScheduledTask task) {
        ScheduledTask builder = task.newBuilder();
        ConfigurationManager.applyDefaultsIfUnset(builder.getAssignedTask().getTask());
        // TODO(ksweeney): Guarantee tasks pass current validation code here and quarantine if they
        // don't.
        guaranteeShardUniqueness(builder, storeProvider.getUnsafeTaskStore(), clock);
        rewriteSandboxDeletedState(builder);
        return IScheduledTask.build(builder);
      }
    });
  }
}
