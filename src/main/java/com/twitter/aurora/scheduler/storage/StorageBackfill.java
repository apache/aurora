/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.aurora.scheduler.storage;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.google.common.base.Charsets;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.ExecutorConfig;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskEvent;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.configuration.ConfigurationManager;
import com.twitter.aurora.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.aurora.scheduler.storage.TaskStore.Mutable.TaskMutation;
import com.twitter.aurora.scheduler.storage.entities.IJobConfiguration;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;

/**
 * Utility class to contain and perform storage backfill operations.
 */
public final class StorageBackfill {

  private static final Logger LOG = Logger.getLogger(StorageBackfill.class.getName());

  private static final AtomicLong SHARD_SANITY_CHECK_FAILS =
      Stats.exportLong("shard_sanity_check_failures");

  private StorageBackfill() {
    // Utility class.
  }

  private static void backfillJobDefaults(JobStore.Mutable jobStore) {
    for (String id : jobStore.fetchManagerIds()) {
      for (JobConfiguration job : IJobConfiguration.toBuildersList(jobStore.fetchJobs(id))) {
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
      TaskConfig config = task.getAssignedTask().getTask();
      Query.Builder query = Query.instanceScoped(
          JobKeys.from(config.getOwner().getRole(), config.getEnvironment(), config.getJobName()),
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
        if (!Tasks.id(task).equals(newestTask)) {
          task.setStatus(ScheduleStatus.KILLED);
          task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.KILLED)
              .setMessage("Killed duplicate shard."));
          // TODO(wfarner); Circle back if this is necessary.  Currently there's a race
          // condition between the time the scheduler is actually available without hitting
          // IllegalStateException (see DriverImpl).
          // driver.killTask(Tasks.id(task));
        } else {
          LOG.info("Retaining task " + Tasks.id(task));
        }
      }
    }
  }

  private static final AtomicLong BOTH_CONFIGS_SET = Stats.exportLong("both_configs_set");
  private static final AtomicLong OLD_CONFIG_SET = Stats.exportLong("old_conifg_id_set");
  private static final AtomicLong NEW_CONFIG_SET = Stats.exportLong("new_config_set");
  private static final AtomicLong CONFIGS_INCONSISTENT = Stats.exportLong("configs_inconsistent");

  private static final AtomicLong JOB_BOTH_CONFIGS_SET = Stats.exportLong("job_both_configs_set");
  private static final AtomicLong JOB_OLD_CONFIG_SET = Stats.exportLong("job_old_conifg_id_set");
  private static final AtomicLong JOB_NEW_CONFIG_SET = Stats.exportLong("job_new_config_set");
  private static final AtomicLong JOB_CONFIGS_INCONSISTENT =
      Stats.exportLong("job_configs_inconsistent");

  private static void mutateTaskConfig(
      TaskConfig taskConfig,
      AtomicLong bothSet,
      AtomicLong oldSet,
      AtomicLong newSet,
      AtomicLong inconsistent) {
    boolean oldFieldSet = taskConfig.isSetThermosConfig();
    boolean newFieldSet = taskConfig.isSetExecutorConfig();
    if (oldFieldSet && newFieldSet) {
      bothSet.incrementAndGet();
      if (!taskConfig.getExecutorConfig().getData().equals(
          new String(taskConfig.getThermosConfig(), Charsets.UTF_8))) {

        inconsistent.incrementAndGet();
      }
    } else if (oldFieldSet) {
      oldSet.incrementAndGet();
      taskConfig.setExecutorConfig(
          new ExecutorConfig(
              "AuroraExecutor",
              new String(taskConfig.getThermosConfig(), Charsets.UTF_8)));
    } else if (newFieldSet) {
      newSet.incrementAndGet();
      taskConfig.setThermosConfig(
          taskConfig.getExecutorConfig().getData().getBytes(Charsets.UTF_8));
    } else {
      throw new IllegalStateException(
          "Task for " + taskConfig.getJobName() + " does not have a thermos config.");
    }
  }

  private static void convertToExecutorConfig(ScheduledTask task) {
    mutateTaskConfig(
        task.getAssignedTask().getTask(),
        BOTH_CONFIGS_SET,
        OLD_CONFIG_SET,
        NEW_CONFIG_SET,
        CONFIGS_INCONSISTENT);
  }

  private static void convertToExecutorConfig(JobStore.Mutable jobStore) {
    for (String id : jobStore.fetchManagerIds()) {
      for (JobConfiguration job : IJobConfiguration.toBuildersList(jobStore.fetchJobs(id))) {
        mutateTaskConfig(
            job.getTaskConfig(),
            JOB_BOTH_CONFIGS_SET,
            JOB_OLD_CONFIG_SET,
            JOB_NEW_CONFIG_SET,
            JOB_CONFIGS_INCONSISTENT);
        jobStore.saveAcceptedJob(id, IJobConfiguration.build(job));
      }
    }
  }

  private static final AtomicLong BOTH_FIELDS_SET = Stats.exportLong("both_instance_ids_set");
  private static final AtomicLong OLD_FIELD_SET = Stats.exportLong("old_instance_id_set");
  private static final AtomicLong NEW_FIELD_SET = Stats.exportLong("new_instance_id_set");
  private static final AtomicLong FIELDS_INCONSISTENT =
      Stats.exportLong("instance_ids_inconsistent");

  /**
   * Ensures backwards-compatible data is present for both the new and deprecated instance ID
   * fields.
   *
   * @param task Task to possibly modify when ensuring backwards compatibility.
   */
  public static void dualWriteInstanceId(AssignedTask task) {
    boolean oldFieldSet = task.getTask().isSetInstanceIdDEPRECATED();
    boolean newFieldSet = task.isSetInstanceId();
    if (oldFieldSet && newFieldSet) {
      BOTH_FIELDS_SET.incrementAndGet();
      if (task.getInstanceId() != task.getTask().getInstanceIdDEPRECATED()) {
        FIELDS_INCONSISTENT.incrementAndGet();
      }
    } else if (oldFieldSet) {
      OLD_FIELD_SET.incrementAndGet();
      task.setInstanceId(task.getTask().getInstanceIdDEPRECATED());
    } else if (newFieldSet) {
      NEW_FIELD_SET.incrementAndGet();
      task.getTask().setInstanceIdDEPRECATED(task.getInstanceId());
    } else {
      throw new IllegalStateException(
          "Task " + task.getTaskId() + " does not have an instance id.");
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
    convertToExecutorConfig(storeProvider.getJobStore());

    LOG.info("Performing shard uniqueness sanity check.");
    storeProvider.getUnsafeTaskStore().mutateTasks(Query.unscoped(), new TaskMutation() {
      @Override public IScheduledTask apply(final IScheduledTask task) {
        ScheduledTask builder = task.newBuilder();
        ConfigurationManager.applyDefaultsIfUnset(builder.getAssignedTask().getTask());
        // TODO(ksweeney): Guarantee tasks pass current validation code here and quarantine if they
        // don't.
        guaranteeShardUniqueness(builder, storeProvider.getUnsafeTaskStore(), clock);
        convertToExecutorConfig(builder);
        dualWriteInstanceId(builder.getAssignedTask());
        return IScheduledTask.build(builder);
      }
    });
  }
}
