package com.twitter.mesos.scheduler.storage;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import com.twitter.common.base.Closure;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;

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
      for (JobConfiguration job : jobStore.fetchJobs(id)) {
        ConfigurationManager.applyDefaultsIfUnset(job);
        jobStore.saveAcceptedJob(id, job);
      }
    }
  }

  private static void guaranteeTaskHasEvents(ScheduledTask task, Clock clock) {
    TaskEvent latestEvent = task.isSetTaskEvents()
        ? Iterables.getLast(task.getTaskEvents(), null) : null;
    if ((latestEvent == null) || (latestEvent.getStatus() != task.getStatus())) {
      LOG.severe("Task " + Tasks.id(task) + " has no event for current status.");
      task.addToTaskEvents(new TaskEvent(clock.nowMillis(), task.getStatus())
          .setMessage("Synthesized missing event."));
    }
  }

  private static void guaranteeShardUniqueness(
      ScheduledTask task,
      TaskStore taskStore,
      Clock clock) {

    if (Tasks.isActive(task.getStatus())) {
      // Perform a sanity check on the number of active shards.
      Set<String> activeTasksInShard = activeShards(
          taskStore,
          Tasks.getRole(task),
          Tasks.getJob(task),
          Tasks.SCHEDULED_TO_SHARD_ID.apply(task));

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

  /**
   * Backfills the storage to make it match any assumptions that may have changed since
   * the structs were first written.
   *
   * @param storeProvider Storage provider.
   * @param clock Clock, used for timestamping backfilled task events.
   */
  public static void backfill(final MutableStoreProvider storeProvider, final Clock clock) {
    backfillJobDefaults(storeProvider.getJobStore());

    LOG.info("Performing shard uniqueness sanity check.");
    storeProvider.getUnsafeTaskStore().mutateTasks(Query.GET_ALL, new Closure<ScheduledTask>() {
      @Override public void execute(final ScheduledTask task) {
        ConfigurationManager.applyDefaultsIfUnset(task.getAssignedTask().getTask());
        guaranteeTaskHasEvents(task, clock);
        guaranteeShardUniqueness(task, storeProvider.getUnsafeTaskStore(), clock);
      }
    });
  }

  private static Set<String> activeShards(
      TaskStore taskStore,
      String role,
      String job,
      int shardId) {

    return taskStore.fetchTaskIds(Query.shardScoped(role, job, shardId).active().get());
  }
}
