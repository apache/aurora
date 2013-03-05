package com.twitter.mesos.scheduler;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;

import com.twitter.common.stats.StatsProvider;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.events.PubsubEvent.TasksDeleted;
import com.twitter.mesos.scheduler.storage.Storage;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * StateManager variables.
 * TODO(William Farner): Rename this class now that it's decoupled from StateManager.
 */
class StateManagerVars implements EventSubscriber {

  private final LoadingCache<String, AtomicLong> vars;

  private final Storage storage;

  @Inject
  StateManagerVars(Storage storage, final StatsProvider statProvider) {
    this.storage = checkNotNull(storage);
    checkNotNull(statProvider);
    vars = CacheBuilder.newBuilder().build(new CacheLoader<String, AtomicLong>() {
      @Override public AtomicLong load(String statName) throws Exception {
        return statProvider.makeCounter(statName);
      }
    });

    // Initialize by-status counters.
    for (ScheduleStatus status : ScheduleStatus.values()) {
      vars.getUnchecked(getVarName(status));
    }
  }

  @VisibleForTesting
  static String getVarName(String role, String job, ScheduleStatus status) {
    return "job_" + role + "_" + job + "_tasks_" + status.name();
  }

  @VisibleForTesting
  static String getVarName(ScheduleStatus status) {
    return "task_store_" + status;
  }

  private AtomicLong getCounter(ScheduleStatus status) {
    return vars.getUnchecked(getVarName(status));
  }

  private AtomicLong getCounter(String role, String job, ScheduleStatus status) {
    return vars.getUnchecked(getVarName(role, job, status));
  }

  private void incrementCount(String role, String job, ScheduleStatus status) {
    getCounter(status).incrementAndGet();
    getCounter(role, job, status).incrementAndGet();
  }

  private void decrementCount(String role, String job, ScheduleStatus status) {
    getCounter(status).decrementAndGet();
    getCounter(role, job, status).decrementAndGet();
  }

  @Subscribe
  public void taskChangedState(TaskStateChange stateChange) {
    ScheduledTask task = stateChange.getTask();
    String role = Tasks.getRole(task);
    String job = Tasks.getJob(task);
    if (stateChange.getOldState() != ScheduleStatus.INIT) {
      decrementCount(role, job, stateChange.getOldState());
    }
    incrementCount(role, job, task.getStatus());
  }

  @Subscribe
  public void storageStarted(StorageStarted event) {
    for (ScheduledTask task : Storage.Util.fetchTasks(storage, Query.GET_ALL)) {
      incrementCount(Tasks.getRole(task), Tasks.getJob(task), task.getStatus());
    }
  }

  @Subscribe
  public void tasksDeleted(final TasksDeleted event) {
    for (ScheduledTask task : event.getTasks()) {
      decrementCount(Tasks.getRole(task), Tasks.getJob(task), task.getStatus());
    }
  }
}
