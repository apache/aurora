package com.twitter.mesos.scheduler;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;

import com.twitter.common.stats.StatsProvider;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.scheduler.base.Query;
import com.twitter.mesos.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.events.PubsubEvent.TasksDeleted;
import com.twitter.mesos.scheduler.storage.AttributeStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A container that tracks and exports stat counters for tasks.
 */
class TaskVars implements EventSubscriber {
  private static final Logger LOG = Logger.getLogger(TaskVars.class.getName());

  private final LoadingCache<String, AtomicLong> countersByStatus;
  private final LoadingCache<String, AtomicLong> countersByRack;

  private final Storage storage;

  @Inject
  TaskVars(Storage storage, final StatsProvider statProvider) {
    this.storage = checkNotNull(storage);
    checkNotNull(statProvider);
    countersByStatus = CacheBuilder.newBuilder().build(new CacheLoader<String, AtomicLong>() {
      @Override public AtomicLong load(String statName) {
        return statProvider.makeCounter(statName);
      }
    });
    countersByRack = CacheBuilder.newBuilder().build(new CacheLoader<String, AtomicLong>() {
      @Override public AtomicLong load(String rack) {
        return statProvider.makeCounter(rackStatName(rack));
      }
    });
  }

  @VisibleForTesting
  static String getVarName(ScheduleStatus status) {
    return "task_store_" + status;
  }

  @VisibleForTesting
  static String rackStatName(String rack) {
    return "tasks_lost_rack_" + rack;
  }

  private static final Predicate<Attribute> IS_RACK = new Predicate<Attribute>() {
    @Override public boolean apply(Attribute attr) {
      return "rack".equals(attr.getName());
    }
  };

  private static final Function<Attribute, String> ATTR_VALUE = new Function<Attribute, String>() {
    @Override public String apply(Attribute attr) {
      return Iterables.getOnlyElement(attr.getValues());
    }
  };

  private AtomicLong getCounter(ScheduleStatus status) {
    return countersByStatus.getUnchecked(getVarName(status));
  }

  private void incrementCount(ScheduleStatus status) {
    getCounter(status).incrementAndGet();
  }

  private void decrementCount(ScheduleStatus status) {
    getCounter(status).decrementAndGet();
  }

  @Subscribe
  public void taskChangedState(TaskStateChange stateChange) {
    ScheduledTask task = stateChange.getTask();
    if (stateChange.getOldState() != ScheduleStatus.INIT) {
      decrementCount(stateChange.getOldState());
    }
    incrementCount(task.getStatus());

    if (stateChange.getNewState() == ScheduleStatus.LOST) {
      final String host = stateChange.getTask().getAssignedTask().getSlaveHost();
      Optional<String> rack = storage.consistentRead(new Work.Quiet<Optional<String>>() {
        @Override public Optional<String> apply(StoreProvider storeProvider) {
          Optional<Attribute> rack = FluentIterable
              .from(AttributeStore.Util.attributesOrNone(storeProvider, host))
              .firstMatch(IS_RACK);
          return rack.transform(ATTR_VALUE);
        }
      });

      if (rack.isPresent()) {
        countersByRack.getUnchecked(rack.get()).incrementAndGet();
      } else {
        LOG.warning("Failed to find rack attribute associated with host " + host);
      }
    }
  }

  @Subscribe
  public void storageStarted(StorageStarted event) {
    for (ScheduledTask task : Storage.Util.consistentFetchTasks(storage, Query.GET_ALL)) {
      incrementCount(task.getStatus());
    }

    // Dummy read the counter for each status counter. This is important to guarantee a stat with
    // value zero is present for each state, even if all states are not represented in the task
    // store.
    for (ScheduleStatus status : ScheduleStatus.values()) {
      getCounter(status);
    }
  }

  @Subscribe
  public void tasksDeleted(final TasksDeleted event) {
    for (ScheduledTask task : event.getTasks()) {
      decrementCount(task.getStatus());
    }
  }
}
