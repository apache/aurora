/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.inject.Inject;

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
import com.twitter.common.stats.StatsProvider;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.StorageStarted;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A container that tracks and exports stat counters for tasks.
 */
class TaskVars implements EventSubscriber {
  private static final Logger LOG = Logger.getLogger(TaskVars.class.getName());

  // Used to ignore pubsub events sent before storage has completely started.  This avoids a
  // miscount where a StorageStarted consumer is invoked before storageStarted is invoked here,
  // and pubsub events are fired for tasks that we have not yet counted.  For example, if
  // tasksDeleted is invoked, we would end up with a negative count.
  private volatile boolean storageStarted = false;

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
    if (!storageStarted) {
      return;
    }

    IScheduledTask task = stateChange.getTask();
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
    for (IScheduledTask task : Storage.Util.consistentFetchTasks(storage, Query.unscoped())) {
      incrementCount(task.getStatus());
    }

    // Dummy read the counter for each status counter. This is important to guarantee a stat with
    // value zero is present for each state, even if all states are not represented in the task
    // store.
    for (ScheduleStatus status : ScheduleStatus.values()) {
      getCounter(status);
    }
    storageStarted = true;
  }

  @Subscribe
  public void tasksDeleted(final TasksDeleted event) {
    if (!storageStarted) {
      return;
    }

    for (IScheduledTask task : event.getTasks()) {
      decrementCount(task.getStatus());
    }
  }
}
