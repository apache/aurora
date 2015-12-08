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
package org.apache.aurora.scheduler;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import org.apache.aurora.scheduler.events.PubsubEvent.Vetoed;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.filter.SchedulingFilter.VetoGroup;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAttribute;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.util.Objects.requireNonNull;

/**
 * A container that tracks and exports stat counters for tasks.
 */
class TaskVars extends AbstractIdleService implements EventSubscriber {
  private static final Logger LOG = Logger.getLogger(TaskVars.class.getName());
  private static final ImmutableSet<ScheduleStatus> TRACKED_JOB_STATES =
      ImmutableSet.of(ScheduleStatus.LOST, ScheduleStatus.FAILED);

  @VisibleForTesting
  static final Map<VetoGroup, String> VETO_GROUPS_TO_COUNTERS = ImmutableMap.of(
      VetoGroup.STATIC, "scheduling_veto_static",
      VetoGroup.DYNAMIC, "scheduling_veto_dynamic",
      VetoGroup.MIXED, "scheduling_veto_mixed"
  );

  private final LoadingCache<String, Counter> counters;
  private final LoadingCache<String, Counter> untrackedCounters;
  private final Storage storage;
  private volatile boolean exporting = false;

  @Inject
  TaskVars(Storage storage, final StatsProvider statProvider) {
    this.storage = requireNonNull(storage);
    requireNonNull(statProvider);
    counters = buildCache(statProvider);
    untrackedCounters = buildCache(statProvider.untracked());
  }

  private LoadingCache<String, Counter> buildCache(final StatsProvider provider) {
    return CacheBuilder.newBuilder().build(new CacheLoader<String, Counter>() {
      @Override
      public Counter load(String statName) {
        Counter counter = new Counter(provider);
        if (exporting) {
          counter.exportAs(statName);
        }
        return counter;
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

  @VisibleForTesting
  static String jobStatName(IScheduledTask task, ScheduleStatus status) {
    return String.format(
        "tasks_%s_%s",
        status,
        JobKeys.canonicalString(task.getAssignedTask().getTask().getJob()));
  }

  private static final Predicate<IAttribute> IS_RACK = attr -> "rack".equals(attr.getName());

  private static final Function<IAttribute, String> ATTR_VALUE =
      attr -> Iterables.getOnlyElement(attr.getValues());

  private Counter getCounter(ScheduleStatus status) {
    return counters.getUnchecked(getVarName(status));
  }

  private void incrementCount(ScheduleStatus status) {
    getCounter(status).increment();
  }

  private void decrementCount(ScheduleStatus status) {
    getCounter(status).decrement();
  }

  private void updateRackCounters(IScheduledTask task, ScheduleStatus newState) {
    final String host = task.getAssignedTask().getSlaveHost();
    Optional<String> rack;
    if (Strings.isNullOrEmpty(task.getAssignedTask().getSlaveHost())) {
      rack = Optional.absent();
    } else {
      rack = storage.read(storeProvider -> {
        Optional<IAttribute> rack1 = FluentIterable
            .from(AttributeStore.Util.attributesOrNone(storeProvider, host))
            .firstMatch(IS_RACK);
        return rack1.transform(ATTR_VALUE);
      });
    }

    // Always dummy-read the lost-tasks-per-rack stat. This ensures that there is at least a zero
    // exported for all racks.
    if (rack.isPresent()) {
      counters.getUnchecked(rackStatName(rack.get()));
    }

    if (newState == ScheduleStatus.LOST) {
      if (rack.isPresent()) {
        counters.getUnchecked(rackStatName(rack.get())).increment();
      } else {
        LOG.warning("Failed to find rack attribute associated with host " + host);
      }
    }
  }

  private void updateJobCounters(IScheduledTask task, ScheduleStatus newState) {
    if (TRACKED_JOB_STATES.contains(newState)) {
      untrackedCounters.getUnchecked(jobStatName(task, newState)).increment();
    }
  }

  @Subscribe
  public void taskChangedState(TaskStateChange stateChange) {
    IScheduledTask task = stateChange.getTask();
    Optional<ScheduleStatus> previousState = stateChange.getOldState();

    if (stateChange.isTransition() && !previousState.equals(Optional.of(ScheduleStatus.INIT))) {
      decrementCount(previousState.get());
    }
    incrementCount(task.getStatus());

    updateRackCounters(task, task.getStatus());
    updateJobCounters(task, task.getStatus());
  }

  @Override
  protected void startUp() {
    // Dummy read the counter for each status counter. This is important to guarantee a stat with
    // value zero is present for each state, even if all states are not represented in the task
    // store.
    for (ScheduleStatus status : ScheduleStatus.values()) {
      getCounter(status);
    }

    exportCounters(counters.asMap());
    exportCounters(untrackedCounters.asMap());
  }

  @Override
  protected void shutDown() {
    // Ignored. VM shutdown is required to stop exporting task vars.
  }

  private void exportCounters(Map<String, Counter> counterMap) {
    // Initiate export of all counters.  This is not done initially to avoid exporting values that
    // do not represent the entire storage contents.
    exporting = true;
    for (Map.Entry<String, Counter> entry : counterMap.entrySet()) {
      entry.getValue().exportAs(entry.getKey());
    }
  }

  @Subscribe
  public void tasksDeleted(final TasksDeleted event) {
    for (IScheduledTask task : event.getTasks()) {
      decrementCount(task.getStatus());
    }
  }

  @Subscribe
  public void taskVetoed(Vetoed event) {
    VetoGroup vetoGroup = Veto.identifyGroup(event.getVetoes());
    if (vetoGroup != VetoGroup.EMPTY) {
      counters.getUnchecked(VETO_GROUPS_TO_COUNTERS.get(vetoGroup)).increment();
    }
  }

  private static class Counter implements Supplier<Long> {
    private final AtomicLong value = new AtomicLong();
    private boolean exported = false;
    private final StatsProvider stats;

    Counter(StatsProvider stats) {
      this.stats = stats;
    }

    @Override
    public Long get() {
      return value.get();
    }

    private synchronized void exportAs(String name) {
      if (!exported) {
        stats.makeGauge(name, this);
        exported = true;
      }
    }

    private void increment() {
      value.incrementAndGet();
    }

    private void decrement() {
      value.decrementAndGet();
    }
  }
}
