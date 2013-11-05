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
package com.twitter.aurora.scheduler.state;

import java.util.Set;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;

import com.twitter.aurora.gen.HostAttributes;
import com.twitter.aurora.gen.HostStatus;
import com.twitter.aurora.gen.MaintenanceMode;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.events.PubsubEvent;
import com.twitter.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.aurora.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.aurora.scheduler.storage.AttributeStore;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.MutateWork;
import com.twitter.aurora.scheduler.storage.Storage.StoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.Work;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Closures;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.aurora.gen.MaintenanceMode.DRAINED;
import static com.twitter.aurora.gen.MaintenanceMode.DRAINING;

/**
 * Logic that puts hosts into maintenance mode, and triggers draining of hosts upon request.
 * All state-changing functions return their results.  Additionally, all state-changing functions
 * will ignore requests to change state of unknown hosts and subsequently omit these hosts from
 * return values.
 */
public interface MaintenanceController {

  /**
   * Places hosts in maintenance mode.
   * Hosts in maintenance mode are less-preferred for scheduling.
   * No change will be made for hosts that are not recognized, and unrecognized hosts will not be
   * included in the result.
   *
   * @param hosts Hosts to put into maintenance mode.
   * @return The adjusted state of the hosts.
   */
  Set<HostStatus> startMaintenance(Set<String> hosts);

  /**
   * Initiate a drain of all active tasks on {@code hosts}.
   *
   * @param hosts Hosts to drain.
   * @return The adjusted state of the hosts.  Hosts without any active tasks will be immediately
   *         moved to DRAINED.
   */
  Set<HostStatus> drain(Set<String> hosts);

  /**
   * Fetches the current maintenance mode of {$code host}.
   *
   * @param host Host to fetch state for.
   * @return Maintenance mode of host, {@link MaintenanceMode#NONE} if the host is not known.
   */
  MaintenanceMode getMode(String host);

  /**
   * Fetches the current state of {@code hosts}.
   *
   * @param hosts Hosts to fetch state for.
   * @return The state of the hosts.
   */
  Set<HostStatus> getStatus(Set<String> hosts);

  /**
   * Moves {@code hosts} out of maintenance mode, returning them to mode NONE.
   *
   * @param hosts Hosts to move out of maintenance mode.
   * @return The adjusted state of the hosts.
   */
  Set<HostStatus> endMaintenance(Set<String> hosts);

  /**
   * Fetches a mapping from names of DRAINING hosts to the IDs of tasks that are currently being
   * drained from the hosts.
   * This is intended for display/debugging only.
   *
   * @return Draining task IDs, mapped by host name.
   */
  Multimap<String, String> getDrainingTasks();

  class MaintenanceControllerImpl implements MaintenanceController, EventSubscriber {
    private final Storage storage;
    private final StateManager stateManager;
    private final Closure<PubsubEvent> eventSink;
    // Access on views, or non-atomic operations on this map must synchronize on the map itself.
    // Please be careful to avoid securing any external locks when locking on this data structure,
    // however.
    private final Multimap<String, String> drainingTasksByHost =
        Multimaps.synchronizedMultimap(HashMultimap.<String, String>create());

    @Inject
    public MaintenanceControllerImpl(
        Storage storage,
        StateManager stateManager,
        Closure<PubsubEvent> eventSink) {

      this.storage = checkNotNull(storage);
      this.stateManager = checkNotNull(stateManager);
      this.eventSink = checkNotNull(eventSink);
    }

    private Set<HostStatus> watchDrainingTasks(
        MutableStoreProvider store,
        Set<String> hosts,
        Closure<Query.Builder> callback) {

      Set<String> emptyHosts = Sets.newHashSet();
      for (String host : hosts) {
        // If there are no tasks on the host, immediately transition to DRAINED.
        Query.Builder query = Query.slaveScoped(host).active();
        Set<String> activeTasks = FluentIterable.from(store.getTaskStore().fetchTasks(query))
            .transform(Tasks.SCHEDULED_TO_ID)
            .toSet();
        if (activeTasks.isEmpty()) {
          emptyHosts.add(host);
        } else {
          drainingTasksByHost.putAll(host, activeTasks);
          callback.execute(query);
        }
      }

      return ImmutableSet.<HostStatus>builder()
          .addAll(setMaintenanceMode(store, emptyHosts, DRAINED))
          .addAll(setMaintenanceMode(store, Sets.difference(hosts, emptyHosts), DRAINING))
          .build();
    }

    private Set<HostStatus> watchDrainingTasks(MutableStoreProvider store, Set<String> hosts) {
      return watchDrainingTasks(store, hosts, Closures.<Query.Builder>noop());
    }

    private static final Predicate<HostAttributes> IS_DRAINING = new Predicate<HostAttributes>() {
      @Override public boolean apply(HostAttributes attributes) {
        return DRAINING == attributes.getMode();
      }
    };

    /**
     * Notifies the MaintenanceController that storage has started, and maintenance statuses are
     * ready to be loaded.
     *
     * @param started Event.
     */
    @Subscribe
    public void storageStarted(StorageStarted started) {
      storage.write(new MutateWork.NoResult.Quiet() {
        @Override protected void execute(MutableStoreProvider storeProvider) {
          Set<String> drainingHosts =
              FluentIterable.from(storeProvider.getAttributeStore().getHostAttributes())
                  .filter(IS_DRAINING)
                  .transform(HOST_NAME)
                  .toSet();
          watchDrainingTasks(storeProvider, drainingHosts);
        }
      });
    }

    /**
     * Notifies the MaintenanceController that a task has changed state
     *
     * @param change Event
     */
    @Subscribe
    public void taskChangedState(final TaskStateChange change) {
      if (Tasks.isTerminated(change.getNewState())) {
        final String host = change.getTask().getAssignedTask().getSlaveHost();
        storage.write(new MutateWork.NoResult.Quiet() {
          @Override public void execute(MutableStoreProvider store) {
            // If the task _was_ associated with a draining host, and it was the last task on the
            // host.
            boolean drained;
            synchronized (drainingTasksByHost) {
              drained = drainingTasksByHost.remove(host, change.getTaskId())
                  && !drainingTasksByHost.containsKey(host);
            }
            if (drained) {
              setMaintenanceMode(store, ImmutableSet.of(host), DRAINED);
            }
          }
        });
      }
    }

    @Override
    public Set<HostStatus> startMaintenance(final Set<String> hosts) {
      return storage.write(new MutateWork.Quiet<Set<HostStatus>>() {
        @Override public Set<HostStatus> apply(MutableStoreProvider storeProvider) {
          return setMaintenanceMode(storeProvider, hosts, MaintenanceMode.SCHEDULED);
        }
      });
    }

    @VisibleForTesting
    static final Optional<String> DRAINING_MESSAGE =
        Optional.of("Draining machine for maintenance.");

    @Override
    public Set<HostStatus> drain(final Set<String> hosts) {
      return storage.write(new MutateWork.Quiet<Set<HostStatus>>() {
        @Override public Set<HostStatus> apply(MutableStoreProvider store) {
          return watchDrainingTasks(store, hosts, new Closure<Query.Builder>() {
            @Override public void execute(Query.Builder query) {
              stateManager.changeState(query, ScheduleStatus.RESTARTING, DRAINING_MESSAGE);
            }
          });
        }
      });
    }

    private static final Function<HostAttributes, String> HOST_NAME =
        new Function<HostAttributes, String>() {
          @Override public String apply(HostAttributes attributes) {
            return attributes.getHost();
          }
        };

    private static final Function<HostAttributes, HostStatus> ATTRS_TO_STATUS =
        new Function<HostAttributes, HostStatus>() {
          @Override public HostStatus apply(HostAttributes attributes) {
            return new HostStatus().setHost(attributes.getHost()).setMode(attributes.getMode());
          }
        };

    private static final Function<HostStatus, MaintenanceMode> GET_MODE =
      new Function<HostStatus, MaintenanceMode>() {
        @Override public MaintenanceMode apply(HostStatus status) {
          return status.getMode();
        }
      };

    @Override
    public MaintenanceMode getMode(final String host) {
      return storage.weaklyConsistentRead(new Work.Quiet<MaintenanceMode>() {
        @Override public MaintenanceMode apply(StoreProvider storeProvider) {
          return storeProvider.getAttributeStore().getHostAttributes(host)
              .transform(ATTRS_TO_STATUS)
              .transform(GET_MODE)
              .or(MaintenanceMode.NONE);
        }
      });
    }

    @Override
    public Set<HostStatus> getStatus(final Set<String> hosts) {
      return storage.weaklyConsistentRead(new Work.Quiet<Set<HostStatus>>() {
        @Override public Set<HostStatus> apply(StoreProvider storeProvider) {
          // Warning - this is filtering _all_ host attributes.  If using this to frequently query
          // for a small set of hosts, a getHostAttributes variant should be added.
          return FluentIterable.from(storeProvider.getAttributeStore().getHostAttributes())
              .filter(Predicates.compose(Predicates.in(hosts), HOST_NAME))
              .transform(ATTRS_TO_STATUS).toSet();
        }
      });
    }

    @Override
    public Set<HostStatus> endMaintenance(final Set<String> hosts) {
      return storage.write(new MutateWork.Quiet<Set<HostStatus>>() {
        @Override public Set<HostStatus> apply(MutableStoreProvider storeProvider) {
          synchronized (drainingTasksByHost) {
            drainingTasksByHost.keys().removeAll(hosts);
          }
          return setMaintenanceMode(storeProvider, hosts, MaintenanceMode.NONE);
        }
      });
    }

    @Override
    public Multimap<String, String> getDrainingTasks() {
      synchronized (drainingTasksByHost) {
        return ImmutableMultimap.copyOf(drainingTasksByHost);
      }
    }

    private Set<HostStatus> setMaintenanceMode(
        MutableStoreProvider storeProvider,
        Set<String> hosts,
        MaintenanceMode mode) {

      AttributeStore.Mutable store = storeProvider.getAttributeStore();
      ImmutableSet.Builder<HostStatus> statuses = ImmutableSet.builder();
      for (String host : hosts) {
        if (store.setMaintenanceMode(host, mode)) {
          HostStatus status = new HostStatus().setHost(host).setMode(mode);
          eventSink.execute(new PubsubEvent.HostMaintenanceStateChange(status.deepCopy()));
          statuses.add(status);
        }
      }
      return statuses.build();
    }
  }
}
