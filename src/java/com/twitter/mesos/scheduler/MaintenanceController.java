package com.twitter.mesos.scheduler;

import java.util.Set;

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
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;

import com.twitter.common.base.Closure;
import com.twitter.common.base.Closures;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.HostStatus;
import com.twitter.mesos.gen.MaintenanceMode;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.storage.AttributeStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.mesos.gen.MaintenanceMode.DRAINED;
import static com.twitter.mesos.gen.MaintenanceMode.DRAINING;

/**
 * Logic that puts hosts into maintenance mode, and triggers draining of hosts upon request.
 * All state-changing functions return their results.  Additionally, all state-changing functions
 * will ignore requests to change state of unknown hosts and subsequently omit these hosts from
 * return values.
 *
 * TODO(William Farner): Augment the scheduler to use adjust scheduling based on a host's mode.
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
    private final Multimap<String, String> drainingTasksByHost = HashMultimap.create();

    @Inject
    MaintenanceControllerImpl(Storage storage, StateManager stateManager) {
      this.storage = checkNotNull(storage);
      this.stateManager = checkNotNull(stateManager);
    }

    private Set<HostStatus> watchDrainingTasks(
        MutableStoreProvider store,
        Set<String> hosts,
        Closure<TaskQuery> callback) {

      Set<String> emptyHosts = Sets.newHashSet();
      for (String host : hosts) {
        // If there are no tasks on the host, immediately transition to DRAINED.
        TaskQuery query = new TaskQuery().setStatuses(Tasks.ACTIVE_STATES).setSlaveHost(host);
        Set<String> activeTasks = store.getTaskStore().fetchTaskIds(query);
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
      return watchDrainingTasks(store, hosts, Closures.<TaskQuery>noop());
    }

    private static final Predicate<HostAttributes> IS_DRAINING = new Predicate<HostAttributes>() {
      @Override public boolean apply(HostAttributes attributes) {
        return DRAINING == attributes.getMode();
      }
    };

    @Subscribe
    public synchronized void storageStarted(StorageStarted started) {
      storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
        @Override protected void execute(MutableStoreProvider storeProvider) {
          Set<String> drainingHosts =
              FluentIterable.from(storeProvider.getAttributeStore().getHostAttributes())
                  .filter(IS_DRAINING)
                  .transform(HOST_NAME)
                  .toImmutableSet();
          watchDrainingTasks(storeProvider, drainingHosts);
        }
      });
    }

    @Subscribe
    public synchronized void taskChangedState(TaskStateChange change) {
      if (Tasks.isTerminated(change.getNewState())) {
        final String host = change.getTask().getAssignedTask().getSlaveHost();

        // If the task _was_ associated with a draining host, and it was the last task on the host.
        if (drainingTasksByHost.remove(host, change.getTaskId())
            && !drainingTasksByHost.containsKey(host)) {

          storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
            @Override public void execute(MutableStoreProvider store) {
              setMaintenanceMode(store, ImmutableSet.of(host), DRAINED);
            }
          });
        }
      }
    }

    @Override
    public Set<HostStatus> startMaintenance(final Set<String> hosts) {
      return storage.doInWriteTransaction(new MutateWork.Quiet<Set<HostStatus>>() {
        @Override public Set<HostStatus> apply(MutableStoreProvider storeProvider) {
          return setMaintenanceMode(storeProvider, hosts, MaintenanceMode.SCHEDULED);
        }
      });
    }

    @VisibleForTesting
    static final Optional<String> DRAINING_MESSAGE =
        Optional.of("Draining machine for maintenance.");

    @Override
    public synchronized Set<HostStatus> drain(final Set<String> hosts) {
      return storage.doInWriteTransaction(new MutateWork.Quiet<Set<HostStatus>>() {
        @Override public Set<HostStatus> apply(MutableStoreProvider store) {
          return watchDrainingTasks(store, hosts, new Closure<TaskQuery>() {
            @Override public void execute(TaskQuery query) {
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

    @Override
    public Set<HostStatus> getStatus(final Set<String> hosts) {
      return storage.doInTransaction(new Work.Quiet<Set<HostStatus>>() {
        @Override public Set<HostStatus> apply(StoreProvider storeProvider) {
          return FluentIterable.from(storeProvider.getAttributeStore().getHostAttributes())
              .filter(Predicates.compose(Predicates.in(hosts), HOST_NAME))
              .transform(ATTRS_TO_STATUS).toImmutableSet();
        }
      });
    }

    @Override
    public synchronized Set<HostStatus> endMaintenance(final Set<String> hosts) {
      return storage.doInWriteTransaction(new MutateWork.Quiet<Set<HostStatus>>() {
        @Override public Set<HostStatus> apply(MutableStoreProvider storeProvider) {
          drainingTasksByHost.keys().removeAll(hosts);
          return setMaintenanceMode(storeProvider, hosts, MaintenanceMode.NONE);
        }
      });
    }

    @Override
    public Multimap<String, String> getDrainingTasks() {
      return ImmutableMultimap.copyOf(drainingTasksByHost);
    }

    private Set<HostStatus> setMaintenanceMode(
        MutableStoreProvider storeProvider,
        Set<String> hosts,
        MaintenanceMode mode) {

      AttributeStore.Mutable store = storeProvider.getAttributeStore();
      ImmutableSet.Builder<HostStatus> statuses = ImmutableSet.builder();
      for (String host : hosts) {
        if (store.setMaintenanceMode(host, mode)) {
          statuses.add(new HostStatus().setHost(host).setMode(mode));
        }
      }
      return statuses.build();
    }
  }
}
