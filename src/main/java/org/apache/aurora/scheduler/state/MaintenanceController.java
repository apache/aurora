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
package org.apache.aurora.scheduler.state;

import java.util.Set;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;

import org.apache.aurora.gen.HostStatus;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.MaintenanceMode.DRAINED;
import static org.apache.aurora.gen.MaintenanceMode.DRAINING;

/**
 * Logic that puts hosts into maintenance mode, and triggers draining of hosts upon request.
 * All state-changing functions return their results.  Additionally, all state-changing functions
 * will ignore requests to change state of unknown hosts and subsequently omit these hosts from
 * return values.
 * TODO(wfarner): Convert use of HostStatus in this API to IHostStatus (immutable).
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

  class MaintenanceControllerImpl implements MaintenanceController, EventSubscriber {
    private static final Logger LOG = Logger.getLogger(MaintenanceControllerImpl.class.getName());
    private final Storage storage;
    private final StateManager stateManager;

    @Inject
    public MaintenanceControllerImpl(Storage storage, StateManager stateManager) {
      this.storage = requireNonNull(storage);
      this.stateManager = requireNonNull(stateManager);
    }

    private Set<HostStatus> watchDrainingTasks(MutableStoreProvider store, Set<String> hosts) {
      LOG.info("Hosts to drain: " + hosts);
      Set<String> emptyHosts = Sets.newHashSet();
      for (String host : hosts) {
        // If there are no tasks on the host, immediately transition to DRAINED.
        Query.Builder query = Query.slaveScoped(host).active();
        Set<String> activeTasks = FluentIterable.from(store.getTaskStore().fetchTasks(query))
            .transform(Tasks.SCHEDULED_TO_ID)
            .toSet();
        if (activeTasks.isEmpty()) {
          LOG.info("No tasks to drain for host: " + host);
          emptyHosts.add(host);
        } else {
          LOG.info(String.format("Draining tasks: %s on host: %s", activeTasks, host));
          for (String taskId : activeTasks) {
            stateManager.changeState(
                store,
                taskId,
                Optional.absent(),
                ScheduleStatus.DRAINING,
                DRAINING_MESSAGE);
          }
        }
      }

      return ImmutableSet.<HostStatus>builder()
          .addAll(setMaintenanceMode(store, emptyHosts, DRAINED))
          .addAll(setMaintenanceMode(store, Sets.difference(hosts, emptyHosts), DRAINING))
          .build();
    }

    /**
     * Notifies the MaintenanceController that a task has changed state.
     *
     * @param change Event
     */
    @Subscribe
    public void taskChangedState(final TaskStateChange change) {
      if (Tasks.isTerminated(change.getNewState())) {
        final String host = change.getTask().getAssignedTask().getSlaveHost();
        storage.write(new MutateWork.NoResult.Quiet() {
          @Override
          public void execute(MutableStoreProvider store) {
            // If the task _was_ associated with a draining host, and it was the last task on the
            // host.
            Optional<IHostAttributes> attributes =
                store.getAttributeStore().getHostAttributes(host);
            if (attributes.isPresent() && attributes.get().getMode() == DRAINING) {
              Query.Builder builder = Query.slaveScoped(host).active();
              Iterable<IScheduledTask> activeTasks = store.getTaskStore().fetchTasks(builder);
              if (Iterables.isEmpty(activeTasks)) {
                LOG.info(String.format("Moving host %s into DRAINED", host));
                setMaintenanceMode(store, ImmutableSet.of(host), DRAINED);
              } else {
                LOG.info(String.format("Host %s is DRAINING with active tasks: %s",
                    host,
                    Tasks.ids(activeTasks)));
              }
            }
          }
        });
      }
    }

    @Override
    public Set<HostStatus> startMaintenance(final Set<String> hosts) {
      return storage.write(new MutateWork.Quiet<Set<HostStatus>>() {
        @Override
        public Set<HostStatus> apply(MutableStoreProvider storeProvider) {
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
        @Override
        public Set<HostStatus> apply(MutableStoreProvider store) {
          return watchDrainingTasks(store, hosts);
        }
      });
    }

    private static final Function<IHostAttributes, String> HOST_NAME =
        new Function<IHostAttributes, String>() {
          @Override
          public String apply(IHostAttributes attributes) {
            return attributes.getHost();
          }
        };

    private static final Function<IHostAttributes, HostStatus> ATTRS_TO_STATUS =
        new Function<IHostAttributes, HostStatus>() {
          @Override
          public HostStatus apply(IHostAttributes attributes) {
            return new HostStatus().setHost(attributes.getHost()).setMode(attributes.getMode());
          }
        };

    private static final Function<HostStatus, MaintenanceMode> GET_MODE =
        new Function<HostStatus, MaintenanceMode>() {
          @Override
          public MaintenanceMode apply(HostStatus status) {
            return status.getMode();
          }
        };

    @Override
    public MaintenanceMode getMode(final String host) {
      return storage.read(new Work.Quiet<MaintenanceMode>() {
        @Override
        public MaintenanceMode apply(StoreProvider storeProvider) {
          return storeProvider.getAttributeStore().getHostAttributes(host)
              .transform(ATTRS_TO_STATUS)
              .transform(GET_MODE)
              .or(MaintenanceMode.NONE);
        }
      });
    }

    @Override
    public Set<HostStatus> getStatus(final Set<String> hosts) {
      return storage.read(new Work.Quiet<Set<HostStatus>>() {
        @Override
        public Set<HostStatus> apply(StoreProvider storeProvider) {
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
        @Override
        public Set<HostStatus> apply(MutableStoreProvider storeProvider) {
          return setMaintenanceMode(storeProvider, hosts, MaintenanceMode.NONE);
        }
      });
    }

    private Set<HostStatus> setMaintenanceMode(
        MutableStoreProvider storeProvider,
        Set<String> hosts,
        MaintenanceMode mode) {

      AttributeStore.Mutable store = storeProvider.getAttributeStore();
      ImmutableSet.Builder<HostStatus> statuses = ImmutableSet.builder();
      for (String host : hosts) {
        LOG.info(String.format("Setting maintenance mode to %s for host %s", mode, host));
        Optional<IHostAttributes> toSave = AttributeStore.Util.mergeMode(store, host, mode);
        if (toSave.isPresent()) {
          store.saveHostAttributes(toSave.get());
          LOG.info("Updated host attributes: " + toSave.get());
          HostStatus status = new HostStatus().setHost(host).setMode(mode);
          statuses.add(status);
        }
      }
      return statuses.build();
    }
  }
}
