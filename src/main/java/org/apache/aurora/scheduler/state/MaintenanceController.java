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
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IHostStatus;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  Set<IHostStatus> startMaintenance(Set<String> hosts);

  /**
   * Initiate a drain of all active tasks on {@code hosts}.
   *
   * @param hosts Hosts to drain.
   * @return The adjusted state of the hosts.  Hosts without any active tasks will be immediately
   *         moved to DRAINED.
   */
  Set<IHostStatus> drain(Set<String> hosts);

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
  Set<IHostStatus> getStatus(Set<String> hosts);

  /**
   * Moves {@code hosts} out of maintenance mode, returning them to mode NONE.
   *
   * @param hosts Hosts to move out of maintenance mode.
   * @return The adjusted state of the hosts.
   */
  Set<IHostStatus> endMaintenance(Set<String> hosts);

  class MaintenanceControllerImpl implements MaintenanceController, EventSubscriber {
    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceControllerImpl.class);
    private final Storage storage;
    private final StateManager stateManager;

    @Inject
    public MaintenanceControllerImpl(Storage storage, StateManager stateManager) {
      this.storage = requireNonNull(storage);
      this.stateManager = requireNonNull(stateManager);
    }

    private Set<IHostStatus> watchDrainingTasks(MutableStoreProvider store, Set<String> hosts) {
      LOG.info("Hosts to drain: " + hosts);
      Set<String> emptyHosts = Sets.newHashSet();
      for (String host : hosts) {
        // If there are no tasks on the host, immediately transition to DRAINED.
        Query.Builder query = Query.slaveScoped(host).active();
        Set<String> activeTasks = FluentIterable.from(store.getTaskStore().fetchTasks(query))
            .transform(Tasks::id)
            .toSet();
        if (activeTasks.isEmpty()) {
          LOG.info("No tasks to drain for host: " + host);
          emptyHosts.add(host);
        } else {
          LOG.info("Draining tasks: {} on host: {}", activeTasks, host);
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

      return ImmutableSet.<IHostStatus>builder()
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
        storage.write((NoResult.Quiet) (MutableStoreProvider store) -> {
          // If the task _was_ associated with a draining host, and it was the last task on the
          // host.
          Optional<IHostAttributes> attributes =
              store.getAttributeStore().getHostAttributes(host);
          if (attributes.isPresent() && attributes.get().getMode() == DRAINING) {
            Query.Builder builder = Query.slaveScoped(host).active();
            Iterable<IScheduledTask> activeTasks = store.getTaskStore().fetchTasks(builder);
            if (Iterables.isEmpty(activeTasks)) {
              LOG.info("Moving host {} into DRAINED", host);
              setMaintenanceMode(store, ImmutableSet.of(host), DRAINED);
            } else {
              LOG.info("Host {} is DRAINING with active tasks: {}", host, Tasks.ids(activeTasks));
            }
          }
        });
      }
    }

    @Override
    public Set<IHostStatus> startMaintenance(Set<String> hosts) {
      return storage.write(
          storeProvider -> setMaintenanceMode(storeProvider, hosts, MaintenanceMode.SCHEDULED));
    }

    @VisibleForTesting
    static final Optional<String> DRAINING_MESSAGE =
        Optional.of("Draining machine for maintenance.");

    @Override
    public Set<IHostStatus> drain(Set<String> hosts) {
      return storage.write(store -> watchDrainingTasks(store, hosts));
    }

    private static final Function<IHostAttributes, String> HOST_NAME =
        IHostAttributes::getHost;

    private static final Function<IHostAttributes, IHostStatus> ATTRS_TO_STATUS =
        attributes -> IHostStatus.build(
            new HostStatus().setHost(attributes.getHost()).setMode(attributes.getMode()));

    private static final Function<IHostStatus, MaintenanceMode> GET_MODE = IHostStatus::getMode;

    @Override
    public MaintenanceMode getMode(final String host) {
      return storage.read(storeProvider -> storeProvider.getAttributeStore().getHostAttributes(host)
          .transform(ATTRS_TO_STATUS)
          .transform(GET_MODE)
          .or(MaintenanceMode.NONE));
    }

    @Override
    public Set<IHostStatus> getStatus(final Set<String> hosts) {
      return storage.read(storeProvider -> {
        // Warning - this is filtering _all_ host attributes.  If using this to frequently query
        // for a small set of hosts, a getHostAttributes variant should be added.
        return FluentIterable.from(storeProvider.getAttributeStore().getHostAttributes())
            .filter(Predicates.compose(Predicates.in(hosts), HOST_NAME))
            .transform(ATTRS_TO_STATUS)
            .toSet();
      });
    }

    @Override
    public Set<IHostStatus> endMaintenance(final Set<String> hosts) {
      return storage.write(
          storeProvider -> setMaintenanceMode(storeProvider, hosts, MaintenanceMode.NONE));
    }

    private Set<IHostStatus> setMaintenanceMode(
        MutableStoreProvider storeProvider,
        Set<String> hosts,
        MaintenanceMode mode) {

      AttributeStore.Mutable store = storeProvider.getAttributeStore();
      ImmutableSet.Builder<IHostStatus> statuses = ImmutableSet.builder();
      for (String host : hosts) {
        LOG.info("Setting maintenance mode to {} for host {}", mode, host);
        Optional<IHostAttributes> toSave = AttributeStore.Util.mergeMode(store, host, mode);
        if (toSave.isPresent()) {
          store.saveHostAttributes(toSave.get());
          LOG.info("Updated host attributes: " + toSave.get());
          statuses.add(IHostStatus.build(new HostStatus().setHost(host).setMode(mode)));
        }
      }
      return statuses.build();
    }
  }
}
