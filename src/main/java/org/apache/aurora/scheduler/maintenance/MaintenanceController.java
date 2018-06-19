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
package org.apache.aurora.scheduler.maintenance;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractScheduledService;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.gen.HostMaintenanceRequest;
import org.apache.aurora.gen.HostStatus;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.PercentageSlaPolicy;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.SlaPolicy;
import org.apache.aurora.scheduler.BatchWorker;
import org.apache.aurora.scheduler.SchedulerModule.TaskEventBatchWorker;
import org.apache.aurora.scheduler.base.InstanceKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.sla.SlaManager;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IHostMaintenanceRequest;
import org.apache.aurora.scheduler.storage.entities.IHostStatus;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ISlaPolicy;
import org.apache.mesos.v1.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.apache.aurora.gen.MaintenanceMode.DRAINED;
import static org.apache.aurora.gen.MaintenanceMode.DRAINING;

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
   * Initiate an SLA-aware drain of all active tasks on {@code hosts}.
   *
   * @param hosts Hosts to drain.
   * @param defaultSlaPolicy SlaPolicy to use if a task does not have an SlaPolicy.
   * @param timeoutSecs Interval after which tasks will be forcefully drained without checking SLA.
   * @return The adjusted state of the hosts. Hosts without any active tasks will be immediately
   *         moved to DRAINED.
   */
  Set<IHostStatus> slaDrain(Set<String> hosts, SlaPolicy defaultSlaPolicy, long timeoutSecs);

  /**
   * Drain tasks defined by the inverse offer.
   * This method doesn't set any host attributes.
   *
   * @param inverseOffer the inverse offer to use.
   */
  void drainForInverseOffer(Protos.InverseOffer inverseOffer);

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

  /**
   * Records the maintenance requests for hosts and drains any active tasks from the host
   * asynchronously.
   *
   * Tasks are drained iff it will satisfy the required SLA for the task. Task's SLA is either the
   * {@link SlaPolicy} configured as part of the TaskConfig or the default {@link SlaPolicy}
   * specified as part of the maintenance request. If neither then the task is drained immediately.
   *
   * In order to avoid tasks from blocking maintenance perpetually each maintenance request has a
   * timeout after which all tasks forcefully drained.
   */
  class MaintenanceControllerImpl
      extends AbstractScheduledService implements MaintenanceController, EventSubscriber {
    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceControllerImpl.class);

    @VisibleForTesting
    @Qualifier
    @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
    public @interface PollingInterval { }

    @VisibleForTesting
    static final String DRAINING_MESSAGE = "Draining machine for maintenance.";

    private static final String MAINTENANCE_COUNTDOWN_STAT_NAME = "maintenance_countdown_ms";
    private static final String MISSING_MAINTENANCE_REQUEST = "missing_maintenance_request";
    private static final SlaPolicy ZERO_PERCENT_SLA = SlaPolicy.percentageSlaPolicy(
        new PercentageSlaPolicy()
            .setPercentage(0)
            .setDurationSecs(0));

    private final Storage storage;
    private final Amount<Long, Time> pollingInterval;
    private final TaskEventBatchWorker batchWorker;
    private final SlaManager slaManager;
    private final StateManager stateManager;

    private final AtomicLong missingMaintenanceCounter;
    private final LoadingCache<String, AtomicLong> maintenanceCountDownByTask;

    @Inject
    public MaintenanceControllerImpl(
        Storage storage,
        @PollingInterval Amount<Long, Time> pollingInterval,
        TaskEventBatchWorker batchWorker,
        SlaManager slaManager,
        StateManager stateManager,
        StatsProvider statsProvider) {

      this.storage = requireNonNull(storage);
      this.pollingInterval = checkNotNull(pollingInterval);
      this.batchWorker = requireNonNull(batchWorker);
      this.slaManager = requireNonNull(slaManager);
      this.stateManager = requireNonNull(stateManager);
      this.missingMaintenanceCounter = statsProvider.makeCounter(MISSING_MAINTENANCE_REQUEST);
      this.maintenanceCountDownByTask = CacheBuilder.newBuilder().build(
          new CacheLoader<String, AtomicLong>() {
            @Override
            public AtomicLong load(String key) {
              return statsProvider.makeCounter(key);
            }
          }
      );
    }

    private Set<String> drainTasksOnHost(String host, StoreProvider store) {
      Query.Builder query = Query.slaveScoped(host).active();

      List<IScheduledTask> candidates = new ArrayList<>(store.getTaskStore().fetchTasks(query));

      if (candidates.isEmpty()) {
        LOG.info("No tasks to drain on host: {}", host);
        return Collections.emptySet();
      }

      // shuffle the candidates to avoid head-of-line blocking
      Collections.shuffle(candidates);
      candidates.forEach(task -> {
        try {
          drainTask(task, store);
        } catch (ExecutionException e) {
          LOG.error("Exception when trying to drain task: {}", Tasks.id(task), e);
        }
      });

      return candidates.stream().map(Tasks::id).collect(Collectors.toSet());
    }

    private Set<IHostStatus> watchDrainingTasks(MutableStoreProvider store, Set<String> hosts) {
      LOG.info("Hosts to drain: " + hosts);
      Set<String> emptyHosts = Sets.newHashSet();
      for (String host : hosts) {
        Set<String> drainedTasks = drainTasksOnHost(host, store);
        // If there are no tasks on the host, immediately transition to DRAINED.
        if (drainedTasks.isEmpty()) {
          emptyHosts.add(host);
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
        batchWorker.execute(store -> {
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
              store.getHostMaintenanceStore().removeHostMaintenanceRequest(host);
            } else {
              LOG.info("Host {} is DRAINING with active tasks: {}", host, Tasks.ids(activeTasks));
            }
          }
          return BatchWorker.NO_RESULT;
        });
      }
    }

    @Override
    public Set<IHostStatus> startMaintenance(Set<String> hosts) {
      return storage.write(
          storeProvider -> setMaintenanceMode(storeProvider, hosts, MaintenanceMode.SCHEDULED));
    }

    private void recordMaintenanceRequests(
        MutableStoreProvider store,
        Set<String> hosts,
        SlaPolicy defaultSlaPolicy,
        long timeoutSecs) {

      hosts.forEach(
          host -> store.getHostMaintenanceStore().saveHostMaintenanceRequest(
              IHostMaintenanceRequest.build(
                  new HostMaintenanceRequest()
                      .setHost(host)
                      .setDefaultSlaPolicy(defaultSlaPolicy)
                      .setTimeoutSecs(timeoutSecs)
                      .setCreatedTimestampMs(System.currentTimeMillis()))));
    }

    @Override
    public Set<IHostStatus> drain(Set<String> hosts) {
      return storage.write(store -> {
        // Create a dummy maintenance request zero percent sla and timeout to force drain.
        recordMaintenanceRequests(store, hosts, ZERO_PERCENT_SLA, 0);
        return watchDrainingTasks(store, hosts);
      });
    }

    @Override
    public Set<IHostStatus> slaDrain(
        Set<String> hosts,
        SlaPolicy defaultSlaPolicy,
        long timeoutSecs) {

      // We can have only one maintenance request per host at any time.
      // So we will simply overwrite any existing request. If the current one is actively handled,
      // during the write, the new one will just be a no-op, since the host is already being
      // drained. If host is in DRAINED it will be moved back into DRAINING and then back into
      // DRAINED without having to perform any work.
      return storage.write(store -> {
        recordMaintenanceRequests(store, hosts, defaultSlaPolicy, timeoutSecs);
        return setMaintenanceMode(store, hosts, DRAINING);
      });
    }

    private Optional<String> getHostname(Protos.InverseOffer offer) {
      if (offer.getUrl().getAddress().hasHostname()) {
        return Optional.of(offer.getUrl().getAddress().getHostname());
      } else {
        return Optional.empty();
      }
    }

    @Override
    public void drainForInverseOffer(Protos.InverseOffer offer) {
      // TaskStore does not allow for querying by agent id.
      Optional<String> hostname = getHostname(offer);

      if (hostname.isPresent()) {
        String host = hostname.get();
        storage.write(storeProvider -> {
          // Create a dummy maintenance request zero percent sla and timeout to force drain.
          recordMaintenanceRequests(storeProvider, ImmutableSet.of(host), ZERO_PERCENT_SLA, 0);
          return drainTasksOnHost(host, storeProvider);
        });
      } else {
        LOG.error("Unable to drain tasks on agent {} because "
            + "no hostname attached to inverse offer {}.", offer.getAgentId(), offer.getId());
      }
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
          .map(ATTRS_TO_STATUS)
          .map(GET_MODE)
          .orElse(MaintenanceMode.NONE));
    }

    @Override
    public Set<IHostStatus> getStatus(final Set<String> hosts) {
      return storage.read(storeProvider -> {
        // Warning - this is filtering _all_ host attributes.  If using this to frequently query
        // for a small set of hosts, a getHostAttributes variant should be added.
        return storeProvider.getAttributeStore().getHostAttributes().stream()
            .filter(Predicates.compose(Predicates.in(hosts), HOST_NAME))
            .map(ATTRS_TO_STATUS)
            .collect(Collectors.toSet());
      });
    }

    @Override
    public Set<IHostStatus> endMaintenance(final Set<String> hosts) {
      return storage.write(
          storeProvider -> {
            hosts.forEach(
                h -> storeProvider.getHostMaintenanceStore().removeHostMaintenanceRequest(h));
            return setMaintenanceMode(storeProvider, hosts, MaintenanceMode.NONE);
          });
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

    @VisibleForTesting
    void runForTest() {
      runOneIteration();
    }

    @Timed
    @Override
    protected void runOneIteration() {
      LOG.info("Looking for hosts in DRAINING state");
      storage.read(store -> {
        store.getAttributeStore()
            .getHostAttributes()
            .stream()
            .filter(h -> h.getMode() == DRAINING)
            .forEach(h -> {
              if (drainTasksOnHost(h.getHost(), store).isEmpty()) {
                storage.write(mutable -> setMaintenanceMode(
                    mutable,
                    ImmutableSet.of(h.getHost()),
                    DRAINED));
              }
            });
        return null;
      });
    }

    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedDelaySchedule(
          pollingInterval.getValue(),
          pollingInterval.getValue(),
          pollingInterval.getUnit().getTimeUnit());
    }

    private void drainTask(IScheduledTask task, StoreProvider store) throws ExecutionException {
      String host = task.getAssignedTask().getSlaveHost();
      Optional<IHostMaintenanceRequest> hostMaintenanceRequest =
          store.getHostMaintenanceStore().getHostMaintenanceRequest(host);
      if (!hostMaintenanceRequest.isPresent()) {
        LOG.error("No maintenance request found for host: {}. Assuming SLA not satisfied.", host);
        missingMaintenanceCounter.incrementAndGet();
        return;
      }

      boolean force = false;
      long expireMs =
          System.currentTimeMillis() - hostMaintenanceRequest.get().getCreatedTimestampMs();
      long maintenanceCountDownMs =
          TimeAmount.of(hostMaintenanceRequest.get().getTimeoutSecs(), Time.SECONDS)
              .as(Time.MILLISECONDS) - expireMs;
      maintenanceCountDownByTask.get(
          Joiner.on("_")
              .join(MAINTENANCE_COUNTDOWN_STAT_NAME,
                  InstanceKeys.toString(Tasks.getJob(task), Tasks.getInstanceId(task))))
          .getAndSet(maintenanceCountDownMs);

      if (hostMaintenanceRequest.get().getTimeoutSecs()
            < TimeAmount.of(expireMs, Time.MILLISECONDS).as(Time.SECONDS)) {
        LOG.warn("Maintenance request timed out for host: {} after {} secs. Forcing drain of {}.",
            host, hostMaintenanceRequest.get().getTimeoutSecs(), Tasks.id(task));
        force = true;
      }

      final ISlaPolicy slaPolicy = task.getAssignedTask().getTask().isSetSlaPolicy()
          ? task.getAssignedTask().getTask().getSlaPolicy()
          : hostMaintenanceRequest.get().getDefaultSlaPolicy();

      slaManager.checkSlaThenAct(
          task,
          slaPolicy,
          storeProvider -> stateManager.changeState(
              storeProvider,
              Tasks.id(task),
              Optional.empty(),
              ScheduleStatus.DRAINING,
              Optional.of(DRAINING_MESSAGE)),
          force);
    }
  }
}
