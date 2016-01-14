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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.Resources;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.filter.SchedulingFilter.VetoGroup;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.mesos.Protos.TaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import static org.apache.mesos.Protos.Offer;

/**
 * Responsible for matching a task against an offer and launching it.
 */
public interface TaskAssigner {
  /**
   * Tries to match a task against an offer.  If a match is found, the assigner makes the
   * appropriate changes to the task and requests task launch.
   *
   * @param storeProvider Storage provider.
   * @param resourceRequest The request for resources being scheduled.
   * @param groupKey Task group key.
   * @param taskId Task id to assign.
   * @param slaveReservations Slave reservations.
   * @return Assignment result.
   */
  boolean maybeAssign(
      MutableStoreProvider storeProvider,
      ResourceRequest resourceRequest,
      TaskGroupKey groupKey,
      String taskId,
      Map<String, TaskGroupKey> slaveReservations);

  class TaskAssignerImpl implements TaskAssigner {
    private static final Logger LOG = LoggerFactory.getLogger(TaskAssignerImpl.class);

    @VisibleForTesting
    static final Optional<String> LAUNCH_FAILED_MSG =
        Optional.of("Unknown exception attempting to schedule task.");

    private final AtomicLong launchFailures = Stats.exportLong("assigner_launch_failures");

    private final StateManager stateManager;
    private final SchedulingFilter filter;
    private final MesosTaskFactory taskFactory;
    private final OfferManager offerManager;
    private final TierManager tierManager;

    @Inject
    public TaskAssignerImpl(
        StateManager stateManager,
        SchedulingFilter filter,
        MesosTaskFactory taskFactory,
        OfferManager offerManager,
        TierManager tierManager) {

      this.stateManager = requireNonNull(stateManager);
      this.filter = requireNonNull(filter);
      this.taskFactory = requireNonNull(taskFactory);
      this.offerManager = requireNonNull(offerManager);
      this.tierManager = requireNonNull(tierManager);
    }

    private TaskInfo assign(
        MutableStoreProvider storeProvider,
        Offer offer,
        Set<String> requestedPorts,
        String taskId) {

      String host = offer.getHostname();
      Set<Integer> selectedPorts = Resources.from(offer).getPorts(requestedPorts.size());
      Preconditions.checkState(selectedPorts.size() == requestedPorts.size());

      final Iterator<String> names = requestedPorts.iterator();
      Map<String, Integer> portsByName = FluentIterable.from(selectedPorts)
          .uniqueIndex(input -> names.next());

      IAssignedTask assigned = stateManager.assignTask(
          storeProvider,
          taskId,
          host,
          offer.getSlaveId(),
          portsByName);
      LOG.info(
          "Offer on slave {} (id {}) is being assigned task for {}.",
          host, offer.getSlaveId().getValue(), taskId);
      return taskFactory.createFrom(assigned, offer);
    }

    @Timed("assigner_maybe_assign")
    @Override
    public boolean maybeAssign(
        MutableStoreProvider storeProvider,
        ResourceRequest resourceRequest,
        TaskGroupKey groupKey,
        String taskId,
        Map<String, TaskGroupKey> slaveReservations) {

      for (HostOffer offer : offerManager.getOffers(groupKey)) {
        Optional<TaskGroupKey> reservedGroup = Optional.fromNullable(
            slaveReservations.get(offer.getOffer().getSlaveId().getValue()));

        if (reservedGroup.isPresent() && !reservedGroup.get().equals(groupKey)) {
          // This slave is reserved for a different task group -> skip.
          continue;
        }

        TierInfo tierInfo = tierManager.getTier(groupKey.getTask());
        Set<Veto> vetoes = filter.filter(
            new UnusedResource(
                Resources.from(offer.getOffer()).filter(tierInfo).slot(),
                offer.getAttributes()),
            resourceRequest);

        if (vetoes.isEmpty()) {
          TaskInfo taskInfo = assign(
              storeProvider,
              offer.getOffer(),
              resourceRequest.getRequestedPorts(),
              taskId);

          try {
            offerManager.launchTask(offer.getOffer().getId(), taskInfo);
            return true;
          } catch (OfferManager.LaunchException e) {
            LOG.warn("Failed to launch task.", e);
            launchFailures.incrementAndGet();

            // The attempt to schedule the task failed, so we need to backpedal on the
            // assignment.
            // It is in the LOST state and a new task will move to PENDING to replace it.
            // Should the state change fail due to storage issues, that's okay.  The task will
            // time out in the ASSIGNED state and be moved to LOST.
            stateManager.changeState(
                storeProvider,
                taskId,
                Optional.of(PENDING),
                LOST,
                LAUNCH_FAILED_MSG);
            return false;
          }
        } else {
          if (Veto.identifyGroup(vetoes) == VetoGroup.STATIC) {
            // Never attempt to match this offer/groupKey pair again.
            offerManager.banOffer(offer.getOffer().getId(), groupKey);
          }

          LOG.debug("Slave " + offer.getOffer().getHostname()
              + " vetoed task " + taskId + ": " + vetoes);
        }
      }
      return false;
    }
  }
}
