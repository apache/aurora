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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.InstanceKeys;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.filter.SchedulingFilter.VetoGroup;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.updater.UpdateAgentReserver;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.TaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import static org.apache.mesos.v1.Protos.Offer;

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
   * @param tasks Tasks to assign.
   * @param preemptionReservations Slave reservations.
   * @return Successfully assigned task IDs.
   */
  Set<String> maybeAssign(
      MutableStoreProvider storeProvider,
      ResourceRequest resourceRequest,
      TaskGroupKey groupKey,
      Iterable<IAssignedTask> tasks,
      Map<String, TaskGroupKey> preemptionReservations);

  class FirstFitTaskAssigner implements TaskAssigner {
    private static final Logger LOG = LoggerFactory.getLogger(FirstFitTaskAssigner.class);

    @VisibleForTesting
    static final Optional<String> LAUNCH_FAILED_MSG =
        Optional.of("Unknown exception attempting to schedule task.");
    @VisibleForTesting
    static final String ASSIGNER_LAUNCH_FAILURES = "assigner_launch_failures";
    @VisibleForTesting
    static final String ASSIGNER_EVALUATED_OFFERS = "assigner_evaluated_offers";

    private final AtomicLong launchFailures;
    private final AtomicLong evaluatedOffers;

    private final StateManager stateManager;
    private final SchedulingFilter filter;
    private final MesosTaskFactory taskFactory;
    private final OfferManager offerManager;
    private final TierManager tierManager;
    private final UpdateAgentReserver updateAgentReserver;

    @Inject
    public FirstFitTaskAssigner(
        StateManager stateManager,
        SchedulingFilter filter,
        MesosTaskFactory taskFactory,
        OfferManager offerManager,
        TierManager tierManager,
        UpdateAgentReserver updateAgentReserver,
        StatsProvider statsProvider) {

      this.stateManager = requireNonNull(stateManager);
      this.filter = requireNonNull(filter);
      this.taskFactory = requireNonNull(taskFactory);
      this.offerManager = requireNonNull(offerManager);
      this.tierManager = requireNonNull(tierManager);
      this.launchFailures = statsProvider.makeCounter(ASSIGNER_LAUNCH_FAILURES);
      this.evaluatedOffers = statsProvider.makeCounter(ASSIGNER_EVALUATED_OFFERS);
      this.updateAgentReserver = requireNonNull(updateAgentReserver);
    }

    @VisibleForTesting
    IAssignedTask mapAndAssignResources(Offer offer, IAssignedTask task) {
      IAssignedTask assigned = task;
      for (ResourceType type : ResourceManager.getTaskResourceTypes(assigned)) {
        if (type.getMapper().isPresent()) {
          assigned = type.getMapper().get().mapAndAssign(offer, assigned);
        }
      }
      return assigned;
    }

    private TaskInfo assign(
        MutableStoreProvider storeProvider,
        Offer offer,
        String taskId) {

      String host = offer.getHostname();
      IAssignedTask assigned = stateManager.assignTask(
          storeProvider,
          taskId,
          host,
          offer.getAgentId(),
          task -> mapAndAssignResources(offer, task));
      LOG.info(
          "Offer on agent {} (id {}) is being assigned task for {}.",
          host, offer.getAgentId().getValue(), taskId);
      return taskFactory.createFrom(assigned, offer);
    }

    private boolean evaluateOffer(
        MutableStoreProvider storeProvider,
        TierInfo tierInfo,
        ResourceRequest resourceRequest,
        TaskGroupKey groupKey,
        IAssignedTask task,
        HostOffer offer,
        ImmutableSet.Builder<String> assignmentResult) throws OfferManager.LaunchException {

      String taskId = task.getTaskId();
      Set<Veto> vetoes = filter.filter(
          new UnusedResource(
              offer.getResourceBag(tierInfo),
              offer.getAttributes(),
              offer.getUnavailabilityStart()),
          resourceRequest);

      if (vetoes.isEmpty()) {
        TaskInfo taskInfo = assign(
            storeProvider,
            offer.getOffer(),
            taskId);
        resourceRequest.getJobState().updateAttributeAggregate(offer.getAttributes());

        try {
          offerManager.launchTask(offer.getOffer().getId(), taskInfo);
          assignmentResult.add(taskId);
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
          throw e;
        }
      } else {
        if (Veto.identifyGroup(vetoes) == VetoGroup.STATIC) {
          // Never attempt to match this offer/groupKey pair again.
          offerManager.banOfferForTaskGroup(offer.getOffer().getId(), groupKey);
        }
        LOG.debug("Agent {} vetoed task {}: {}", offer.getOffer().getHostname(), taskId, vetoes);
      }
      return false;
    }

    private Iterable<IAssignedTask> maybeAssignReserved(
        Iterable<IAssignedTask> tasks,
        MutableStoreProvider storeProvider,
        TierInfo tierInfo,
        ResourceRequest resourceRequest,
        TaskGroupKey groupKey,
        ImmutableSet.Builder<String> assignmentResult) {

      if (!updateAgentReserver.hasReservations(groupKey)) {
        return tasks;
      }

      // Data structure to record which tasks should be excluded from the regular (non-reserved)
      // scheduling loop. This is important because we release reservations once they are used,
      // so we need to record them separately to avoid them being double-scheduled.
      ImmutableSet.Builder<IInstanceKey> excludeBuilder = ImmutableSet.builder();

      for (IAssignedTask task: tasks) {
        IInstanceKey key = InstanceKeys.from(task.getTask().getJob(), task.getInstanceId());
        Optional<String> maybeAgentId = updateAgentReserver.getAgent(key);
        if (maybeAgentId.isPresent()) {
          excludeBuilder.add(key);
          Optional<HostOffer> offer = offerManager.getOffer(
              Protos.AgentID.newBuilder().setValue(maybeAgentId.get()).build());
          if (offer.isPresent()) {
            try {
              // The offer can still be veto'd because of changed constraints, or because the
              // Scheduler hasn't been updated by Mesos yet...
              if (evaluateOffer(
                  storeProvider,
                  tierInfo,
                  resourceRequest,
                  groupKey,
                  task,
                  offer.get(),
                  assignmentResult)) {

                LOG.info("Used update reservation for {} on {}", key, maybeAgentId.get());
                updateAgentReserver.release(maybeAgentId.get(), key);
              } else {
                LOG.info(
                    "Tried to reuse offer on {} for {}, but was not ready yet.",
                    maybeAgentId.get(),
                    key);
              }
            } catch (OfferManager.LaunchException e) {
              updateAgentReserver.release(maybeAgentId.get(), key);
            }
          }
        }
      }

      // Return only the tasks that didn't have reservations. Offers on agents that were reserved
      // might not have been seen by Aurora yet, so we need to wait until the reservation expires
      // before giving up and falling back to the first-fit algorithm.
      Set<IInstanceKey> toBeExcluded = excludeBuilder.build();
      return Iterables.filter(tasks, t -> !toBeExcluded.contains(
          InstanceKeys.from(t.getTask().getJob(), t.getInstanceId())));
    }

    @Timed("assigner_maybe_assign")
    @Override
    public Set<String> maybeAssign(
        MutableStoreProvider storeProvider,
        ResourceRequest resourceRequest,
        TaskGroupKey groupKey,
        Iterable<IAssignedTask> tasks,
        Map<String, TaskGroupKey> preemptionReservations) {

      if (Iterables.isEmpty(tasks)) {
        return ImmutableSet.of();
      }

      TierInfo tierInfo = tierManager.getTier(groupKey.getTask());
      ImmutableSet.Builder<String> assignmentResult = ImmutableSet.builder();

      Iterable<IAssignedTask> nonReservedTasks = maybeAssignReserved(
          tasks,
          storeProvider,
          tierInfo,
          resourceRequest,
          groupKey,
          assignmentResult);

      Iterator<IAssignedTask> remainingTasks = nonReservedTasks.iterator();
      // Make sure we still have tasks to process after reservations are processed.
      if (remainingTasks.hasNext()) {
        IAssignedTask task = remainingTasks.next();
        for (HostOffer offer : offerManager.getOffers(groupKey)) {

          if (!offer.hasCpuAndMem()) {
            // This offer lacks any type of CPU or mem resource, and therefore will never match
            // a task.
            continue;
          }

          String agentId = offer.getOffer().getAgentId().getValue();

          Optional<TaskGroupKey> reservedGroup = Optional.fromNullable(
              preemptionReservations.get(agentId));

          if (reservedGroup.isPresent() && !reservedGroup.get().equals(groupKey)) {
            // This slave is reserved for a different task group -> skip.
            continue;
          }

          if (!updateAgentReserver.getReservations(agentId).isEmpty()) {
            // This agent has been reserved for an update in-progress, skip.
            continue;
          }

          evaluatedOffers.incrementAndGet();
          try {
            boolean offerUsed = evaluateOffer(
                storeProvider, tierInfo, resourceRequest, groupKey, task, offer, assignmentResult);
            if (offerUsed) {
              if (remainingTasks.hasNext()) {
                task = remainingTasks.next();
              } else {
                break;
              }
            }
          } catch (OfferManager.LaunchException e) {
            break;
          }
        }
      }

      return assignmentResult.build();
    }
  }
}
