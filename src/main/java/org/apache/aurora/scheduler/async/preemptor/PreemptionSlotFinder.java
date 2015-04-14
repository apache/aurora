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
package org.apache.aurora.scheduler.async.preemptor;

import java.util.Objects;
import java.util.Set;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.async.OfferManager;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos.SlaveID;

import static java.util.Objects.requireNonNull;

/**
 * Tries to find a slave with a combination of active tasks (victims) and available offer
 * (slack) resources that can accommodate a given task (candidate), provided victims are preempted.
 * <p>
 * A task may preempt another task if the following conditions hold true:
 * <ol>
 *  <li>The resources reserved for a victim (or a set of victims) are sufficient to satisfy
 *    the candidate.
 *  </li>
 *  <li>Both candidate and victim are owned by the same user and the
 *    {@link ITaskConfig#getPriority} of a victim is lower OR a victim is non-production and the
 *    candidate is production.
 *  </li>
 * </ol>
 */
public interface PreemptionSlotFinder {

  class PreemptionSlot {
    private final Set<PreemptionVictim> victims;
    private final String slaveId;

    @VisibleForTesting
    PreemptionSlot(ImmutableSet<PreemptionVictim> victims, String slaveId) {
      this.victims = requireNonNull(victims);
      this.slaveId = requireNonNull(slaveId);
    }

    Set<PreemptionVictim> getVictims() {
      return victims;
    }

    String getSlaveId() {
      return slaveId;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof PreemptionSlot)) {
        return false;
      }

      PreemptionSlot other = (PreemptionSlot) o;
      return Objects.equals(getVictims(), other.getVictims())
          && Objects.equals(getSlaveId(), other.getSlaveId());
    }

    @Override
    public int hashCode() {
      return Objects.hash(victims, slaveId);
    }

    @Override
    public String toString() {
      return com.google.common.base.Objects.toStringHelper(this)
          .add("victims", getVictims())
          .add("slaveId", getSlaveId())
          .toString();
    }
  }

  /**
   * Searches for a {@link PreemptionSlot} for a given {@code pendingTask}.
   *
   * @param pendingTask Task to search preemption slot for.
   * @param attributeAggregate An {@link AttributeAggregate} instance for the task's job.
   * @param storeProvider A store provider to access task data.
   * @return An instance of {@link PreemptionSlot} if preemption is possible.
   */
  Optional<PreemptionSlot> findPreemptionSlotFor(
      IAssignedTask pendingTask,
      AttributeAggregate attributeAggregate,
      StoreProvider storeProvider);

  /**
   * Validates that a previously-found {@code preemptionSlot} can still accommodate a given task.
   *
   * @param pendingTask Task to validate preemption slot for.
   * @param attributeAggregate An {@link AttributeAggregate} instance for the task's job.
   * @param preemptionSlot A previously found preemption slot to validate.
   * @param storeProvider A store provider to access task data.
   * @return A finalized set of {@code PreemptionVictim} instances to preempt for a given task.
   */
  Optional<ImmutableSet<PreemptionVictim>> validatePreemptionSlotFor(
      IAssignedTask pendingTask,
      AttributeAggregate attributeAggregate,
      PreemptionSlot preemptionSlot,
      StoreProvider storeProvider);

  class PreemptionSlotFinderImpl implements PreemptionSlotFinder {
    private final OfferManager offerManager;
    private final ClusterState clusterState;
    private final SchedulingFilter schedulingFilter;
    private final ExecutorSettings executorSettings;
    private final PreemptorMetrics metrics;

    @Inject
    PreemptionSlotFinderImpl(
        OfferManager offerManager,
        ClusterState clusterState,
        SchedulingFilter schedulingFilter,
        ExecutorSettings executorSettings,
        PreemptorMetrics metrics) {

      this.offerManager = requireNonNull(offerManager);
      this.clusterState = requireNonNull(clusterState);
      this.schedulingFilter = requireNonNull(schedulingFilter);
      this.executorSettings = requireNonNull(executorSettings);
      this.metrics = requireNonNull(metrics);
    }

    private static final Function<HostOffer, ResourceSlot> OFFER_TO_RESOURCE_SLOT =
        new Function<HostOffer, ResourceSlot>() {
          @Override
          public ResourceSlot apply(HostOffer offer) {
            return ResourceSlot.from(offer.getOffer());
          }
        };

    private static final Function<HostOffer, String> OFFER_TO_HOST =
        new Function<HostOffer, String>() {
          @Override
          public String apply(HostOffer offer) {
            return offer.getOffer().getHostname();
          }
        };

    private static final Function<PreemptionVictim, String> VICTIM_TO_HOST =
        new Function<PreemptionVictim, String>() {
          @Override
          public String apply(PreemptionVictim victim) {
            return victim.getSlaveHost();
          }
        };

    private final Function<PreemptionVictim, ResourceSlot> victimToResources =
        new Function<PreemptionVictim, ResourceSlot>() {
          @Override
          public ResourceSlot apply(PreemptionVictim victim) {
            return ResourceSlot.from(victim, executorSettings);
          }
        };

    // TODO(zmanji) Consider using Dominant Resource Fairness for ordering instead of the vector
    // ordering
    private final Ordering<PreemptionVictim> resourceOrder =
        ResourceSlot.ORDER.onResultOf(victimToResources).reverse();

    private static final Function<HostOffer, String> OFFER_TO_SLAVE_ID =
        new Function<HostOffer, String>() {
          @Override
          public String apply(HostOffer offer) {
            return offer.getOffer().getSlaveId().getValue();
          }
        };

    // TODO(maxim): This should take pre-computed mappings (e.g. slaveToOffers) to avoid
    // unnecessary repeated work.
    @Override
    public Optional<PreemptionSlot> findPreemptionSlotFor(
        final IAssignedTask pendingTask,
        AttributeAggregate attributeAggregate,
        StoreProvider storeProvider) {

      Multimap<String, PreemptionVictim> slavesToActiveTasks =
          clusterState.getSlavesToActiveTasks();

      if (slavesToActiveTasks.isEmpty()) {
        return Optional.absent();
      }

      // Group the offers by slave id so they can be paired with active tasks from the same slave.
      Multimap<String, HostOffer> slavesToOffers =
          Multimaps.index(offerManager.getOffers(), OFFER_TO_SLAVE_ID);

      Set<String> allSlaves = ImmutableSet.<String>builder()
          .addAll(slavesToOffers.keySet())
          .addAll(slavesToActiveTasks.keySet())
          .build();

      for (String slaveId : allSlaves) {
        final Optional<ImmutableSet<PreemptionVictim>> preemptionVictims = getTasksToPreempt(
            slavesToActiveTasks.get(slaveId),
            slavesToOffers.get(slaveId),
            pendingTask,
            attributeAggregate,
            storeProvider);

        if (preemptionVictims.isPresent()) {
          return Optional.of(new PreemptionSlot(preemptionVictims.get(), slaveId));
        }
      }

      return Optional.absent();
    }

    @Override
    public Optional<ImmutableSet<PreemptionVictim>> validatePreemptionSlotFor(
        IAssignedTask pendingTask,
        AttributeAggregate attributeAggregate,
        PreemptionSlot preemptionSlot,
        StoreProvider storeProvider) {

      Optional<HostOffer> offer =
          offerManager.getOffer(SlaveID.newBuilder().setValue(preemptionSlot.getSlaveId()).build());

      return getTasksToPreempt(
          preemptionSlot.getVictims(),
          offer.asSet(),
          pendingTask,
          attributeAggregate,
          storeProvider);
    }

    /**
     * Optional.absent indicates that this slave does not have enough resources to satisfy the task.
     * A set with elements indicates those tasks and the offers are enough.
     */
    private Optional<ImmutableSet<PreemptionVictim>> getTasksToPreempt(
        Iterable<PreemptionVictim> possibleVictims,
        Iterable<HostOffer> offers,
        IAssignedTask pendingTask,
        AttributeAggregate jobState,
        StoreProvider storeProvider) {

      // This enforces the precondition that all of the resources are from the same host. We need to
      // get the host for the schedulingFilter.
      Set<String> hosts = ImmutableSet.<String>builder()
          .addAll(Iterables.transform(possibleVictims, VICTIM_TO_HOST))
          .addAll(Iterables.transform(offers, OFFER_TO_HOST)).build();

      ResourceSlot slackResources =
          ResourceSlot.sum(Iterables.transform(offers, OFFER_TO_RESOURCE_SLOT));

      FluentIterable<PreemptionVictim> preemptableTasks = FluentIterable.from(possibleVictims)
          .filter(preemptionFilter(pendingTask.getTask()));

      if (preemptableTasks.isEmpty()) {
        return Optional.absent();
      }

      Set<PreemptionVictim> toPreemptTasks = Sets.newHashSet();

      Iterable<PreemptionVictim> sortedVictims =
          resourceOrder.immutableSortedCopy(preemptableTasks);

      Optional<IHostAttributes> attributes =
          storeProvider.getAttributeStore().getHostAttributes(Iterables.getOnlyElement(hosts));

      if (!attributes.isPresent()) {
        metrics.recordMissingAttributes();
        return Optional.absent();
      }

      for (PreemptionVictim victim : sortedVictims) {
        toPreemptTasks.add(victim);

        ResourceSlot totalResource = ResourceSlot.sum(
            ResourceSlot.sum(Iterables.transform(toPreemptTasks, victimToResources)),
            slackResources);

        Set<Veto> vetoes = schedulingFilter.filter(
            new UnusedResource(totalResource, attributes.get()),
            new ResourceRequest(pendingTask.getTask(), jobState));

        if (vetoes.isEmpty()) {
          return Optional.of(ImmutableSet.copyOf(toPreemptTasks));
        }
      }
      return Optional.absent();
    }

    /**
     * Creates a filter that will find tasks that the provided {@code pendingTask} may preempt.
     *
     * @param pendingTask A task that is not scheduled to possibly preempt other tasks for.
     * @return A filter that will compare the priorities and resources required by other tasks
     *     with {@code preemptableTask}.
     */
    private static Predicate<PreemptionVictim> preemptionFilter(final ITaskConfig pendingTask) {
      return new Predicate<PreemptionVictim>() {
        @Override
        public boolean apply(PreemptionVictim possibleVictim) {
          boolean pendingIsProduction = pendingTask.isProduction();
          boolean victimIsProduction = possibleVictim.isProduction();

          if (pendingIsProduction && !victimIsProduction) {
            return true;
          } else if (pendingIsProduction == victimIsProduction) {
            // If production flags are equal, preemption is based on priority within the same role.
            if (pendingTask.getJob().getRole().equals(possibleVictim.getRole())) {
              return pendingTask.getPriority() > possibleVictim.getPriority();
            } else {
              return false;
            }
          } else {
            return false;
          }
        }
      };
    }
  }
}
