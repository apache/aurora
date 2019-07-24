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
package org.apache.aurora.scheduler.preemptor;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.resources.ResourceBag.EMPTY;
import static org.apache.aurora.scheduler.resources.ResourceBag.IS_MESOS_REVOCABLE;
import static org.apache.aurora.scheduler.resources.ResourceManager.bagFromMesosResources;
import static org.apache.aurora.scheduler.resources.ResourceManager.getNonRevocableOfferResources;

/**
 * Filters active tasks (victims) and available offer (slack) resources that can accommodate a
 * given task (candidate), provided victims are preempted.
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
public interface PreemptionVictimFilter {
  /**
   * Returns a set of {@link PreemptionVictim} that can accommodate a given task if preempted.
   *
   * @param pendingTask Task to search preemption slot for.
   * @param victims Active tasks on a slave.
   * @param attributeAggregate An {@link AttributeAggregate} instance for the task's job.
   * @param offer A resource offer for a slave.
   * @param storeProvider A store provider to access task data.
   * @return A set of {@code PreemptionVictim} instances to preempt for a given task.
   */
  Optional<ImmutableSet<PreemptionVictim>> filterPreemptionVictims(
      ITaskConfig pendingTask,
      Iterable<PreemptionVictim> victims,
      AttributeAggregate attributeAggregate,
      Optional<HostOffer> offer,
      StoreProvider storeProvider);

  class PreemptionVictimFilterImpl implements PreemptionVictimFilter {
    private final SchedulingFilter schedulingFilter;
    private final ExecutorSettings executorSettings;
    private final PreemptorMetrics metrics;
    private final TierManager tierManager;

    @Inject
    PreemptionVictimFilterImpl(
        SchedulingFilter schedulingFilter,
        ExecutorSettings executorSettings,
        PreemptorMetrics metrics,
        TierManager tierManager) {

      this.schedulingFilter = requireNonNull(schedulingFilter);
      this.executorSettings = requireNonNull(executorSettings);
      this.metrics = requireNonNull(metrics);
      this.tierManager = requireNonNull(tierManager);
    }

    private static final Function<HostOffer, String> OFFER_TO_HOST =
        offer -> offer.getOffer().getHostname();

    private static final Function<PreemptionVictim, String> VICTIM_TO_HOST =
        PreemptionVictim::getSlaveHost;

    private final Function<PreemptionVictim, ResourceBag> victimToResources =
        new Function<PreemptionVictim, ResourceBag>() {
          @Override
          public ResourceBag apply(PreemptionVictim victim) {
            ResourceBag bag = victim.getResourceBag(executorSettings);

            if (tierManager.getTier(victim.getConfig()).isRevocable()) {
              // Revocable task CPU cannot be used for preemption purposes as it's a compressible
              // resource. We can still use RAM, DISK and PORTS as they are not compressible.
              bag = bag.filter(IS_MESOS_REVOCABLE.negate());
            }

            return bag;
          }
        };

    /**
     * We compare ResourceBags lexicographically according to the order of ResourceType enum
     * declarations. This ensures we have a deterministic order that does not break any Java
     * sorting algorithms.
     *
     * TODO(serb) Consider refactoring and re-using the OfferOrderBuilder here
     */
    @VisibleForTesting
    static final Ordering<ResourceBag> ORDER = new Ordering<ResourceBag>() {
      @Override
      public int compare(ResourceBag left, ResourceBag right) {
        for (ResourceType type : ResourceType.values()) {
          int compare = Double.compare(left.valueOf(type), right.valueOf(type));
          if (compare != 0) {
            return compare;
          }
        }
        return 0;
      }
    };

    private final Ordering<PreemptionVictim> resourceOrder =
        ORDER.onResultOf(victimToResources).reverse();

    @Override
    public Optional<ImmutableSet<PreemptionVictim>> filterPreemptionVictims(
        ITaskConfig pendingTask,
        Iterable<PreemptionVictim> possibleVictims,
        AttributeAggregate jobState,
        Optional<HostOffer> offer,
        StoreProvider storeProvider) {

      List<PreemptionVictim> sortedVictims = StreamSupport
          .stream(possibleVictims.spliterator(), false)
          .filter(preemptionFilter(pendingTask))
          .sorted(resourceOrder)
          .collect(ImmutableList.toImmutableList());
      if (sortedVictims.isEmpty()) {
        return Optional.empty();
      }

      // This enforces the precondition that all of the resources are from the same host. We need to
      // get the host for the schedulingFilter.
      Set<String> hosts = ImmutableSet.<String>builder()
          .addAll(Iterables.transform(possibleVictims, VICTIM_TO_HOST))
          .addAll(offer.map(OFFER_TO_HOST).map(ImmutableSet::of).orElse(ImmutableSet.of()))
          .build();

      ResourceBag slackResources = offer
          .map(o -> bagFromMesosResources(getNonRevocableOfferResources(o.getOffer())))
          .orElse(EMPTY);

      Optional<IHostAttributes> attributes =
          storeProvider.getAttributeStore().getHostAttributes(Iterables.getOnlyElement(hosts));
      if (!attributes.isPresent()) {
        metrics.recordMissingAttributes();
        return Optional.empty();
      }

      ResourceRequest requiredResources =
          ResourceRequest.fromTask(pendingTask, executorSettings, jobState, tierManager);

      Optional<Instant> unavailability = offer.flatMap(HostOffer::getUnavailabilityStart);

      ResourceBag totalResource = slackResources;
      Set<PreemptionVictim> toPreemptTasks = Sets.newHashSet();
      for (PreemptionVictim victim : sortedVictims) {
        toPreemptTasks.add(victim);
        totalResource = totalResource.add(victimToResources.apply(victim));

        Set<Veto> vetoes = schedulingFilter.filter(
            new UnusedResource(totalResource, attributes.get(), unavailability),
            requiredResources);

        if (vetoes.isEmpty()) {
          return Optional.of(ImmutableSet.copyOf(toPreemptTasks));
        }
      }

      return Optional.empty();
    }

    /**
     * Creates a filter that will find tasks that the provided {@code pendingTask} may preempt.
     *
     * @param pendingTask A task that is not scheduled to possibly preempt other tasks for.
     * @return A filter that will compare the priorities and resources required by other tasks
     *     with {@code preemptibleTask}.
     */
    private Predicate<PreemptionVictim> preemptionFilter(final ITaskConfig pendingTask) {
      return possibleVictim -> {
        boolean pendingIsPreemptible = tierManager.getTier(pendingTask).isPreemptible();
        boolean victimIsPreemptible =
            tierManager.getTier(possibleVictim.getConfig()).isPreemptible();

        if (!pendingIsPreemptible && victimIsPreemptible) {
          return true;
        } else if (pendingIsPreemptible == victimIsPreemptible) {
          // If preemptible flags are equal, preemption is based on priority within the same role.
          if (pendingTask.getJob().getRole().equals(possibleVictim.getRole())) {
            return pendingTask.getPriority() > possibleVictim.getPriority();
          } else {
            return false;
          }
        } else {
          return false;
        }
      };
    }
  }
}
