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

import java.util.Set;

import javax.inject.Inject;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.Resources;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.ResourceSlot.sum;

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

    private static final Function<HostOffer, ResourceSlot> OFFER_TO_RESOURCE_SLOT =
        new Function<HostOffer, ResourceSlot>() {
          @Override
          public ResourceSlot apply(HostOffer offer) {
            return Resources.from(offer.getOffer()).filter(Resources.NON_REVOCABLE).slot();
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
            ResourceSlot slot = victim.getResourceSlot();
            if (tierManager.getTier(victim.getConfig()).isRevocable()) {
              // Revocable task CPU cannot be used for preemption purposes as it's a compressible
              // resource. We can still use RAM, DISK and PORTS as they are not compressible.
              slot = new ResourceSlot(0.0, slot.getRam(), slot.getDisk(), slot.getNumPorts());
            }
            return slot.withOverhead(executorSettings);
          }
        };

    // TODO(zmanji) Consider using Dominant Resource Fairness for ordering instead of the vector
    // ordering
    private final Ordering<PreemptionVictim> resourceOrder =
        ResourceSlot.ORDER.onResultOf(victimToResources).reverse();

    @Override
    public Optional<ImmutableSet<PreemptionVictim>> filterPreemptionVictims(
        ITaskConfig pendingTask,
        Iterable<PreemptionVictim> possibleVictims,
        AttributeAggregate jobState,
        Optional<HostOffer> offer,
        StoreProvider storeProvider) {

      // This enforces the precondition that all of the resources are from the same host. We need to
      // get the host for the schedulingFilter.
      Set<String> hosts = ImmutableSet.<String>builder()
          .addAll(Iterables.transform(possibleVictims, VICTIM_TO_HOST))
          .addAll(Iterables.transform(offer.asSet(), OFFER_TO_HOST)).build();

      ResourceSlot slackResources = sum(Iterables.transform(offer.asSet(), OFFER_TO_RESOURCE_SLOT));

      FluentIterable<PreemptionVictim> preemptableTasks = FluentIterable.from(possibleVictims)
          .filter(preemptionFilter(pendingTask));

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

        ResourceSlot totalResource =
            sum(Iterables.transform(toPreemptTasks, victimToResources)).add(slackResources);

        Set<Veto> vetoes = schedulingFilter.filter(
            new UnusedResource(totalResource, attributes.get()),
            new ResourceRequest(pendingTask, jobState));

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
