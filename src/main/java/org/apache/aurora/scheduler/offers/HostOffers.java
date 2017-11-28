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
package org.apache.aurora.scheduler.offers;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.aurora.common.collections.Pair;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.mesos.v1.Protos;

import static java.util.Objects.requireNonNull;

/**
 * A container for the data structures used by this {@link OfferManagerImpl}, to make it easier to
 * reason about the different indices used and their consistency.
 */
class HostOffers {
  private final Set<HostOffer> offers;

  private final Map<Protos.OfferID, HostOffer> offersById = Maps.newHashMap();
  private final Map<Protos.AgentID, HostOffer> offersBySlave = Maps.newHashMap();
  private final Map<String, HostOffer> offersByHost = Maps.newHashMap();

  // Keep track of offer->groupKey mappings that will never be matched to avoid redundant
  // scheduling attempts. See VetoGroup for more details on static ban.
  private final Cache<Pair<Protos.OfferID, TaskGroupKey>, Boolean> staticallyBannedOffers;
  private final SchedulingFilter schedulingFilter;

  // Keep track of globally banned offers that will never be matched to anything.
  private final Set<Protos.OfferID> globallyBannedOffers = Sets.newHashSet();

  // Keep track of the number of offers evaluated for vetoes when getting matching offers
  private final AtomicLong vetoEvaluatedOffers;

  HostOffers(StatsProvider statsProvider,
             OfferSettings offerSettings,
             SchedulingFilter schedulingFilter) {
    this.offers = new ConcurrentSkipListSet<>(offerSettings.getOrdering());
    this.staticallyBannedOffers = offerSettings
        .getStaticBanCacheBuilder()
        .build();
    this.schedulingFilter = requireNonNull(schedulingFilter);

    // Potential gotcha - since this is a ConcurrentSkipListSet, size() is more expensive.
    // Could track this separately if it turns out to pose problems.
    statsProvider.exportSize(OfferManagerImpl.OUTSTANDING_OFFERS, offers);
    statsProvider.makeGauge(OfferManagerImpl.STATICALLY_BANNED_OFFERS,
        staticallyBannedOffers::size);
    statsProvider.makeGauge(OfferManagerImpl.STATICALLY_BANNED_OFFERS_HIT_RATE,
        () -> staticallyBannedOffers.stats().hitRate());
    statsProvider.makeGauge(OfferManagerImpl.GLOBALLY_BANNED_OFFERS, globallyBannedOffers::size);

    vetoEvaluatedOffers = statsProvider.makeCounter(OfferManagerImpl.VETO_EVALUATED_OFFERS);
  }

  /**
   * Adds an offer while maintaining a guarantee that no two offers may exist with the same
   * agent ID.  If an offer exists with the same agent ID, the existing offer is removed
   * and returned, and {@code offer} is not added.
   *
   * @param offer Offer to add.
   * @return The pre-existing offer with the same agent ID as {@code offer}, if one exists,
   *         which will also be removed prior to returning.
   */
  synchronized Optional<HostOffer> addAndPreventAgentCollision(HostOffer offer) {
    HostOffer sameAgent = offersBySlave.get(offer.getOffer().getAgentId());
    if (sameAgent != null) {
      remove(sameAgent.getOffer().getId());
      return Optional.of(sameAgent);
    }

    addInternal(offer);
    return Optional.absent();
  }

  private void addInternal(HostOffer offer) {
    offers.add(offer);
    offersById.put(offer.getOffer().getId(), offer);
    offersBySlave.put(offer.getOffer().getAgentId(), offer);
    offersByHost.put(offer.getOffer().getHostname(), offer);
  }

  synchronized boolean remove(Protos.OfferID id) {
    HostOffer removed = offersById.remove(id);
    if (removed != null) {
      offers.remove(removed);
      offersBySlave.remove(removed.getOffer().getAgentId());
      offersByHost.remove(removed.getOffer().getHostname());
    }
    globallyBannedOffers.remove(id);
    return removed != null;
  }

  synchronized void addGlobalBan(Protos.OfferID offerId) {
    globallyBannedOffers.add(offerId);
  }

  synchronized void updateHostAttributes(IHostAttributes attributes) {
    HostOffer offer = offersByHost.remove(attributes.getHost());
    if (offer != null) {
      // Remove and re-add a host's offer to re-sort based on its new hostStatus
      remove(offer.getOffer().getId());
      addInternal(new HostOffer(offer.getOffer(), attributes));
    }
  }

  synchronized Optional<HostOffer> get(Protos.AgentID slaveId) {
    HostOffer offer = offersBySlave.get(slaveId);
    if (offer == null || globallyBannedOffers.contains(offer.getOffer().getId())) {
      return Optional.absent();
    }

    return Optional.of(offer);
  }

  /**
   * Returns an iterable giving the state of the offers at the time the method is called. Unlike
   * {@code getWeaklyConsistentOffers}, the underlying collection is a copy of the original and
   * will not be modified outside of the returned iterable.
   *
   * @return The offers currently known by the scheduler.
   */
  synchronized Iterable<HostOffer> getOffers() {
    return FluentIterable.from(offers)
        .filter(o -> !globallyBannedOffers.contains(o.getOffer().getId()))
        .toSet();
  }

  synchronized Optional<HostOffer> getMatching(Protos.AgentID slaveId,
                                               ResourceRequest resourceRequest,
                                               boolean revocable) {

    Optional<HostOffer> optionalOffer = get(slaveId);
    if (optionalOffer.isPresent()) {
      HostOffer offer = optionalOffer.get();

      if (isGloballyBanned(offer)
          || isVetoed(offer, resourceRequest, revocable, Optional.absent())) {

        return Optional.absent();
      }
    }

    return optionalOffer;
  }

  /**
   * Returns a weakly-consistent iterable giving the available offers to a given
   * {@code groupKey}. This iterable can handle concurrent operations on its underlying
   * collection, and may reflect changes that happen after the construction of the iterable.
   * This property is mainly used in {@code launchTask}.
   *
   * @param groupKey The task group to get offers for.
   * @return The offers a given task group can use.
   */
  synchronized Iterable<HostOffer> getAllMatching(TaskGroupKey groupKey,
                                                  ResourceRequest resourceRequest,
                                                  boolean revocable) {

    return Iterables.unmodifiableIterable(FluentIterable.from(offers)
        .filter(o -> !isGloballyBanned(o))
        .filter(o -> !isStaticallyBanned(o, groupKey))
        .filter(HostOffer::hasCpuAndMem)
        .filter(o -> !isVetoed(o, resourceRequest, revocable, Optional.of(groupKey))));
  }

  private synchronized boolean isGloballyBanned(HostOffer offer) {
    return globallyBannedOffers.contains(offer.getOffer().getId());
  }

  private synchronized boolean isStaticallyBanned(HostOffer offer, TaskGroupKey groupKey) {
    return staticallyBannedOffers.getIfPresent(Pair.of(offer.getOffer().getId(), groupKey)) != null;
  }

  /**
   * Determine whether or not the {@link HostOffer} is vetoed for the given {@link ResourceRequest}.
   * If {@code groupKey} is present, this method will also temporarily ban the offer from ever
   * matching the {@link TaskGroupKey}.
   */
  private boolean isVetoed(HostOffer offer,
                           ResourceRequest resourceRequest,
                           boolean revocable,
                           Optional<TaskGroupKey> groupKey) {

    vetoEvaluatedOffers.incrementAndGet();
    UnusedResource unusedResource = new UnusedResource(offer, revocable);
    Set<Veto> vetoes = schedulingFilter.filter(unusedResource, resourceRequest);
    if (!vetoes.isEmpty()) {
      if (groupKey.isPresent() && Veto.identifyGroup(vetoes) == SchedulingFilter.VetoGroup.STATIC) {
        addStaticGroupBan(offer.getOffer().getId(), groupKey.get());
      }

      return true;
    }

    return false;
  }

  @VisibleForTesting
  synchronized void addStaticGroupBan(Protos.OfferID offerId, TaskGroupKey groupKey) {
    if (offersById.containsKey(offerId)) {
      staticallyBannedOffers.put(Pair.of(offerId, groupKey), true);
    }
  }

  @VisibleForTesting
  synchronized Set<Pair<Protos.OfferID, TaskGroupKey>> getStaticBans() {
    return staticallyBannedOffers.asMap().keySet();
  }

  synchronized void clear() {
    offers.clear();
    offersById.clear();
    offersBySlave.clear();
    offersByHost.clear();
    staticallyBannedOffers.invalidateAll();
    globallyBannedOffers.clear();
  }

  @VisibleForTesting
  synchronized void cleanUpStaticallyBannedOffers() {
    staticallyBannedOffers.cleanUp();
  }
}
