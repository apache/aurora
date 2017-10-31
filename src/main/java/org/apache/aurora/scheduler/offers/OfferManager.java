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

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;

import org.apache.aurora.common.collections.Pair;
import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverDisconnected;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.AgentID;
import org.apache.mesos.v1.Protos.Offer.Operation;
import org.apache.mesos.v1.Protos.OfferID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.events.PubsubEvent.HostAttributesChanged;

/**
 * Tracks the Offers currently known by the scheduler.
 */
public interface OfferManager extends EventSubscriber {

  /**
   * Notifies the scheduler of a new resource offer.
   *
   * @param offer Newly-available resource offer.
   */
  void addOffer(HostOffer offer);

  /**
   * Invalidates an offer.  This indicates that the scheduler should not attempt to match any
   * tasks against the offer.
   *
   * @param offerId Cancelled offer.
   * @return A boolean on whether or not the offer was successfully cancelled.
   */
  boolean cancelOffer(OfferID offerId);

  /**
   * Exclude an offer from being matched against all tasks.
   *
   * @param offerId Offer ID to ban.
   */
  void banOffer(OfferID offerId);

  /**
   * Exclude an offer that results in a static mismatch from further attempts to match against all
   * tasks from the same group.
   *
   * @param offerId Offer ID to exclude for the given {@code groupKey}.
   * @param groupKey Task group key to exclude.
   */
  void banOfferForTaskGroup(OfferID offerId, TaskGroupKey groupKey);

  /**
   * Launches the task matched against the offer.
   *
   * @param offerId Matched offer ID.
   * @param task Matched task info.
   * @throws LaunchException If there was an error launching the task.
   */
  void launchTask(OfferID offerId, Protos.TaskInfo task) throws LaunchException;

  /**
   * Notifies the offer queue that a host's attributes have changed.
   *
   * @param change State change notification.
   */
  void hostAttributesChanged(HostAttributesChanged change);

  /**
   * Gets the offers that the scheduler is holding, excluding banned offers.
   *
   * @return A snapshot of the offers that the scheduler is currently holding.
   */
  Iterable<HostOffer> getOffers();

  /**
   * Gets all offers that are not banned for the given {@code groupKey}.
   *
   * @param groupKey Task group key to check offers for.
   * @return A snapshot of all offers eligible for the given {@code groupKey}.
   */
  Iterable<HostOffer> getOffers(TaskGroupKey groupKey);

  /**
   * Gets an offer for the given slave ID.
   *
   * @param slaveId Slave ID to get offer for.
   * @return An offer for the slave ID.
   */
  Optional<HostOffer> getOffer(AgentID slaveId);

  /**
   * Thrown when there was an unexpected failure trying to launch a task.
   */
  class LaunchException extends Exception {
    @VisibleForTesting
    public LaunchException(String msg) {
      super(msg);
    }

    LaunchException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  class OfferManagerImpl implements OfferManager {
    @VisibleForTesting
    static final Logger LOG = LoggerFactory.getLogger(OfferManagerImpl.class);
    @VisibleForTesting
    static final String OFFER_ACCEPT_RACES = "offer_accept_races";
    @VisibleForTesting
    static final String OUTSTANDING_OFFERS = "outstanding_offers";
    @VisibleForTesting
    static final String STATICALLY_BANNED_OFFERS = "statically_banned_offers_size";
    @VisibleForTesting
    static final String STATICALLY_BANNED_OFFERS_HIT_RATE = "statically_banned_offers_hit_rate";
    @VisibleForTesting
    static final String OFFER_CANCEL_FAILURES = "offer_cancel_failures";
    @VisibleForTesting
    static final String GLOBALLY_BANNED_OFFERS = "globally_banned_offers_size";

    private final HostOffers hostOffers;
    private final AtomicLong offerRaces;
    private final AtomicLong offerCancelFailures;

    private final Driver driver;
    private final OfferSettings offerSettings;
    private final Deferment offerDecline;

    @Inject
    @VisibleForTesting
    public OfferManagerImpl(
        Driver driver,
        OfferSettings offerSettings,
        StatsProvider statsProvider,
        Deferment offerDecline) {

      this.driver = requireNonNull(driver);
      this.offerSettings = requireNonNull(offerSettings);
      this.hostOffers = new HostOffers(statsProvider, offerSettings);
      this.offerRaces = statsProvider.makeCounter(OFFER_ACCEPT_RACES);
      this.offerCancelFailures = statsProvider.makeCounter(OFFER_CANCEL_FAILURES);
      this.offerDecline = requireNonNull(offerDecline);
    }

    @Override
    public void addOffer(HostOffer offer) {
      Optional<HostOffer> sameAgent = hostOffers.addAndPreventAgentCollision(offer);
      if (sameAgent.isPresent()) {
        // We have an existing offer for the same agent.  We choose to return both offers so that
        // they may be combined into a single offer.
        LOG.info("Returning offers for " + offer.getOffer().getAgentId().getValue()
            + " for compaction.");
        decline(offer.getOffer().getId());
        decline(sameAgent.get().getOffer().getId());
      } else {
        offerDecline.defer(() -> removeAndDecline(offer.getOffer().getId()));
      }
    }

    private void removeAndDecline(OfferID id) {
      if (removeFromHostOffers(id)) {
        decline(id);
      }
    }

    private void decline(OfferID id) {
      LOG.debug("Declining offer {}", id);
      driver.declineOffer(id, getOfferFilter());
    }

    private Protos.Filters getOfferFilter() {
      return Protos.Filters.newBuilder()
          .setRefuseSeconds(offerSettings.getFilterDuration().as(Time.SECONDS))
          .build();
    }

    @Override
    public boolean cancelOffer(final OfferID offerId) {
      boolean success = removeFromHostOffers(offerId);
      if (!success) {
        // This will happen rarely when we race to process this rescind against accepting the offer
        // to launch a task.
        // If it happens frequently, we are likely processing rescinds before the offer itself.
        LOG.warn("Failed to cancel offer: {}.", offerId.getValue());
        this.offerCancelFailures.incrementAndGet();
      }
      return success;
    }

    @Override
    public void banOffer(OfferID offerId) {
      hostOffers.addGlobalBan(offerId);
    }

    private boolean removeFromHostOffers(final OfferID offerId) {
      requireNonNull(offerId);

      // The small risk of inconsistency is acceptable here - if we have an accept/remove race
      // on an offer, the master will mark the task as LOST and it will be retried.
      return hostOffers.remove(offerId);
    }

    @Override
    public Iterable<HostOffer> getOffers() {
      return hostOffers.getOffers();
    }

    @Override
    public Iterable<HostOffer> getOffers(TaskGroupKey groupKey) {
      return hostOffers.getWeaklyConsistentOffers(groupKey);
    }

    @Override
    public Optional<HostOffer> getOffer(AgentID slaveId) {
      return hostOffers.get(slaveId);
    }

    /**
     * Updates the preference of a host's offers.
     *
     * @param change Host change notification.
     */
    @Subscribe
    public void hostAttributesChanged(HostAttributesChanged change) {
      hostOffers.updateHostAttributes(change.getAttributes());
    }

    /**
     * Notifies the queue that the driver is disconnected, and all the stored offers are now
     * invalid.
     * <p>
     * The queue takes this as a signal to flush its queue.
     *
     * @param event Disconnected event.
     */
    @Subscribe
    public void driverDisconnected(DriverDisconnected event) {
      LOG.info("Clearing stale offers since the driver is disconnected.");
      hostOffers.clear();
    }

    /**
     * Used for testing to ensure that the underlying cache's `size` method returns an accurate
     * value by not including evicted entries.
     */
    @VisibleForTesting
    public void cleanupStaticBans() {
      hostOffers.staticallyBannedOffers.cleanUp();
    }

    /**
     * A container for the data structures used by this class, to make it easier to reason about
     * the different indices used and their consistency.
     */
    private static class HostOffers {

      private final Set<HostOffer> offers;
      private final Map<OfferID, HostOffer> offersById = Maps.newHashMap();
      private final Map<AgentID, HostOffer> offersBySlave = Maps.newHashMap();
      private final Map<String, HostOffer> offersByHost = Maps.newHashMap();

      // Keep track of offer->groupKey mappings that will never be matched to avoid redundant
      // scheduling attempts. See VetoGroup for more details on static ban.
      private final Cache<Pair<OfferID, TaskGroupKey>, Boolean> staticallyBannedOffers;

      // Keep track of globally banned offers that will never be matched to anything.
      private final Set<OfferID> globallyBannedOffers = Sets.newConcurrentHashSet();

      HostOffers(StatsProvider statsProvider, OfferSettings offerSettings) {
        offers = new ConcurrentSkipListSet<>(offerSettings.getOrdering());
        staticallyBannedOffers = offerSettings
            .getStaticBanCacheBuilder()
            .build();
        // Potential gotcha - since this is a ConcurrentSkipListSet, size() is more expensive.
        // Could track this separately if it turns out to pose problems.
        statsProvider.exportSize(OUTSTANDING_OFFERS, offers);
        statsProvider.makeGauge(STATICALLY_BANNED_OFFERS, staticallyBannedOffers::size);
        statsProvider.makeGauge(STATICALLY_BANNED_OFFERS_HIT_RATE,
            () -> staticallyBannedOffers.stats().hitRate());
        statsProvider.makeGauge(GLOBALLY_BANNED_OFFERS, globallyBannedOffers::size);
      }

      synchronized Optional<HostOffer> get(AgentID slaveId) {
        HostOffer offer = offersBySlave.get(slaveId);
        if (offer == null || globallyBannedOffers.contains(offer.getOffer().getId())) {
          return Optional.absent();
        }

        return Optional.of(offer);
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

      synchronized boolean remove(OfferID id) {
        HostOffer removed = offersById.remove(id);
        if (removed != null) {
          offers.remove(removed);
          offersBySlave.remove(removed.getOffer().getAgentId());
          offersByHost.remove(removed.getOffer().getHostname());
        }
        globallyBannedOffers.remove(id);
        return removed != null;
      }

      synchronized void updateHostAttributes(IHostAttributes attributes) {
        HostOffer offer = offersByHost.remove(attributes.getHost());
        if (offer != null) {
          // Remove and re-add a host's offer to re-sort based on its new hostStatus
          remove(offer.getOffer().getId());
          addInternal(new HostOffer(offer.getOffer(), attributes));
        }
      }

      /**
       * Returns an iterable giving the state of the offers at the time the method is called. Unlike
       * {@code getWeaklyConsistentOffers}, the underlying collection is a copy of the original and
       * will not be modified outside of the returned iterable.
       *
       * @return The offers currently known by the scheduler.
       */
      synchronized Iterable<HostOffer> getOffers() {
        return FluentIterable.from(offers).filter(
            e -> !globallyBannedOffers.contains(e.getOffer().getId())
        ).toSet();
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
      synchronized Iterable<HostOffer> getWeaklyConsistentOffers(TaskGroupKey groupKey) {
        return Iterables.unmodifiableIterable(FluentIterable.from(offers).filter(e ->
            staticallyBannedOffers.getIfPresent(Pair.of(e.getOffer().getId(), groupKey)) == null
                && !globallyBannedOffers.contains(e.getOffer().getId())));
      }

      synchronized void addGlobalBan(OfferID offerId) {
        globallyBannedOffers.add(offerId);
      }

      synchronized void addStaticGroupBan(OfferID offerId, TaskGroupKey groupKey) {
        if (offersById.containsKey(offerId)) {
          staticallyBannedOffers.put(Pair.of(offerId, groupKey), true);
        }
      }

      synchronized void clear() {
        offers.clear();
        offersById.clear();
        offersBySlave.clear();
        offersByHost.clear();
        staticallyBannedOffers.invalidateAll();
        globallyBannedOffers.clear();
      }
    }

    @Override
    public void banOfferForTaskGroup(OfferID offerId, TaskGroupKey groupKey) {
      hostOffers.addStaticGroupBan(offerId, groupKey);
    }

    @Timed("offer_manager_launch_task")
    @Override
    public void launchTask(OfferID offerId, Protos.TaskInfo task) throws LaunchException {
      // Guard against an offer being removed after we grabbed it from the iterator.
      // If that happens, the offer will not exist in hostOffers, and we can immediately
      // send it back to LOST for quick reschedule.
      // Removing while iterating counts on the use of a weakly-consistent iterator being used,
      // which is a feature of ConcurrentSkipListSet.
      if (hostOffers.remove(offerId)) {
        try {
          Operation launch = Operation.newBuilder()
              .setType(Operation.Type.LAUNCH)
              .setLaunch(Operation.Launch.newBuilder().addTaskInfos(task))
              .build();
          driver.acceptOffers(offerId, ImmutableList.of(launch), getOfferFilter());
        } catch (IllegalStateException e) {
          // TODO(William Farner): Catch only the checked exception produced by Driver
          // once it changes from throwing IllegalStateException when the driver is not yet
          // registered.
          throw new LaunchException("Failed to launch task.", e);
        }
      } else {
        offerRaces.incrementAndGet();
        throw new LaunchException("Offer no longer exists in offer queue, likely data race.");
      }
    }
  }
}
