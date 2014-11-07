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
package org.apache.aurora.scheduler.async;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.eventbus.Subscribe;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;

import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverDisconnected;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.HostMaintenanceStateChange;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.MaintenanceMode.DRAINED;
import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.gen.MaintenanceMode.SCHEDULED;

/**
 * Tracks the Offers currently known by the scheduler.
 */
public interface OfferQueue extends EventSubscriber {

  /**
   * Notifies the scheduler of a new resource offer.
   *
   * @param offer Newly-available resource offer.
   */
  void addOffer(Offer offer);

  /**
   * Invalidates an offer.  This indicates that the scheduler should not attempt to match any
   * tasks against the offer.
   *
   * @param offer Canceled offer.
   */
  void cancelOffer(OfferID offer);

  /**
   * Launches the first task that satisfies the {@code acceptor} by returning a {@link TaskInfo}.
   *
   * @param acceptor Function that determines if an offer is accepted.
   * @return {@code true} if the task was launched, {@code false} if no offers satisfied the
   *         {@code acceptor}.
   * @throws LaunchException If the acceptor accepted an offer, but there was an error launching the
   *                         task.
   */
  boolean launchFirst(Function<HostOffer, Optional<TaskInfo>> acceptor) throws LaunchException;

  /**
   * Notifies the offer queue that a host has changed state.
   *
   * @param change State change notification.
   */
  void hostChangedState(HostMaintenanceStateChange change);

  /**
   * Gets the offers that the scheduler is holding.
   *
   * @return A snapshot of the offers that the scheduler is currently holding.
   */
  Iterable<HostOffer> getOffers();

  /**
   * Calculates the amount of time before an offer should be 'returned' by declining it.
   * The delay is calculated for each offer that is received, so the return delay may be
   * fixed or variable.
   */
  interface OfferReturnDelay extends Supplier<Amount<Integer, Time>> {
  }

  /**
   * Thrown when there was an unexpected failure trying to launch a task.
   */
  class LaunchException extends Exception {
    LaunchException(String msg) {
      super(msg);
    }

    LaunchException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  /**
   * Encapsulate an offer from a host, and the host's maintenance mode.
   */
  class HostOffer {
    private final Offer offer;

    // TODO(wfarner): Replace this with HostAttributes for more use of this caching.
    private final MaintenanceMode mode;

    public HostOffer(Offer offer, MaintenanceMode mode) {
      this.offer = requireNonNull(offer);
      this.mode = requireNonNull(mode);
    }

    public Offer getOffer() {
      return offer;
    }

    public MaintenanceMode getMode() {
      return mode;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof HostOffer)) {
        return false;
      }
      HostOffer other = (HostOffer) o;
      return Objects.equals(offer, other.offer) && mode == other.mode;
    }

    @Override
    public int hashCode() {
      return Objects.hash(offer, mode);
    }

    @Override
    public String toString() {
      return com.google.common.base.Objects.toStringHelper(this)
          .add("offer", offer)
          .add("mode", mode)
          .toString();
    }
  }

  class OfferQueueImpl implements OfferQueue {
    private static final Logger LOG = Logger.getLogger(OfferQueueImpl.class.getName());

    private final HostOffers hostOffers = new HostOffers();
    private final AtomicLong offerRaces = Stats.exportLong("offer_accept_races");

    private final Driver driver;
    private final OfferReturnDelay returnDelay;
    private final ScheduledExecutorService executor;
    private final MaintenanceController maintenance;

    @Inject
    OfferQueueImpl(Driver driver,
        OfferReturnDelay returnDelay,
        ScheduledExecutorService executor,
        MaintenanceController maintenance) {

      this.driver = driver;
      this.returnDelay = returnDelay;
      this.executor = executor;
      this.maintenance = maintenance;
    }

    @Override
    public void addOffer(final Offer offer) {
      // We run a slight risk of a race here, which is acceptable.  The worst case is that we
      // temporarily hold two offers for the same host, which should be corrected when we return
      // them after the return delay.
      // There's also a chance that we return an offer for compaction ~simultaneously with the
      // same-host offer being canceled/returned.  This is also fine.
      Optional<HostOffer> sameSlave = hostOffers.get(offer.getSlaveId());
      if (sameSlave.isPresent()) {
        // If there are existing offers for the slave, decline all of them so the master can
        // compact all of those offers into a single offer and send them back.
        LOG.info("Returning offers for " + offer.getSlaveId().getValue() + " for compaction.");
        decline(offer.getId());
        removeAndDecline(sameSlave.get().offer.getId());
      } else {
        hostOffers.add(new HostOffer(offer, maintenance.getMode(offer.getHostname())));
        executor.schedule(
            new Runnable() {
              @Override
              public void run() {
                removeAndDecline(offer.getId());
              }
            },
            returnDelay.get().as(Time.MILLISECONDS),
            TimeUnit.MILLISECONDS);
      }
    }

    void removeAndDecline(OfferID id) {
      if (removeFromHostOffers(id)) {
        decline(id);
      }
    }

    void decline(OfferID id) {
      LOG.fine("Declining offer " + id);
      driver.declineOffer(id);
    }

    @Override
    public void cancelOffer(final OfferID offerId) {
      removeFromHostOffers(offerId);
    }

    private boolean removeFromHostOffers(final OfferID offerId) {
      requireNonNull(offerId);

      // The small risk of inconsistency is acceptable here - if we have an accept/remove race
      // on an offer, the master will mark the task as LOST and it will be retried.
      return hostOffers.remove(offerId);
    }

    @Override
    public Iterable<HostOffer> getOffers() {
      return hostOffers.getWeaklyConsistentOffers();
    }

    /**
     * Updates the preference of a host's offers.
     *
     * @param change Host change notification.
     */
    @Subscribe
    public void hostChangedState(HostMaintenanceStateChange change) {
      hostOffers.updateHostMode(change.getStatus().getHost(), change.getStatus().getMode());
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
     * A container for the data structures used by this class, to make it easier to reason about
     * the different indices used and their consistency.
     */
    private static class HostOffers {
      private static final Comparator<HostOffer> PREFERENCE_COMPARATOR =
          // Currently, the only preference is based on host maintenance status.
          Ordering.explicit(NONE, SCHEDULED, DRAINING, DRAINED)
              .onResultOf(new Function<HostOffer, MaintenanceMode>() {
                @Override
                public MaintenanceMode apply(HostOffer offer) {
                  return offer.mode;
                }
              })
              .compound(Ordering.arbitrary());

      private final Set<HostOffer> hostOffers = new ConcurrentSkipListSet<>(PREFERENCE_COMPARATOR);
      private final Map<OfferID, HostOffer> offersById = Maps.newHashMap();
      private final Map<SlaveID, HostOffer> offersBySlave = Maps.newHashMap();
      private final Map<String, HostOffer> offersByHost = Maps.newHashMap();

      HostOffers() {
        // Potential gotcha - since this is a ConcurrentSkipListSet, size() is more expensive.
        // Could track this separately if it turns out to pose problems.
        Stats.exportSize("outstanding_offers", hostOffers);
      }

      synchronized Optional<HostOffer> get(SlaveID slaveId) {
        return Optional.fromNullable(offersBySlave.get(slaveId));
      }

      synchronized void add(HostOffer offer) {
        hostOffers.add(offer);
        offersById.put(offer.offer.getId(), offer);
        offersBySlave.put(offer.offer.getSlaveId(), offer);
        offersByHost.put(offer.offer.getHostname(), offer);
      }

      synchronized boolean remove(OfferID id) {
        HostOffer removed = offersById.remove(id);
        if (removed != null) {
          hostOffers.remove(removed);
          offersBySlave.remove(removed.offer.getSlaveId());
          offersByHost.remove(removed.offer.getHostname());
        }
        return removed != null;
      }

      synchronized void updateHostMode(String hostName, MaintenanceMode mode) {
        HostOffer offer = offersByHost.remove(hostName);
        if (offer != null) {
          // Remove and re-add a host's offer to re-sort based on its new hostStatus
          remove(offer.offer.getId());
          add(new HostOffer(offer.offer, mode));
        }
      }

      synchronized Iterable<HostOffer> getWeaklyConsistentOffers() {
        return Iterables.unmodifiableIterable(hostOffers);
      }

      synchronized void clear() {
        hostOffers.clear();
        offersById.clear();
        offersBySlave.clear();
        offersByHost.clear();
      }
    }

    @Override
    public boolean launchFirst(Function<HostOffer, Optional<TaskInfo>> acceptor)
        throws LaunchException {

      // It's important that this method is not called concurrently - doing so would open up the
      // possibility of a race between the same offers being accepted by different threads.

      for (HostOffer hostOffer : hostOffers.getWeaklyConsistentOffers()) {
        Optional<TaskInfo> assignment = acceptor.apply(hostOffer);
        if (assignment.isPresent()) {
          // Guard against an offer being removed after we grabbed it from the iterator.
          // If that happens, the offer will not exist in hostOffers, and we can immediately
          // send it back to LOST for quick reschedule.
          // Removing while iterating counts on the use of a weakly-consistent iterator being used,
          // which is a feature of ConcurrentSkipListSet.
          if (hostOffers.remove(hostOffer.offer.getId())) {
            try {
              driver.launchTask(hostOffer.offer.getId(), assignment.get());
              return true;
            } catch (IllegalStateException e) {
              // TODO(William Farner): Catch only the checked exception produced by Driver
              // once it changes from throwing IllegalStateException when the driver is not yet
              // registered.
              throw new LaunchException("Failed to launch task.", e);
            }
          } else {
            offerRaces.incrementAndGet();
            throw new LaunchException(
                "Accepted offer no longer exists in offer queue, likely data race.");
          }
        }
      }

      return false;
    }
  }
}
