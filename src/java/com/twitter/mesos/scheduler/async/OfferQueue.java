package com.twitter.mesos.scheduler.async;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.TaskInfo;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.gen.HostStatus;
import com.twitter.mesos.gen.MaintenanceMode;
import com.twitter.mesos.scheduler.Driver;
import com.twitter.mesos.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.mesos.scheduler.events.PubsubEvent.HostMaintenanceStateChange;
import com.twitter.mesos.scheduler.state.MaintenanceController;

import static com.twitter.mesos.gen.MaintenanceMode.DRAINED;
import static com.twitter.mesos.gen.MaintenanceMode.DRAINING;
import static com.twitter.mesos.gen.MaintenanceMode.NONE;
import static com.twitter.mesos.gen.MaintenanceMode.SCHEDULED;

/**
 * Tracks the Offers currently known by the scheduler
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
  boolean launchFirst(Function<Offer, Optional<TaskInfo>> acceptor) throws LaunchException;

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
  Iterable<Offer> getOffers();

  /**
   * Calculates the amount of time before an offer should be 'returned' by declining it.
   * The delay is calculated for each offer that is received, so the return delay may be
   * fixed or variable.
   */
  public interface OfferReturnDelay extends Supplier<Amount<Integer, Time>> {
  }

  /**
   * Thrown when there was an unexpected failure trying to launch a task.
   */
  static class LaunchException extends Exception {
    LaunchException(String msg) {
      super(msg);
    }

    LaunchException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  class OfferQueueImpl implements OfferQueue {
    private static final Logger LOG = Logger.getLogger(OfferQueueImpl.class.getName());

    static final Comparator<HostOffer> PREFERENCE_COMPARATOR =
        // Currently, the only preference is based on host maintenance status.
        Ordering.explicit(NONE, SCHEDULED, DRAINING, DRAINED)
            .onResultOf(new Function<HostOffer, MaintenanceMode>() {
              @Override public MaintenanceMode apply(HostOffer offer) {
                return offer.mode;
              }
            })
            .compound(Ordering.arbitrary());

    final Set<HostOffer> hostOffers = new ConcurrentSkipListSet<HostOffer>(PREFERENCE_COMPARATOR);
    final AtomicLong offerRaces = Stats.exportLong("offer_accept_races");

    final Driver driver;
    final OfferReturnDelay returnDelay;
    final ScheduledExecutorService executor;
    final MaintenanceController maintenance;

    @Inject
    OfferQueueImpl(Driver driver,
        OfferReturnDelay returnDelay,
        ScheduledExecutorService executor,
        MaintenanceController maintenance) {

      this.driver = driver;
      this.returnDelay = returnDelay;
      this.executor = executor;
      this.maintenance = maintenance;
      // Potential gotcha - since this is now a ConcurrentSkipListSet, size() is more expensive.
      // Could track this separately if it turns out to pose problems.
      Stats.exportSize("outstanding_offers", hostOffers);
    }

    @Override
    public void addOffer(final Offer offer) {
      // We run a slight risk of a race here, which is acceptable.  The worst case is that we
      // temporarily hold two offers for the same host, which should be corrected when we return
      // them after the return delay.
      // There's also a chance that we return an offer for compaction ~simultaneously with the
      // same-host offer being canceled/returned.  This is also fine.
      List<HostOffer> sameSlave = FluentIterable.from(hostOffers)
          .filter(new Predicate<HostOffer>() {
            @Override public boolean apply(HostOffer hostOffer) {
              return hostOffer.offer.getSlaveId().equals(offer.getSlaveId());
            }
          })
          .toList();
      if (sameSlave.isEmpty()) {
        hostOffers.add(new HostOffer(offer, maintenance.getMode(offer.getHostname())));
        executor.schedule(
            new Runnable() {
              @Override public void run() {
                decline(offer.getId());
              }
            },
            returnDelay.get().as(Time.MILLISECONDS),
            TimeUnit.MILLISECONDS);
      } else {
        LOG.info("Returning " + (sameSlave.size() + 1)
            + " offers for " + offer.getSlaveId().getValue() + " for compaction.");
        decline(offer.getId());
        for (HostOffer sameSlaveOffer : sameSlave) {
          decline(sameSlaveOffer.offer.getId());
        }
      }
    }

    void decline(OfferID id) {
      LOG.fine("Declining offer " + id);
      cancelOffer(id);
      driver.declineOffer(id);
    }

    @Override
    public void cancelOffer(final OfferID offerId) {
      Preconditions.checkNotNull(offerId);

      // The small risk of inconsistency is acceptable here - if we have an accept/remove race
      // on an offer, the master will mark the task as LOST and it will be retried.
      Iterables.removeIf(hostOffers,
          new Predicate<HostOffer>() {
            @Override public boolean apply(HostOffer input) {
              return input.offer.getId().equals(offerId);
            }
          });
    }

    @Override
    public Iterable<Offer> getOffers() {
      return Iterables.unmodifiableIterable(
          FluentIterable.from(hostOffers)
              .transform(new Function<HostOffer, Offer>() {
                @Override public Offer apply(HostOffer offer) {
                  return offer.offer;
                }
              }));
    }

    /**
     * Updates the preference of a host's offers.
     *
     * @param change Host change notification.
     */
    @Subscribe
    public void hostChangedState(HostMaintenanceStateChange change) {
      final HostStatus hostStatus = change.getStatus();

      // Remove and re-add a host's offers to re-sort based on its new hostStatus
      Set<HostOffer> changedOffers = FluentIterable.from(hostOffers)
          .filter(new Predicate<HostOffer>() {
            @Override public boolean apply(HostOffer hostOffer) {
              return hostOffer.offer.getHostname().equals(hostStatus.getHost());
            }
          })
          .toSet();
      hostOffers.removeAll(changedOffers);
      hostOffers.addAll(
          FluentIterable.from(changedOffers)
              .transform(new Function<HostOffer, HostOffer>() {
                @Override public HostOffer apply(HostOffer hostOffer) {
                  return new HostOffer(hostOffer.offer, hostStatus.getMode());
                }
              })
              .toSet());
    }

    /**
     * Encapsulate an offer from a host, and the host's maintenance mode.
     */
    private static class HostOffer {
      final Offer offer;
      final MaintenanceMode mode;

      HostOffer(Offer offer, MaintenanceMode mode) {
        this.offer = offer;
        this.mode = mode;
      }

      @Override
      public boolean equals(Object o) {
        if (!(o instanceof HostOffer)) {
          return false;
        }
        HostOffer other = (HostOffer) o;
        return Objects.equal(offer, other.offer) && (mode == other.mode);
      }

      @Override
      public int hashCode() {
        return Objects.hashCode(offer, mode);
      }
    }

    @Override
    public boolean launchFirst(Function<Offer, Optional<TaskInfo>> acceptor)
        throws LaunchException {

      // It's important that this method is not called concurrently - doing so would open up the
      // possibility of a race between the same offers being accepted by different threads.

      for (HostOffer hostOffer : hostOffers) {
        Optional<TaskInfo> assignment = acceptor.apply(hostOffer.offer);
        if (assignment.isPresent()) {
          // Guard against an offer being removed after we grabbed it from the iterator.
          // If that happens, the offer will not exist in hostOffers, and we can immediately
          // send it back to LOST for quick reschedule.
          if (hostOffers.remove(hostOffer)) {
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
