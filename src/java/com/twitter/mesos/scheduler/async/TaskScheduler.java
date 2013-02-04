package com.twitter.mesos.scheduler.async;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.common.util.Clock;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.Driver;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.StateManager;
import com.twitter.mesos.scheduler.TaskAssigner;
import com.twitter.mesos.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.events.PubsubEvent.TasksDeleted;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;

/**
 * An asynchronous task scheduler.  Scheduling of tasks is performed on a delay, where each task
 * backs off after a failed scheduling attempt.
 */
public interface TaskScheduler extends EventSubscriber {

  /**
   * Notifies the scheduler of new resource ofers.
   *
   * @param offers Newly-available resource offers.
   */
  void offer(Collection<Offer> offers);

  /**
   * Invalidates an offer.  This indicates that the scheduler should not attempt to match any
   * tasks against the offer.
   *
   * @param offer Canceled offer.
   */
  void cancelOffer(OfferID offer);

  /**
   * Gets the offers that the scheduler is holding.
   *
   * @return A snapshot of the offers that the scheduler is currently holding.
   */
  @VisibleForTesting
  Iterable<Offer> getOffers();

  /**
   * A task scheduler that learns of pending tasks via internal pubsub notifications.
   * <p>
   * Additionally, it will automatically return resource offers after a configurable amount of time.
   */
  class TaskSchedulerImpl implements TaskScheduler {

    private static final Logger LOG = Logger.getLogger(TaskSchedulerImpl.class.getName());

    @VisibleForTesting
    static final long OFFER_CLEANUP_INTERVAL_SECS = 5;

    @VisibleForTesting
    final Map<String, Future<?>> futures =
        Collections.synchronizedMap(Maps.<String, Future<?>>newHashMap());

    private final Storage storage;
    private final StateManager stateManager;
    private final TaskAssigner assigner;
    private final BackoffStrategy pendingRetryStrategy;
    private final ScheduledExecutorService executor;
    private final Offers offers;

    private final AtomicLong scheduleAttemptsFired = Stats.exportLong("schedule_attempts_fired");
    private final AtomicLong scheduleAttemptsFailed = Stats.exportLong("schedule_attempts_failed");

    @VisibleForTesting
    TaskSchedulerImpl(
        Storage storage,
        StateManager stateManager,
        TaskAssigner assigner,
        BackoffStrategy pendingRetryStrategy,
        Driver driver,
        final ScheduledExecutorService executor,
        Amount<Long, Time> offerExpiry,
        Clock clock) {

      this.storage = checkNotNull(storage);
      this.stateManager = checkNotNull(stateManager);
      this.assigner = checkNotNull(assigner);
      this.pendingRetryStrategy = checkNotNull(pendingRetryStrategy);
      this.executor = checkNotNull(executor);
      this.offers = new Offers(driver, offerExpiry, clock);

      LOG.info("Declining resource offers not accepted for " + offerExpiry);
      executor.scheduleAtFixedRate(
          new Runnable() {
            @Override public void run() {
              offers.declineExpired();
            }
          },
          OFFER_CLEANUP_INTERVAL_SECS,
          OFFER_CLEANUP_INTERVAL_SECS,
          TimeUnit.SECONDS);
    }

    @Inject
    TaskSchedulerImpl(
        Storage storage,
        StateManager stateManager,
        TaskAssigner assigner,
        BackoffStrategy pendingRetryStrategy,
        Driver driver,
        Amount<Long, Time> offerExpiry,
        ShutdownRegistry shutdownRegistry) {

      this(
          storage,
          stateManager,
          assigner,
          pendingRetryStrategy,
          driver,
          createThreadPool(shutdownRegistry),
          offerExpiry,
          Clock.SYSTEM_CLOCK);
    }

    private static ScheduledExecutorService createThreadPool(ShutdownRegistry shutdownRegistry) {
      final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
          1,
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("TaskScheduler-%d").build());
      Stats.exportSize("schedule_queue_size", executor.getQueue());
      shutdownRegistry.addAction(new Command() {
        @Override public void execute() {
          new ExecutorServiceShutdown(executor, Amount.of(1L, Time.SECONDS)).execute();
        }
      });
      return executor;
    }

    @Override
    public void offer(Collection<Offer> newOffers) {
      for (Offer offer : newOffers) {
        offers.receive(offer);
      }
    }

    @Override
    public void cancelOffer(OfferID offerId) {
      Preconditions.checkNotNull(offerId);

      // The small risk of inconsistency is acceptable here - if we have an accept/remove race
      // on an offer, the master will mark the task as LOST and it will be retried.
      offers.remove(offerId);
    }

    private static final Function<ExpiringOffer, Offer> GET_OFFER =
        new Function<ExpiringOffer, Offer>() {
          @Override public Offer apply(ExpiringOffer offer) {
            return offer.offer;
          }
        };

    @Override
    public Iterable<Offer> getOffers() {
      return offers.getAll();
    }

    private void removeTask(String taskId, boolean cancelIfPresent) {
      Future<?> future = futures.remove(taskId);
      if (cancelIfPresent && (future != null)) {
        future.cancel(true);
      }
    }

    private void addTask(String taskId, long delayMillis, boolean cancelIfPresent) {
      removeTask(taskId, cancelIfPresent);
      LOG.fine("Evaluating task " + taskId + " in " + delayMillis + " ms");
      PendingTask task = new PendingTask(taskId, delayMillis);
      futures.put(
          taskId,
          executor.schedule(task, delayMillis, TimeUnit.MILLISECONDS));
    }

    @Subscribe
    public void taskChangedState(TaskStateChange stateChange) {
      // TODO(William Farner): Use the ancestry of a task to check if the task is flapping, use
      // this to implement scheduling backoff (inducing a longer delay).
      String taskId = stateChange.getTaskId();
      if (stateChange.getNewState() == PENDING) {
        addTask(taskId, pendingRetryStrategy.calculateBackoffMs(0), true);
      }
    }

    @Subscribe
    public void tasksDeleted(TasksDeleted deletion) {
      for (ScheduledTask task : deletion.getTasks()) {
        removeTask(Tasks.id(task), true);
      }
    }

    @Subscribe
    public void storageStarted(StorageStarted event) {
      // TODO(William Farner): This being separate from TaskStateChange events is asking for a bug
      // where a subscriber forgets to handle storage start.
      // Wrap this concept into TaskStateChange.
      storage.doInTransaction(new Work.Quiet<Void>() {
        @Override public Void apply(StoreProvider store) {
          for (ScheduledTask task : store.getTaskStore().fetchTasks(Query.byStatus(PENDING))) {
            addTask(Tasks.id(task), pendingRetryStrategy.calculateBackoffMs(0), true);
          }
          return null;
        }
      });
    }

    @VisibleForTesting
    static final Optional<String> LAUNCH_FAILED_MSG =
        Optional.of("Unknown exception attempting to schedule task.");

    private class PendingTask implements Runnable {
      final String taskId;
      final long penaltyMs;
      final TaskQuery selfQuery;

      PendingTask(String taskId, long penaltyMs) {
        this.taskId = taskId;
        this.penaltyMs = penaltyMs;
        this.selfQuery = new TaskQuery()
            .setStatuses(ImmutableSet.of(PENDING))
            .setTaskIds(ImmutableSet.of(taskId));
      }

      private void runInTransaction(StoreProvider store) {
        removeTask(taskId, false);
        LOG.fine("Attempting to schedule task " + taskId);
        final ScheduledTask task =
            Iterables.getOnlyElement(store.getTaskStore().fetchTasks(selfQuery), null);
        if (task == null) {
          LOG.warning("Failed to look up task " + taskId);
        } else {
          Function<ExpiringOffer, Optional<TaskInfo>> assignment =
              new Function<ExpiringOffer, Optional<TaskInfo>>() {
                @Override public Optional<TaskInfo> apply(ExpiringOffer expiring) {
                  return assigner.maybeAssign(expiring.offer, task);
                }
              };
          try {
            if (!offers.launchFirst(assignment)) {
              // Task could not be scheduled.
              retryLater();
            }
          } catch (Offers.LaunchException e) {
            LOG.log(Level.WARNING, "Failed to launch task.", e);
            scheduleAttemptsFailed.incrementAndGet();

            // The attempt to schedule the task failed, so we need to backpedal on the
            // assignment.
            // It is in the LOST state and a new task will move to PENDING to replace it.
            // Should the state change fail due to storage issues, that's okay.  The task will
            // time out in the ASSIGNED state and be moved to LOST.
            stateManager.changeState(selfQuery, LOST, LAUNCH_FAILED_MSG);
          }
        }
      }

      @Override public void run() {
        scheduleAttemptsFired.incrementAndGet();
        try {
          storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
            @Override public void execute(MutableStoreProvider store) {
              runInTransaction(store);
            }
          });
        } catch (RuntimeException e) {
          // We catch the generic unchecked exception here to ensure tasks are not abandoned
          // if there is a transient issue resulting in an unchecked exception.
          LOG.log(Level.WARNING, "Task scheduling unexpectedly failed, will be retried", e);
          scheduleAttemptsFailed.incrementAndGet();
          retryLater();
        }
      }

      private void retryLater() {
        addTask(taskId, pendingRetryStrategy.calculateBackoffMs(penaltyMs), false);
      }
    }

    private static class ExpiringOffer {
      final Offer offer;
      final long expirationMs;

      ExpiringOffer(Offer offer, long expirationMs) {
        this.offer = offer;
        this.expirationMs = expirationMs;
      }
    }

    private static class Offers {
      private final Map<OfferID, ExpiringOffer> offers = Maps.newHashMap();
      private final Driver driver;
      private final long offerExpiryMs;
      private final Clock clock;
      private final Predicate<ExpiringOffer> isExpired;

      Offers(Driver driver, Amount<Long, Time> offerExpiry, final Clock clock) {
        this.driver = driver;
        this.offerExpiryMs = offerExpiry.as(Time.MILLISECONDS);
        this.clock = clock;
        isExpired = new Predicate<ExpiringOffer>() {
          @Override public boolean apply(ExpiringOffer expiring) {
            return clock.nowMillis() >= expiring.expirationMs;
          }
        };

        Stats.exportSize("outstanding_offers", offers);
      }

      synchronized void remove(OfferID id) {
        offers.remove(id);
      }

      private static Predicate<ExpiringOffer> isSlave(final SlaveID slave) {
        return new Predicate<ExpiringOffer>() {
          @Override public boolean apply(ExpiringOffer expiring) {
            return expiring.offer.getSlaveId().equals(slave);
          }
        };
      }

      synchronized void receive(Offer offer) {
        List<ExpiringOffer> sameSlave = FluentIterable.from(offers.values())
            .filter(isSlave(offer.getSlaveId()))
            .toImmutableList();
        if (sameSlave.isEmpty()) {
          offers.put(offer.getId(), new ExpiringOffer(offer, clock.nowMillis() + offerExpiryMs));
        } else {
          LOG.info("Returning " + (sameSlave.size() + 1)
              + " offers for " + offer.getSlaveId().getValue() + " for compaction.");
          decline(offer.getId());
          for (ExpiringOffer expiring : sameSlave) {
            decline(expiring.offer.getId());
          }
        }
      }

      synchronized void decline(OfferID id) {
        LOG.fine("Declining offer " + id);
        remove(id);
        driver.declineOffer(id);
      }

      synchronized boolean launchFirst(Function<ExpiringOffer, Optional<TaskInfo>> acceptor)
          throws LaunchException {

        for (ExpiringOffer expiring : offers.values()) {
          Optional<TaskInfo> assignment = acceptor.apply(expiring);
          if (assignment.isPresent()) {
            OfferID id = expiring.offer.getId();
            remove(id);
            try {
              driver.launchTask(id, assignment.get());
            } catch (RuntimeException e) {
              // TODO(William Farner): Catch only the checked exception produced by Driver
              // once it changes from throwing IllegalStateException when the driver is not yet
              // registered.
              throw new LaunchException("Failed to launch task.", e);
            }
            return true;
          }
        }
        return false;
      }

      static class LaunchException extends Exception {
        LaunchException(String msg, Throwable cause) {
          super(msg, cause);
        }
      }

      synchronized void declineExpired() {
        for (ExpiringOffer expired
            : FluentIterable.from(offers.values()).filter(isExpired).toImmutableList()) {

          decline(expired.offer.getId());
        }
      }

      synchronized List<Offer> getAll() {
        return FluentIterable.from(offers.values()).transform(GET_OFFER).toImmutableList();
      }
    }
  }
}
