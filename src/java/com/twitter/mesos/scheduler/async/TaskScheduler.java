package com.twitter.mesos.scheduler.async;

import java.util.Collection;
import java.util.Collections;
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
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.TaskInfo;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.BackoffStrategy;
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
   * A task scheduler that learns of pending tasks via internal pubsub notifications.
   * <p>
   * Additionally, it will automatically return resource offers after a configurable amount of time.
   */
  class TaskSchedulerImpl implements TaskScheduler {

    private static final Logger LOG = Logger.getLogger(TaskSchedulerImpl.class.getName());

    @VisibleForTesting
    final Map<String, Future<?>> futures =
        Collections.synchronizedMap(Maps.<String, Future<?>>newHashMap());

    private final Storage storage;
    private final StateManager stateManager;
    private final TaskAssigner assigner;
    private final BackoffStrategy pendingRetryStrategy;
    private final Driver driver;
    private final ScheduledExecutorService executor;
    private final Cache<OfferID, ExpiringOffer> offers;

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
        Ticker ticker) {

      this.storage = checkNotNull(storage);
      this.stateManager = checkNotNull(stateManager);
      this.assigner = checkNotNull(assigner);
      this.pendingRetryStrategy = checkNotNull(pendingRetryStrategy);
      this.driver = checkNotNull(driver);
      this.executor = checkNotNull(executor);

      LOG.info("Declining resource offers not accepted for " + offerExpiry);
      offers = CacheBuilder.newBuilder()
          .expireAfterWrite(offerExpiry.as(Time.MILLISECONDS), TimeUnit.MILLISECONDS)
          .ticker(ticker)
          .removalListener(new RemovalListener<OfferID, ExpiringOffer>() {
            @Override public void onRemoval(RemovalNotification<OfferID, ExpiringOffer> removal) {
              ExpiringOffer offer = removal.getValue();
              if (offer != null) {
                offer.maybeDecline();
              }
            }
          })
          .build();
      Stats.export(new StatImpl<Long>("outstanding_offers") {
        @Override public Long read() {
          return offers.size();
        }
      });
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
          Ticker.systemTicker());
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
      this.offers.putAll(Maps.uniqueIndex(Iterables.transform(newOffers, wrap), GET_ID));
    }

    @Override
    public void cancelOffer(OfferID offerId) {
      Preconditions.checkNotNull(offerId);

      // The small risk of inconsistency is acceptable here - if we have an accept/remove race
      // on an offer, the master will mark the task as LOST and it will be retried.
      ExpiringOffer offer = offers.getIfPresent(offerId);
      if (offer != null) {
        // Declining the offer would be a no-op, so we disable decline here to prevent an
        // auto-decline when it is invalidated.
        offer.disableDecline();
      }
      offers.invalidate(offerId);
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
      // this to implement scheduling backoff (inducing a longer delay) and remove ScheduleBackoff.
      String taskId = stateChange.getTaskId();
      if (stateChange.getNewState() == PENDING) {
        addTask(taskId, pendingRetryStrategy.calculateBackoffMs(0), true);
      }
    }

    @Subscribe
    public void tasksDeleted(TasksDeleted deletion) {
      for (String taskId : deletion.getTaskIds()) {
        removeTask(taskId, true);
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
    static final Optional<String> NOT_REGISTERED_MSG = Optional.of("Scheduler is not registered.");

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
        ScheduledTask task =
            Iterables.getOnlyElement(store.getTaskStore().fetchTasks(selfQuery), null);
        if (task == null) {
          LOG.warning("Failed to look up task " + taskId);
        } else {
          // According to docs for CacheBuilder, cache iterators are weakly consistent
          // and never throw ConcurrentModificationException.
          for (Map.Entry<OfferID, ExpiringOffer> entry : offers.asMap().entrySet()) {
            ExpiringOffer expiring = entry.getValue();
            Offer offer = expiring.offer;
            Optional<TaskInfo> assignment = assigner.maybeAssign(offer, task);
            if (assignment.isPresent()) {
              expiring.disableDecline();
              offers.invalidate(offer.getId());
              try {
                driver.launchTask(offer.getId(), assignment.get());
              } catch (RuntimeException e) {
                // TODO(William Farner): Catch only the checked exception produced by Driver
                // once it changes from throwing IllegalStateException when the driver is not yet
                // registered.

                LOG.log(Level.WARNING, "Failed to launch task.", e);
                scheduleAttemptsFailed.incrementAndGet();

                // The attempt to schedule the task failed, so we need to backpedal on the
                // assignment.
                stateManager.changeState(selfQuery, LOST, NOT_REGISTERED_MSG);

                // Break out completely to avoid retrying this task.  It is in the LOST state
                // and a new task will move to PENDING to replace it.
                // Should the state change fail due to storage issues, that's okay.  The task will
                // time out in the ASSIGNED state and be moved to LOST.
                return;
              }
              return;
            }
          }

          // Task could not be scheduled.
          retryLater();
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

    /**
     * An expiring offer, which will decline itself unless declining is explicitly disabled.
     */
    private class ExpiringOffer {
      final Offer offer;
      boolean shouldDecline = true;

      ExpiringOffer(Offer offer) {
        this.offer = offer;
      }

      synchronized void disableDecline() {
        shouldDecline = false;
      }

      synchronized void maybeDecline() {
        if (shouldDecline) {
          LOG.fine("Declining offer " + offer.getId());
          driver.declineOffer(offer.getId());
        } else {
          LOG.fine("Removing (not declining) offer " + offer.getId());
        }
      }
    }

    private final Function<Offer, ExpiringOffer> wrap =
        new Function<Offer, ExpiringOffer>() {
          @Override public ExpiringOffer apply(Offer offer) {
            return new ExpiringOffer(offer);
          }
        };

    private static final Function<ExpiringOffer, OfferID> GET_ID =
        new Function<ExpiringOffer, OfferID>() {
          @Override public OfferID apply(ExpiringOffer offer) {
            return offer.offer.getId();
          }
        };
  }
}
