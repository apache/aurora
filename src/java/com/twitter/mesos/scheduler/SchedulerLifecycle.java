package com.twitter.mesos.scheduler;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import com.twitter.common.application.Lifecycle;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.SingletonService.LeaderControl;
import com.twitter.mesos.scheduler.events.PubsubEvent.DriverRegistered;
import com.twitter.mesos.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.Storage.NonVolatileStorage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.StorageBackfill;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The central driver of the scheduler runtime lifecycle.  Handles the transitions from startup and
 * initialization through acting as a standby scheduler / log replica and finally to becoming the
 * scheduler leader.
 *
 * <p>TODO(John Sirois): This class contains the old logic of SchedulerMain - now that its extracted
 * it should be tested.
 */
public class SchedulerLifecycle implements EventSubscriber {

  /**
   * A {@link SingletonService} scheduler leader candidate that exposes a method for awaiting clean
   * shutdown.
   */
  public interface SchedulerCandidate extends SingletonService.LeadershipListener {
    /**
     * Waits for this candidate to abdicate or otherwise decide to quit.
     */
    void awaitShutdown();
  }

  @CmdLine(name = "max_registration_delay",
      help = "Max allowable delay to allow the driver to register before aborting")
  private static final Arg<Amount<Long, Time>> MAX_REGISTRATION_DELAY =
      Arg.create(Amount.of(1L, Time.MINUTES));

  @CmdLine(name = "max_leading_duration",
      help = "After leading for this duration, the scheduler should commit suicide.")
  private static final Arg<Amount<Long, Time>> MAX_LEADING_DURATION =
      Arg.create(Amount.of(1L, Time.DAYS));

  /**
   * Binding annotation to attach to the flag indicating whether to initiate application shutdown
   * when the driver returns from {@link Driver#run()}.
   */
  @BindingAnnotation
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface ShutdownOnDriverExit { }

  private static final Logger LOG = Logger.getLogger(SchedulerLifecycle.class.getName());

  private final DriverFactory driverFactory;
  private final NonVolatileStorage storage;
  private final Lifecycle lifecycle;
  private final CountDownLatch registeredLatch = new CountDownLatch(1);
  private final AtomicInteger registeredFlag = Stats.exportInt("framework_registered");

  private final Driver driver;
  private final DriverReference driverRef;
  private final boolean shutdownAfterRunning;
  private final Clock clock;

  @Inject
  SchedulerLifecycle(
      DriverFactory driverFactory,
      NonVolatileStorage storage,
      Lifecycle lifecycle,
      Driver driver,
      DriverReference driverRef,
      @ShutdownOnDriverExit boolean shutdownAfterRunning,
      Clock clock) {

    this.driverFactory = checkNotNull(driverFactory);
    this.storage = checkNotNull(storage);
    this.lifecycle = checkNotNull(lifecycle);
    this.driver  = checkNotNull(driver);
    this.driverRef = checkNotNull(driverRef);
    this.shutdownAfterRunning = shutdownAfterRunning;
    this.clock = checkNotNull(clock);
  }

  /**
   * Prepares a scheduler to offer itself as a leader candidate.  After this call the scheduler will
   * host a live log replica and start syncing data from the leader via the log until it gets called
   * upon to lead.
   *
   * @return A candidate that can be offered for leadership of a distributed election.
   */
  public SchedulerCandidate prepare() {
    LOG.info("Preparing storage");
    storage.prepare();
    return new SchedulerCandidateImpl();
  }

  @Subscribe
  public void registered(DriverRegistered event) {
    registeredLatch.countDown();
    registeredFlag.set(1);
  }

  /**
   * Maintains a reference to the driver.
   */
  static class DriverReference implements Supplier<Optional<SchedulerDriver>> {
    private volatile Optional<SchedulerDriver> driver = Optional.absent();

    @Override public Optional<SchedulerDriver> get() {
      return driver;
    }

    private void set(SchedulerDriver ref) {
      this.driver = Optional.of(ref);
    }
  }

  /**
   * Implementation of the scheduler candidate lifecycle.
   */
  private class SchedulerCandidateImpl implements SchedulerCandidate {
    @Override public void onLeading(LeaderControl control) {
      LOG.info("Elected as leading scheduler!");
      try {
        lead();
        control.advertise();
      } catch (Group.JoinException e) {
        LOG.log(Level.SEVERE, "Failed to advertise leader, shutting down.", e);
        lifecycle.shutdown();
      } catch (InterruptedException e) {
        LOG.log(Level.SEVERE, "Failed to update endpoint status, shutting down.", e);
        lifecycle.shutdown();
        Thread.currentThread().interrupt();
      } catch (RuntimeException e) {
        LOG.log(Level.SEVERE, "Unexpected exception attempting to lead, shutting down.", e);
        lifecycle.shutdown();
      }
    }

    private void startDaemonThread(String name, Runnable work) {
      new ThreadFactoryBuilder()
          .setNameFormat(name + "-%d")
          .setDaemon(true)
          .build()
          .newThread(work)
          .start();
    }

    private void lead() {
      storage.start(new MutateWork.NoResult.Quiet() {
        @Override protected void execute(MutableStoreProvider storeProvider) {
          StorageBackfill.backfill(storeProvider, clock);
        }
      });
      @Nullable final String frameworkId = storage.consistentRead(new Work.Quiet<String>() {
        @Override public String apply(StoreProvider storeProvider) {
          return storeProvider.getSchedulerStore().fetchFrameworkId();
        }
      });

      driverRef.set(driverFactory.apply(frameworkId));

      startDaemonThread("Driver-Runner", new Runnable() {
        @Override public void run() {
          Protos.Status status = driver.run();
          LOG.info("Driver completed with exit code " + status);
          if (shutdownAfterRunning) {
            lifecycle.shutdown();
          }
        }
      });

      startDaemonThread("Driver-Register-Watchdog", new Runnable() {
        @Override public void run() {
          LOG.info(String.format("Waiting up to %s for scheduler registration.",
              MAX_REGISTRATION_DELAY.get()));

          try {
            boolean registered = registeredLatch.await(
                MAX_REGISTRATION_DELAY.get().getValue(),
                MAX_REGISTRATION_DELAY.get().getUnit().getTimeUnit());
            if (!registered) {
              LOG.severe("Framework has not been registered within the tolerated delay, quitting.");
              lifecycle.shutdown();
            }
          } catch (InterruptedException e) {
            LOG.log(Level.WARNING, "Delayed registration check interrupted.", e);
            Thread.currentThread().interrupt();
          }
        }
      });

      startDaemonThread("Leader-Assassin", new Runnable() {
        @Override public void run() {
          try {
            Thread.sleep(MAX_LEADING_DURATION.get().as(Time.MILLISECONDS));
            LOG.info(
                "Leader has been active for " + MAX_LEADING_DURATION.get() + ", forcing failover.");
            onDefeated(null);
          } catch (InterruptedException e) {
            LOG.warning("Leader assassin thread interrupted.");
            Thread.interrupted();
          }
        }
      });
    }

    @Override public void onDefeated(@Nullable ServerSet.EndpointStatus status) {
      LOG.info("Lost leadership, committing suicide.");

      try {
        if (status != null) {
          status.leave();
        }

        driver.stop(); // shut down incoming offers
        storage.stop();
      } catch (ServerSet.UpdateException e) {
        LOG.log(Level.WARNING, "Failed to leave server set.", e);
      } finally {
        lifecycle.shutdown(); // shut down the server
      }
    }

    @Override public void awaitShutdown() {
      lifecycle.awaitShutdown();
    }
  }
}
