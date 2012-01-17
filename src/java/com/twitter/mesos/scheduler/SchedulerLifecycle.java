package com.twitter.mesos.scheduler;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import com.twitter.common.application.Lifecycle;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.mesos.scheduler.Driver.DriverImpl;
import com.twitter.thrift.Status;

/**
 * The central driver of the scheduler runtime lifecycle.  Handles the transitions from startup and
 * initialization through acting as a standby scheduler / log replica and finally to becoming the
 * scheduler leader.
 *
 * <p>TODO(John Sirois): This class contains the old logic of SchedulerMain - now that its extracted
 * it should be tested.
 *
 * @author John Sirois
 */
class SchedulerLifecycle {

  /**
   * A {@link SingletonService} scheduler leader candidate that exposes a method for awaiting clean
   * shutdown.
   */
  interface SchedulerCandidate extends SingletonService.LeadershipListener {
    /**
     * Waits for this candidate to abdicate or otherwise decide to quit.
     */
    void awaitShutdown();
  }

  private static final Logger LOG = Logger.getLogger(SchedulerLifecycle.class.getName());

  private final Function<String, SchedulerDriver> driverFactory;
  private final SchedulerCore scheduler;
  private final Lifecycle lifecycle;

  private final Driver driver;
  private final DriverReference driverRef;

  @Inject
  SchedulerLifecycle(Function<String, SchedulerDriver> driverFactory,
      SchedulerCore scheduler,
      Lifecycle lifecycle,
      Driver driver,
      DriverReference driverRef) {
    this.driverFactory = Preconditions.checkNotNull(driverFactory);
    this.scheduler = Preconditions.checkNotNull(scheduler);
    this.lifecycle = Preconditions.checkNotNull(lifecycle);
    this.driver  = Preconditions.checkNotNull(driver);
    this.driverRef = Preconditions.checkNotNull(driverRef);
  }

  /**
   * Prepares a scheduler to offer itself as a leader candidate.  After this call the scheduler will
   * host a live log replica and start syncing data from the leader via the log until it gets called
   * upon to lead.
   *
   * @return A candidate that can be offered for leadership of a distributed election.
   */
  public SchedulerCandidate prepare() {
    LOG.info("Preparing scheduler candidate");
    scheduler.prepare();
    return new SchedulerCandidateImpl();
  }

  static class DriverReference implements Supplier<Optional<SchedulerDriver>> {
    private volatile Optional<SchedulerDriver> driver = Optional.absent();

    @Override
    public Optional<SchedulerDriver> get() {
      return driver;
    }

    private void set(SchedulerDriver driver) {
      this.driver = Optional.of(driver);
    }
  }

  private class SchedulerCandidateImpl implements SchedulerCandidate {

    @Override public void onLeading(ServerSet.EndpointStatus status) {
      LOG.info("Elected as leading scheduler!");
      try {
        lead();
        status.update(Status.ALIVE);
      } catch (ServerSet.UpdateException e) {
        LOG.log(Level.SEVERE, "Failed to update endpoint status, shutting down.", e);
        lifecycle.shutdown();
      } catch (RuntimeException e) {
        LOG.log(Level.SEVERE, "Unexpected exception attempting to lead, shutting down.", e);
        lifecycle.shutdown();
      }
    }

    private void lead() {
      @Nullable final String frameworkId = scheduler.initialize();

      driverRef.set(driverFactory.apply(frameworkId));

      scheduler.start();

      new ThreadFactoryBuilder()
          .setNameFormat("Driver-Runner-%d")
          .setDaemon(true)
          .build()
          .newThread(new Runnable() {
            @Override public void run() {
              Protos.Status status = driver.run();
              LOG.info("Driver completed with exit code " + status);
              lifecycle.shutdown();
            }
          }).start();
    }

    private Protos.TaskID asProto(String taskId) {
      return Protos.TaskID.newBuilder().setValue(taskId).build();
    }

    @Override
    public void onDefeated(@Nullable ServerSet.EndpointStatus status) {
      LOG.info("Lost leadership, committing suicide.");

      try {
        if (status != null) {
          status.update(Status.DEAD);
        }
      } catch (ServerSet.UpdateException e) {
        LOG.log(Level.WARNING, "Failed to leave server set.", e);
      } finally {
        if (driver != null) {
          driver.stop(); // shut down incoming offers
          scheduler.stop(); // shut down offer and slave update processing
        }
        lifecycle.shutdown(); // shut down the server
      }
    }

    @Override
    public void awaitShutdown() {
      lifecycle.awaitShutdown();
    }
  }
}
