package com.twitter.mesos.scheduler;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import com.twitter.common.application.Lifecycle;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.SingletonService;
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

  @CmdLine(name = "task_reaper_start_delay", help =
      "Time to wait after startup before running the task reaper.")
  private static final Arg<Amount<Long, Time>> TASK_REAPER_START_DELAY =
      Arg.create(Amount.of(5L, Time.MINUTES));

  @CmdLine(name = "task_reaper_interval", help = "Time to wait between task reaper runs.")
  private static final Arg<Amount<Long, Time>> TASK_REAPER_INTERVAL =
      Arg.create(Amount.of(5L, Time.MINUTES));

  private static final Logger LOG = Logger.getLogger(SchedulerLifecycle.class.getName());

  private final Function<String, SchedulerDriver> driverFactory;
  private final SchedulerCore scheduler;
  private final TaskReaper taskReaper;
  private final Lifecycle lifecycle;
  private final ShutdownRegistry shutdownRegistry;

  @Inject
  SchedulerLifecycle(Function<String, SchedulerDriver> driverFactory, SchedulerCore scheduler,
                     TaskReaper taskReaper, Lifecycle lifecycle,
                     ShutdownRegistry shutdownRegistry) {
    this.driverFactory = Preconditions.checkNotNull(driverFactory);
    this.scheduler = Preconditions.checkNotNull(scheduler);
    this.taskReaper = Preconditions.checkNotNull(taskReaper);
    this.lifecycle = Preconditions.checkNotNull(lifecycle);
    this.shutdownRegistry = Preconditions.checkNotNull(shutdownRegistry);
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

  class SchedulerCandidateImpl implements SchedulerCandidate {
    @Nullable private Driver driver;

    @Override
    public void onLeading(ServerSet.EndpointStatus status) {
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
      driver = new Driver(new Supplier<SchedulerDriver>() {
        @Override public SchedulerDriver get() {
          return driverFactory.apply(frameworkId);
        }
      });

      scheduler.start(new Closure<String>() {
        @Override public void execute(String taskId) throws RuntimeException {
          // TODO(John Sirois): Is there any way to latch on run (it blocks forever)?.  As it stands
          // the local view says this closure could be executed before the driver is running.
          // Understand the lifecycle and document why this is ok or else fix a hole we seem to not
          // get bitten by in practice.
          Protos.Status status = driver.killTask(asProto(taskId));
          if (status != Protos.Status.OK) {
            LOG.severe(String.format("Attempt to kill task %s failed with code %s",
                taskId, status));
          }
        }
      });

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

      Command shutdownReaper =
          taskReaper.start(TASK_REAPER_START_DELAY.get(), TASK_REAPER_INTERVAL.get());
      shutdownRegistry.addAction(shutdownReaper);
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
