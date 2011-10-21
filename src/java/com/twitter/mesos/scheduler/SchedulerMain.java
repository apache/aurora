package com.twitter.mesos.scheduler;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.SchedulerDriver;

import com.twitter.common.application.AbstractApplication;
import com.twitter.common.application.Lifecycle;
import com.twitter.common.application.LocalServiceRegistry;
import com.twitter.common.application.modules.HttpModule;
import com.twitter.common.application.modules.LogModule;
import com.twitter.common.application.modules.StatsExportModule;
import com.twitter.common.application.modules.StatsModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.ServerSet.EndpointStatus;
import com.twitter.common.zookeeper.ServerSet.UpdateException;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.SingletonService.LeadershipListener;
import com.twitter.common_internal.webassets.Blueprint;
import com.twitter.thrift.Status;

/**
 * Launcher for the twitter mesos scheduler.
 *
 * TODO(William Farner): Include information in /schedulerz about who is the current scheduler
 * leader.
 *
 * @author William Farner
 */
public class SchedulerMain extends AbstractApplication {

  private static final Logger LOG = Logger.getLogger(SchedulerMain.class.getName());

  @CmdLine(name = "task_reaper_start_delay", help =
      "Time to wait after startup before running the task reaper.")
  private static final Arg<Amount<Long, Time>> TASK_REAPER_START_DELAY =
      Arg.create(Amount.of(2L, Time.MINUTES));

  @CmdLine(name = "task_reaper_interval", help = "Time to wait between task reaper runs.")
  private static final Arg<Amount<Long, Time>> TASK_REAPER_INTERVAL =
      Arg.create(Amount.of(2L, Time.MINUTES));

  @Inject private SingletonService schedulerService;
  @Inject private Provider<SchedulerDriver> driverProvider;
  @Inject private SchedulerCore scheduler;
  @Inject private TaskReaper taskReaper;
  @Inject private Lifecycle lifecycle;
  @Inject private LocalServiceRegistry serviceRegistry;

  private final LeadershipListener leadershipListener = new LeadershipListener() {
    @Override public void onLeading(EndpointStatus status) {
      LOG.info("Elected as leading scheduler!");
      try {
        runMesosDriver();
        status.update(Status.ALIVE);
      } catch (UpdateException e) {
        LOG.log(Level.SEVERE, "Failed to update endpoint status.", e);
      }
      taskReaper.start(TASK_REAPER_START_DELAY.get(), TASK_REAPER_INTERVAL.get());
    }

    @Override public void onDefeated(@Nullable EndpointStatus status) {
      LOG.info("Lost leadership, committing suicide.");

      try {
        if (status != null) {
          status.update(Status.DEAD);
        }
      } catch (UpdateException e) {
        LOG.log(Level.WARNING, "Failed to leave server set.", e);
      } finally {

        // TODO(John Sirois): add a call to driver.failover() or driver.restarting() when this
        // becomes available.  The existing driver.stop() deregisters our framework and kills all
        // our executors causing more ripple than is needed.
        scheduler.stop();

        lifecycle.shutdown();
        // TODO(William Farner): This seems necessary to break out of the blocking driver run.
        System.exit(1);
      }
    }
  };

  @Override
  public Iterable<? extends Module> getModules() {
    return Arrays.asList(
        new StatsExportModule(),
        new HttpModule(),
        new LogModule(),
        new SchedulerModule(),
        new StatsModule(),
        new Blueprint.HttpAssetModule()
    );
  }

  @Override
  public void run() {
    try {
      schedulerService.lead(serviceRegistry.getPrimarySocket(),
          serviceRegistry.getAuxiliarySockets(), Status.STARTING, leadershipListener);
    } catch (Group.WatchException e) {
      LOG.log(Level.SEVERE, "Failed to watch group and lead service.", e);
    } catch (Group.JoinException e) {
      LOG.log(Level.SEVERE, "Failed to join scheduler service group.", e);
    } catch (InterruptedException e) {
      LOG.log(Level.SEVERE, "Interrupted while joining scheduler service group.", e);
    }

    lifecycle.awaitShutdown();
  }

  private void runMesosDriver() {
    // Initialize the driver just in time
    // TODO(John Sirois): control this lifecycle in a more explicit - non Guice specific way
    final SchedulerDriver driver = driverProvider.get();

    scheduler.start(new Closure<String>() {
      @Override public void execute(String taskId) throws RuntimeException {
        Protos.Status status = driver.killTask(TaskID.newBuilder().setValue(taskId).build());
        if (status != Protos.Status.OK) {
          LOG.severe(String.format("Attempt to kill task %s failed with code %s",
              taskId, status));
        }
      }
    });

    new ThreadFactoryBuilder().setNameFormat("Driver-Runner-%d").setDaemon(true).build().newThread(
        new Runnable() {
          @Override public void run() {
            Protos.Status status = driver.run();
            LOG.info("Driver completed with exit code " + status);
            lifecycle.shutdown();
          }
        }
    ).start();
  }
}
