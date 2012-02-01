package com.twitter.mesos.scheduler;

import java.util.Arrays;

import com.google.inject.Inject;
import com.google.inject.Module;

import com.twitter.common.application.AbstractApplication;
import com.twitter.common.application.AppLauncher;
import com.twitter.common.application.LocalServiceRegistry;
import com.twitter.common.application.modules.HttpModule;
import com.twitter.common.application.modules.LogModule;
import com.twitter.common.application.modules.StatsModule;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common_internal.webassets.BlueprintModule;
import com.twitter.thrift.Status;

/**
 * Launcher for the twitter mesos scheduler.
 *
 * @author William Farner
 */
public class SchedulerMain extends AbstractApplication {

  @Inject private SingletonService schedulerService;
  @Inject private LocalServiceRegistry serviceRegistry;
  @Inject private SchedulerLifecycle schedulerLifecycle;

  @Override
  public Iterable<? extends Module> getModules() {
    return Arrays.asList(
        new HttpModule(),
        new LogModule(),
        new SchedulerModule(),
        new StatsModule(),
        new BlueprintModule()
    );
  }

  @Override
  public void run() {
    SchedulerLifecycle.SchedulerCandidate candidate = schedulerLifecycle.prepare();

    try {
      schedulerService.lead(serviceRegistry.getPrimarySocket(),
          serviceRegistry.getAuxiliarySockets(), Status.STARTING, candidate);
    } catch (Group.WatchException e) {
      throw new IllegalStateException("Failed to watch group and lead service.", e);
    } catch (Group.JoinException e) {
      throw new IllegalStateException("Failed to join scheduler service group.", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted while joining scheduler service group.", e);
    }

    candidate.awaitShutdown();
  }

  public static void main(String[] args) {
    AppLauncher.launch(SchedulerMain.class, args);
  }
}
