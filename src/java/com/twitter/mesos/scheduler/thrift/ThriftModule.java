package com.twitter.mesos.scheduler.thrift;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.thrift.ThriftServer;
import com.twitter.mesos.scheduler.thrift.auth.AuthModule;

/**
 * Binding module to configure a thrift server.
 */
public class ThriftModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(SchedulerController.class).to(SchedulerThriftInterface.class).in(Singleton.class);

    // TODO(ksweeney): Refactor LoggingThriftInterface to LoggingSchedulerController
    LoggingThriftInterface.bind(binder(), SchedulerThriftRouter.class);
    bind(SchedulerThriftRouter.class).in(Singleton.class);
    bind(ThriftServer.class).to(SchedulerThriftServer.class).in(Singleton.class);
    LifecycleModule.bindServiceRunner(binder(), ThriftServerLauncher.class);
    install(AuthModule.userCapabilityModule());
    install(new AuthModule());
  }
}
