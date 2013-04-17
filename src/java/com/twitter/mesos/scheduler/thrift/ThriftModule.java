package com.twitter.mesos.scheduler.thrift;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

import com.twitter.common.thrift.ThriftServer;

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
  }
}
