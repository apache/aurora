package com.twitter.mesos.scheduler.thrift;

import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.Singleton;

import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.thrift.ThriftServer;
import com.twitter.mesos.scheduler.thrift.FeatureToggleSchedulerController.Delegate;
import com.twitter.mesos.scheduler.thrift.FeatureToggleSchedulerController.EnableUpdates;
import com.twitter.mesos.scheduler.thrift.auth.AuthModule;

/**
 * Binding module to configure a thrift server.
 */
public class ThriftModule extends AbstractModule {

  @CmdLine(name = "enable_job_updates", help = "Whether new job updates should be accepted.")
  private static final Arg<Boolean> ENABLE_UPDATES = Arg.create(true);

  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override protected void configure() {
        bind(SchedulerController.class).annotatedWith(Delegate.class)
            .to(SchedulerThriftInterface.class);
        bind(SchedulerThriftInterface.class).in(Singleton.class);
        bind(Boolean.class).annotatedWith(EnableUpdates.class).toInstance(ENABLE_UPDATES.get());
        bind(SchedulerController.class).to(FeatureToggleSchedulerController.class);
        bind(FeatureToggleSchedulerController.class).in(Singleton.class);
        expose(SchedulerController.class);
      }
    });

    // TODO(ksweeney): Refactor LoggingThriftInterface to LoggingSchedulerController
    LoggingThriftInterface.bind(binder(), SchedulerThriftRouter.class);
    bind(SchedulerThriftRouter.class).in(Singleton.class);
    bind(ThriftServer.class).to(SchedulerThriftServer.class).in(Singleton.class);
    LifecycleModule.bindServiceRunner(binder(), ThriftServerLauncher.class);
    install(AuthModule.userCapabilityModule());
    install(new AuthModule());
  }
}
