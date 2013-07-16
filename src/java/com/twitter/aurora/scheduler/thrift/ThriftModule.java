package com.twitter.aurora.scheduler.thrift;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import com.twitter.aurora.scheduler.thrift.FeatureToggleSchedulerController.Delegate;
import com.twitter.aurora.scheduler.thrift.FeatureToggleSchedulerController.EnableUpdates;
import com.twitter.aurora.scheduler.thrift.auth.CapabilityValidator.Capability;
import com.twitter.aurora.scheduler.thrift.auth.ThriftAuthModule;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotEmpty;
import com.twitter.common.thrift.ThriftServer;

/**
 * Binding module to configure a thrift server.
 */
public class ThriftModule extends AbstractModule {

  @CmdLine(name = "enable_job_updates", help = "Whether new job updates should be accepted.")
  private static final Arg<Boolean> ENABLE_UPDATES = Arg.create(true);

  private static final Map<Capability, String> DEFAULT_CAPABILITIES =
      ImmutableMap.of(Capability.ROOT, "mesos");

  @NotEmpty
  @CmdLine(name = "user_capabilities",
      help = "Concrete name mappings for administration capabilities.")
  private static final Arg<Map<Capability, String>> USER_CAPABILITIES =
      Arg.create(DEFAULT_CAPABILITIES);

  private Map<Capability, String> capabilities;

  public ThriftModule() {
    this(USER_CAPABILITIES.get());
  }

  @VisibleForTesting
  ThriftModule(Map<Capability, String> capabilities) {
    this.capabilities = Preconditions.checkNotNull(capabilities);
  }

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

    Preconditions.checkArgument(
        capabilities.containsKey(Capability.ROOT),
        "A ROOT capability must be provided with --user_capabilities");
    bind(new TypeLiteral<Map<Capability, String>>() { }).toInstance(capabilities);

    // TODO(ksweeney): Refactor LoggingThriftInterface to LoggingSchedulerController
    LoggingThriftInterface.bind(binder(), SchedulerThriftRouter.class);
    bind(SchedulerThriftRouter.class).in(Singleton.class);
    bind(ThriftServer.class).to(SchedulerThriftServer.class).in(Singleton.class);
    LifecycleModule.bindServiceRunner(binder(), ThriftServerLauncher.class);
    install(ThriftAuthModule.userCapabilityModule());
    install(new ThriftAuthModule());
  }
}
