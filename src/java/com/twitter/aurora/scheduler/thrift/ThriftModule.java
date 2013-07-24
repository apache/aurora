package com.twitter.aurora.scheduler.thrift;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

import com.twitter.aurora.gen.MesosAdmin;
import com.twitter.aurora.scheduler.thrift.aop.AopModule;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.thrift.ThriftServer;

/**
 * Binding module to configure a thrift server.
 */
public class ThriftModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(MesosAdmin.Iface.class).to(SchedulerThriftInterface.class);
    bind(ThriftServer.class).to(SchedulerThriftServer.class).in(Singleton.class);
    LifecycleModule.bindServiceRunner(binder(), ThriftServerLauncher.class);

    install(new AopModule());
  }
}
