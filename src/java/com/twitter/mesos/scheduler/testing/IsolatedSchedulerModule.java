package com.twitter.mesos.scheduler.testing;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

import com.twitter.mesos.scheduler.DriverFactory;
import com.twitter.mesos.scheduler.storage.mem.MemStorageModule;

/**
 * A module that binds a fake mesos driver factory and a volatile storage system.
 */
public class IsolatedSchedulerModule extends AbstractModule {
  @Override
  protected void configure() {
    MemStorageModule.bind(binder());

    bind(DriverFactory.class).to(FakeDriverFactory.class);
    bind(FakeDriverFactory.class).in(Singleton.class);
  }
}
