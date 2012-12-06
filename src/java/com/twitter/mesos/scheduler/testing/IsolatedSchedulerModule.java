package com.twitter.mesos.scheduler.testing;

import java.util.logging.Logger;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.scheduler.DriverFactory;
import com.twitter.mesos.scheduler.storage.DistributedSnapshotStore;
import com.twitter.mesos.scheduler.storage.mem.MemStorageModule;

/**
 * A module that binds a fake mesos driver factory and a volatile storage system.
 */
public class IsolatedSchedulerModule extends AbstractModule {

  private static final Logger LOG = Logger.getLogger(IsolatedSchedulerModule.class.getName());

  @Override
  protected void configure() {
    MemStorageModule.bind(binder());

    bind(DriverFactory.class).to(FakeDriverFactory.class);
    bind(FakeDriverFactory.class).in(Singleton.class);
    bind(DistributedSnapshotStore.class).toInstance(new DistributedSnapshotStore() {
      @Override public void persist(Snapshot snapshot) {
        LOG.warning("Pretending to write snapshot.");
      }
    });
  }
}
