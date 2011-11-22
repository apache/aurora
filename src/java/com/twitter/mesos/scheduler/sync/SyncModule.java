package com.twitter.mesos.scheduler.sync;

import java.util.Set;

import javax.inject.Singleton;

import com.google.common.base.Supplier;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

import com.twitter.mesos.ExecutorKey;
import com.twitter.mesos.scheduler.sync.ExecutorWatchdog.ExecutorWatchdogImpl;

/**
 * Binding module for the sync package.
 *
 * @author William Farner
 */
public class SyncModule extends AbstractModule {

  /**
   * Binds the sync module.
   *
   * @param binder a guice binder to bind with the sync module.
   */
  public static void bind(Binder binder) {
    binder.install(new SyncModule());
  }

  @Override
  protected void configure() {
    requireBinding(Key.get(new TypeLiteral<Supplier<Set<ExecutorKey>>>() { }));

    bind(ExecutorWatchdog.class).to(ExecutorWatchdogImpl.class);
    bind(ExecutorWatchdogImpl.class).in(Singleton.class);
  }
}
