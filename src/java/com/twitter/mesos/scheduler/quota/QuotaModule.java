package com.twitter.mesos.scheduler.quota;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;

import com.twitter.mesos.scheduler.quota.QuotaManager.QuotaManagerImpl;
import com.twitter.mesos.scheduler.storage.Storage;

/**
 * @author William Farner
 */
public class QuotaModule extends AbstractModule {

  /**
   * Binds the quota module.
   *
   * @param binder a guice binder to bind primary storage with
   */
  public static void bind(Binder binder) {
    binder.install(new QuotaModule());
  }

  @Override
  protected void configure() {
    requireBinding(Storage.class);

    bind(QuotaManager.class).to(QuotaManagerImpl.class);
  }
}
