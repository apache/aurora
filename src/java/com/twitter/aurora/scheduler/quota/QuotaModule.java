package com.twitter.aurora.scheduler.quota;

import com.google.inject.AbstractModule;

import com.twitter.aurora.scheduler.quota.QuotaManager.QuotaManagerImpl;
import com.twitter.aurora.scheduler.storage.Storage;

/**
 * Guice module for the quota package.
 */
public class QuotaModule extends AbstractModule {

  @Override
  protected void configure() {
    requireBinding(Storage.class);
    bind(QuotaManager.class).to(QuotaManagerImpl.class);
  }
}
