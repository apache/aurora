package com.twitter.aurora.scheduler.metadata;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

import com.twitter.aurora.scheduler.events.PubsubEventModule;

/**
 * Binding module for scheduler metadata management.
 */
public class MetadataModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(NearestFit.class).in(Singleton.class);
    PubsubEventModule.bindSubscriber(binder(), NearestFit.class);
  }
}
