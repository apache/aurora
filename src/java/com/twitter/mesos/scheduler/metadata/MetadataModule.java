package com.twitter.mesos.scheduler.metadata;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

import com.twitter.mesos.scheduler.events.TaskEventModule;

/**
 * Binding module for scheduler metadata management.
 */
public class MetadataModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(NearestFit.class).in(Singleton.class);
    TaskEventModule.bindSubscriber(binder(), NearestFit.class);
  }
}
