package com.twitter.mesos.scheduler.metadata;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Command;
import com.twitter.mesos.scheduler.events.TaskPubsubEvent.EventSubscriber;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Binding module for scheduler metadata management.
 */
public class MetadataModule extends AbstractModule {

  @Override
  protected void configure() {
    requireBinding(Key.get(new TypeLiteral<Closure<EventSubscriber>>() { }));

    bind(NearestFit.class).in(Singleton.class);
    LifecycleModule.bindStartupAction(binder(), SubscribeTaskEvents.class);
  }

  static class SubscribeTaskEvents implements Command {
    private final NearestFit nearestFit;
    private final Closure<EventSubscriber> subscribeSink;

    @Inject
    SubscribeTaskEvents(NearestFit nearestFit, Closure<EventSubscriber> subscribeSink) {
      this.nearestFit = checkNotNull(nearestFit);
      this.subscribeSink = checkNotNull(subscribeSink);
    }

    @Override
    public void execute() {
      subscribeSink.execute(nearestFit);
    }
  }
}
