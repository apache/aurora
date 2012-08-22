package com.twitter.mesos.scheduler.events;

import com.google.common.eventbus.EventBus;
import com.google.inject.Binder;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import com.twitter.common.base.Closure;
import com.twitter.common.inject.ProviderMethodModule;
import com.twitter.mesos.scheduler.SchedulingFilter;
import com.twitter.mesos.scheduler.events.NotifyingSchedulingFilter.Delegate;
import com.twitter.mesos.scheduler.events.TaskPubsubEvent.EventSubscriber;

/**
 * Binding module for plumbing event notifications.
 */
public final class TaskEventModule extends ProviderMethodModule {

  private final EventBus taskEventBus = new EventBus("TaskEvents");

  private TaskEventModule() {
    // Must be constructed through factory.
  }

  /**
   * Binds a task event module.
   *
   * @param binder Binder to bind against.
   * @param filterClass Delegate scheduling filter implementation class.
   */
  public static void bind(Binder binder, final Class<? extends SchedulingFilter> filterClass) {
    binder.install(new PrivateModule() {
      @Override protected void configure() {
        bind(SchedulingFilter.class).annotatedWith(Delegate.class).to(filterClass);
        bind(SchedulingFilter.class).to(NotifyingSchedulingFilter.class);
        bind(NotifyingSchedulingFilter.class).in(Singleton.class);
        expose(SchedulingFilter.class);
      }
    });
    binder.install(new TaskEventModule());
  }

  @Provides
  @Singleton
  Closure<TaskPubsubEvent> provideEventSink() {
    return new Closure<TaskPubsubEvent>() {
      @Override public void execute(TaskPubsubEvent event) {
        taskEventBus.post(event);
      }
    };
  }

  @Provides
  @Singleton
  Closure<EventSubscriber> provideSubecriberSink() {
    return new Closure<EventSubscriber>() {
      @Override public void execute(EventSubscriber subscriber) {
        taskEventBus.register(subscriber);
      }
    };
  }
}
