package com.twitter.mesos.scheduler.events;

import java.util.Set;

import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;

import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Command;
import com.twitter.mesos.scheduler.SchedulingFilter;
import com.twitter.mesos.scheduler.events.NotifyingSchedulingFilter.NotifyDelegate;
import com.twitter.mesos.scheduler.events.TaskPubsubEvent.EventSubscriber;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Binding module for plumbing event notifications.
 */
public final class TaskEventModule extends AbstractModule {

  private TaskEventModule() {
    // Must be constructed through factory.
  }

  @Override
  protected void configure() {
    bind(EventBus.class).toInstance(new EventBus("TaskEvents"));

    // Ensure at least an empty binding is present.
    getSubscriberBinder(binder());
    LifecycleModule.bindStartupAction(binder(), RegisterSubscribers.class);
  }

  static class RegisterSubscribers implements Command {
    private final EventBus eventBus;
    private final Set<EventSubscriber> subscribers;

    @Inject
    RegisterSubscribers(EventBus eventBus, Set<EventSubscriber> subscribers) {
      this.eventBus = checkNotNull(eventBus);
      this.subscribers = checkNotNull(subscribers);
    }

    @Override
    public void execute() {
      for (EventSubscriber subscriber : subscribers) {
        eventBus.register(subscriber);
      }
    }
  }

  /**
   * Binds a task event module.
   *
   * @param binder Binder to bind against.
   * @param filterClass Delegate scheduling filter implementation class.
   */
  public static void bind(Binder binder, final Class<? extends SchedulingFilter> filterClass) {
    binder.bind(SchedulingFilter.class).annotatedWith(NotifyDelegate.class).to(filterClass);
    binder.bind(SchedulingFilter.class).to(NotifyingSchedulingFilter.class);
    binder.bind(NotifyingSchedulingFilter.class).in(Singleton.class);
    binder.install(new TaskEventModule());
  }

  private static Multibinder<EventSubscriber> getSubscriberBinder(Binder binder) {
    return Multibinder.newSetBinder(binder, EventSubscriber.class);
  }

  /**
   * Binds a subscriber to receive task events.
   *
   * @param binder Binder to bind the subscriber with.
   * @param subscriber Subscriber implementation class to register for events.
   */
  public static void bindSubscriber(Binder binder, Class<? extends EventSubscriber> subscriber) {
    getSubscriberBinder(binder).addBinding().to(subscriber);
  }

  @Provides
  @Singleton
  Closure<TaskPubsubEvent> provideEventSink(final EventBus eventBus) {
    return new Closure<TaskPubsubEvent>() {
      @Override public void execute(TaskPubsubEvent event) {
        eventBus.post(event);
      }
    };
  }
}
