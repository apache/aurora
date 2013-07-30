package com.twitter.aurora.scheduler.events;

import java.util.Set;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.multibindings.Multibinder;

import org.aopalliance.intercept.MethodInterceptor;

import com.twitter.aurora.scheduler.events.NotifyingSchedulingFilter.NotifyDelegate;
import com.twitter.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.aurora.scheduler.events.PubsubEvent.Interceptors.SendNotification;
import com.twitter.aurora.scheduler.filter.SchedulingFilter;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Command;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Binding module for plumbing event notifications.
 */
public final class PubsubEventModule extends AbstractModule {

  private static final Logger LOG = Logger.getLogger(PubsubEventModule.class.getName());

  @VisibleForTesting
  PubsubEventModule() {
    // Must be constructed through factory.
  }

  @Override
  protected void configure() {
    final EventBus eventBus = new EventBus("TaskEvents");
    eventBus.register(new Object() {
      @Subscribe public void logDeadEvent(DeadEvent event) {
        LOG.warning("Captured dead event " + event.getEvent());
      }
    });

    bind(EventBus.class).toInstance(eventBus);

    Closure<PubsubEvent> eventPoster = new Closure<PubsubEvent>() {
      @Override public void execute(PubsubEvent event) {
        eventBus.post(event);
      }
    };
    bind(new TypeLiteral<Closure<PubsubEvent>>() { }).toInstance(eventPoster);

    // Ensure at least an empty binding is present.
    getSubscriberBinder(binder());
    LifecycleModule.bindStartupAction(binder(), RegisterSubscribers.class);
    bindNotifyingInterceptor(binder());
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
    binder.install(new PubsubEventModule());
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

  /**
   * Binds a method interceptor to all methods annotated with {@link SendNotification}.
   * <p>
   * The interceptor will send notifications before and/or after the wrapped method invocation.
   *
   * @param binder Guice binder.
   */
  @VisibleForTesting
  public static void bindNotifyingInterceptor(Binder binder) {
    MethodInterceptor interceptor = new NotifyingMethodInterceptor();
    binder.requestInjection(interceptor);
    binder.bindInterceptor(
        Matchers.any(),
        Matchers.annotatedWith(SendNotification.class),
        interceptor);
  }
}
