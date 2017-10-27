/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.events;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;
import javax.inject.Qualifier;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.eventbus.SubscriberExceptionHandler;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.async.AsyncModule.AsyncExecutor;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

/**
 * Binding module for plumbing event notifications.
 */
public final class PubsubEventModule extends AbstractModule {

  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  private @interface DeadEventHandler { }

  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface RegisteredEvents { }

  private final Logger log;
  private final Executor registeredExecutor;

  @VisibleForTesting
  static final String EXCEPTIONS_STAT = "event_bus_exceptions";
  @VisibleForTesting
  static final String EVENT_BUS_DEAD_EVENTS = "event_bus_dead_events";

  public PubsubEventModule() {
    this(LoggerFactory.getLogger(PubsubEventModule.class));
  }

  private PubsubEventModule(Logger log) {
    this.log = requireNonNull(log);
    this.registeredExecutor = AsyncUtil.singleThreadLoggingScheduledExecutor("RegisteredEventSink",
        log);
  }

  @VisibleForTesting
  PubsubEventModule(Logger log, Executor registeredExecutor) {
    this.log = requireNonNull(log);
    this.registeredExecutor = requireNonNull(registeredExecutor);
  }

  @VisibleForTesting
  static final String DEAD_EVENT_MESSAGE = "Captured dead event %s";

  @Override
  protected void configure() {
    // Ensure at least an empty binding is present.
    Multibinder.newSetBinder(binder(), EventSubscriber.class);
    Multibinder.newSetBinder(binder(), EventSubscriber.class, RegisteredEvents.class);

    // TODO(ksweeney): Would this be better as a scheduler active service?
    SchedulerServicesModule.addAppStartupServiceBinding(binder()).to(RegisterSubscribers.class);
  }

  @Provides
  @Singleton
  SubscriberExceptionHandler provideSubscriberExceptionHandler(StatsProvider statsProvider) {
    final AtomicLong subscriberExceptions = statsProvider.makeCounter(EXCEPTIONS_STAT);
    return (exception, context) -> {
      subscriberExceptions.incrementAndGet();
      log.error(
          "Failed to dispatch event to " + context.getSubscriberMethod() + ": " + exception,
          exception);
    };
  }

  @Provides
  @DeadEventHandler
  @Singleton
  Object provideDeadEventHandler(StatsProvider statsProvider) {
    final AtomicLong deadEventCounter = statsProvider.makeCounter(EVENT_BUS_DEAD_EVENTS);
    return new Object() {
      @Subscribe
      public void logDeadEvent(DeadEvent event) {
        deadEventCounter.incrementAndGet();
        log.warn(String.format(DEAD_EVENT_MESSAGE, event.getEvent()));
      }
    };
  }

  @Provides
  @Singleton
  EventBus provideEventBus(@AsyncExecutor Executor executor,
                           SubscriberExceptionHandler subscriberExceptionHandler,
                           @DeadEventHandler Object deadEventHandler) {

    EventBus eventBus = new AsyncEventBus(executor, subscriberExceptionHandler);
    eventBus.register(deadEventHandler);
    return eventBus;
  }

  @Provides
  @Singleton
  EventSink provideEventSink(EventBus eventBus) {
    return eventBus::post;
  }

  @Provides
  @RegisteredEvents
  @Singleton
  EventBus provideRegisteredEventBus(SubscriberExceptionHandler subscriberExceptionHandler,
                                     @DeadEventHandler Object deadEventHandler) {

    EventBus eventBus = new AsyncEventBus(registeredExecutor, subscriberExceptionHandler);
    eventBus.register(deadEventHandler);
    return eventBus;
  }

  @Provides
  @RegisteredEvents
  @Singleton
  EventSink provideRegisteredEventSink(@RegisteredEvents EventBus eventBus) {
    return eventBus::post;
  }

  static class RegisterSubscribers extends AbstractIdleService {
    private final EventBus eventBus;
    private final EventBus registeredEventBus;
    private final Set<EventSubscriber> subscribers;
    private final Set<EventSubscriber> registeredSubscribers;

    @Inject
    RegisterSubscribers(EventBus eventBus,
                        @RegisteredEvents EventBus registeredEventBus,
                        Set<EventSubscriber> subscribers,
                        @RegisteredEvents Set<EventSubscriber> registeredSubscribers) {

      this.eventBus = requireNonNull(eventBus);
      this.registeredEventBus = requireNonNull(registeredEventBus);
      this.subscribers = requireNonNull(subscribers);
      this.registeredSubscribers = requireNonNull(registeredSubscribers);
    }

    @Override
    protected void startUp() {
      subscribers.forEach(eventBus::register);
      registeredSubscribers.forEach(registeredEventBus::register);
    }

    @Override
    protected void shutDown() {
      // Nothing to do - await VM shutdown.
    }
  }

  /**
   * Binds a task event module.
   *
   * @param binder Binder to bind against.
   */
  public static void bind(Binder binder) {
    binder.install(new PubsubEventModule());
  }

  /**
   * Binds a subscriber to receive task events.
   *
   * @param binder Binder to bind the subscriber with.
   * @param subscriber Subscriber implementation class to register for events.
   */
  public static void bindSubscriber(Binder binder, Class<? extends EventSubscriber> subscriber) {
    Multibinder.newSetBinder(binder, EventSubscriber.class).addBinding().to(subscriber);
  }

  /**
   * Binds a subscriber to receive Mesos registered events.
   *
   * @param binder Binder to bind the subscriber with.
   * @param subscriber Subscriber implementation class to register for events.
   */
  public static void bindRegisteredSubscriber(Binder binder,
                                              Class<? extends EventSubscriber> subscriber) {

    Multibinder.newSetBinder(binder, EventSubscriber.class, RegisteredEvents.class)
        .addBinding()
        .to(subscriber);
  }
}
