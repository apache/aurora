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

import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.binder.LinkedBindingBuilder;
import com.google.inject.multibindings.Multibinder;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.async.AsyncModule.AsyncExecutor;
import org.apache.aurora.scheduler.events.NotifyingSchedulingFilter.NotifyDelegate;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.filter.SchedulingFilter;

import static java.util.Objects.requireNonNull;

/**
 * Binding module for plumbing event notifications.
 */
public final class PubsubEventModule extends AbstractModule {

  private final Logger log;

  @VisibleForTesting
  static final String EXCEPTIONS_STAT = "event_bus_exceptions";

  @VisibleForTesting
  PubsubEventModule(Logger log) {
    this.log = requireNonNull(log);
  }

  public PubsubEventModule() {
    this(Logger.getLogger(PubsubEventModule.class.getName()));
  }

  @VisibleForTesting
  static final String DEAD_EVENT_MESSAGE = "Captured dead event %s";

  @Override
  protected void configure() {
    // Ensure at least an empty binding is present.
    getSubscriberBinder(binder());
    // TODO(ksweeney): Would this be better as a scheduler active service?
    SchedulerServicesModule.addAppStartupServiceBinding(binder()).to(RegisterSubscribers.class);
  }

  @Provides
  @Singleton
  EventBus provideEventBus(@AsyncExecutor Executor executor, StatsProvider statsProvider) {
    final AtomicLong subscriberExceptions = statsProvider.makeCounter(EXCEPTIONS_STAT);
    EventBus eventBus = new AsyncEventBus(
        executor,
        (exception, context) -> {
          subscriberExceptions.incrementAndGet();
          log.log(
              Level.SEVERE,
              "Failed to dispatch event to " + context.getSubscriberMethod() + ": " + exception,
              exception);
        }
    );

    eventBus.register(new DeadEventHandler());
    return eventBus;
  }

  @Provides
  @Singleton
  EventSink provideEventSink(EventBus eventBus) {
    return eventBus::post;
  }

  private class DeadEventHandler {
    @Subscribe
    public void logDeadEvent(DeadEvent event) {
      log.warning(String.format(DEAD_EVENT_MESSAGE, event.getEvent()));
    }
  }

  static class RegisterSubscribers extends AbstractIdleService {
    private final EventBus eventBus;
    private final Set<EventSubscriber> subscribers;

    @Inject
    RegisterSubscribers(EventBus eventBus, Set<EventSubscriber> subscribers) {
      this.eventBus = requireNonNull(eventBus);
      this.subscribers = requireNonNull(subscribers);
    }

    @Override
    protected void startUp() {
      for (EventSubscriber subscriber : subscribers) {
        eventBus.register(subscriber);
      }
    }

    @Override
    protected void shutDown() {
      // Nothing to do - await VM shutdown.
    }
  }

  /**
   * Gets a binding builder that must be used to wire up the scheduling filter implementation
   * that backs the delegating scheduling filter that fires pubsub events.
   *
   * @param binder Binder to create a binding against.
   * @return A linked binding builder that may be used to wire up the scheduling filter.
   */
  public static LinkedBindingBuilder<SchedulingFilter> bindSchedulingFilterDelegate(Binder binder) {
    binder.bind(SchedulingFilter.class).to(NotifyingSchedulingFilter.class);
    binder.bind(NotifyingSchedulingFilter.class).in(Singleton.class);
    return binder.bind(SchedulingFilter.class).annotatedWith(NotifyDelegate.class);
  }

  /**
   * Binds a task event module.
   *
   * @param binder Binder to bind against.
   */
  public static void bind(Binder binder) {
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
}
