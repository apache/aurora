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
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.binder.LinkedBindingBuilder;
import com.google.inject.multibindings.Multibinder;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.base.Command;

import org.apache.aurora.scheduler.events.NotifyingSchedulingFilter.NotifyDelegate;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.filter.SchedulingFilter;

import static java.util.Objects.requireNonNull;

/**
 * Binding module for plumbing event notifications.
 */
public final class PubsubEventModule extends AbstractModule {

  private final boolean async;
  private final Logger log;

  @VisibleForTesting
  PubsubEventModule(boolean async, Logger log) {
    this.log = requireNonNull(log);
    this.async = requireNonNull(async);
  }

  public PubsubEventModule(boolean async) {
    this(async, Logger.getLogger(PubsubEventModule.class.getName()));
  }

  @VisibleForTesting
  static final String DEAD_EVENT_MESSAGE = "Captured dead event %s";

  @Override
  protected void configure() {
    final Executor executor;
    if (async) {
      executor = Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("AsyncTaskEvents-%d")
              .build());
    } else {
      executor = MoreExecutors.sameThreadExecutor();
    }

    final EventBus eventBus = new AsyncEventBus("AsyncTaskEvents", executor);
    eventBus.register(new Object() {
      @Subscribe public void logDeadEvent(DeadEvent event) {
        log.warning(String.format(DEAD_EVENT_MESSAGE, event.getEvent()));
      }
    });
    bind(EventBus.class).toInstance(eventBus);

    EventSink eventSink = new EventSink() {
      @Override
      public void post(PubsubEvent event) {
        eventBus.post(event);
      }
    };
    bind(EventSink.class).toInstance(eventSink);

    // Ensure at least an empty binding is present.
    getSubscriberBinder(binder());
    LifecycleModule.bindStartupAction(binder(), RegisterSubscribers.class);
  }

  static class RegisterSubscribers implements Command {
    private final EventBus eventBus;
    private final Set<EventSubscriber> subscribers;

    @Inject
    RegisterSubscribers(EventBus eventBus, Set<EventSubscriber> subscribers) {
      this.eventBus = requireNonNull(eventBus);
      this.subscribers = requireNonNull(subscribers);
    }

    @Override
    public void execute() {
      for (EventSubscriber subscriber : subscribers) {
        eventBus.register(subscriber);
      }
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
    binder.install(new PubsubEventModule(true));
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
