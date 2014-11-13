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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Qualifier;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.binder.LinkedBindingBuilder;
import com.google.inject.multibindings.Multibinder;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.Positive;
import com.twitter.common.base.Command;
import com.twitter.common.stats.StatsProvider;

import org.apache.aurora.scheduler.events.NotifyingSchedulingFilter.NotifyDelegate;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.filter.SchedulingFilter;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

/**
 * Binding module for plumbing event notifications.
 */
public final class PubsubEventModule extends AbstractModule {

  private final boolean async;
  private final Logger log;

  @VisibleForTesting
  static final String PUBSUB_EXECUTOR_QUEUE_GAUGE = "pubsub_executor_queue_size";

  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  private @interface PubsubExecutorQueue { }

  @Positive
  @CmdLine(name = "max_async_event_bus_threads",
      help = "Maximum number of concurrent threads to allow for the async event processing bus.")
  private static final Arg<Integer> MAX_ASYNC_EVENT_BUS_THREADS = Arg.create(4);

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
      LinkedBlockingQueue<Runnable> executorQueue = new LinkedBlockingQueue<>();
      bind(new TypeLiteral<LinkedBlockingQueue<Runnable>>() { })
          .annotatedWith(PubsubExecutorQueue.class)
          .toInstance(executorQueue);

      executor = new ThreadPoolExecutor(
          MAX_ASYNC_EVENT_BUS_THREADS.get(),
          MAX_ASYNC_EVENT_BUS_THREADS.get(),
          0L,
          TimeUnit.MILLISECONDS,
          executorQueue,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("AsyncTaskEvents-%d")
              .build());

      LifecycleModule.bindStartupAction(binder(), RegisterGauges.class);
    } else {
      executor = MoreExecutors.sameThreadExecutor();
    }

    final EventBus eventBus = new AsyncEventBus("AsyncTaskEvents", executor);
    eventBus.register(new DeadEventHandler());
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

  private class DeadEventHandler {
    @Subscribe
    public void logDeadEvent(DeadEvent event) {
      log.warning(String.format(DEAD_EVENT_MESSAGE, event.getEvent()));
    }
  }

  static class RegisterGauges implements Command {
    private final StatsProvider statsProvider;
    private final LinkedBlockingQueue<Runnable> pubsubQueue;

    @Inject
    RegisterGauges(
        StatsProvider statsProvider,
        @PubsubExecutorQueue LinkedBlockingQueue<Runnable> pubsubQueue) {

      this.statsProvider = requireNonNull(statsProvider);
      this.pubsubQueue = requireNonNull(pubsubQueue);
    }

    @Override
    public void execute() throws RuntimeException {
      statsProvider.makeGauge(
          PUBSUB_EXECUTOR_QUEUE_GAUGE,
          new Supplier<Integer>() {
            @Override
            public Integer get() {
              return pubsubQueue.size();
            }
          });
    }
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
