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
package org.apache.aurora.scheduler.offers;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Set;
import javax.inject.Qualifier;
import javax.inject.Singleton;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.args.constraints.NotNegative;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.Random;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.app.MoreModules;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Binding module for resource offer management.
 */
public class OffersModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(OffersModule.class);

  @CmdLine(name = "min_offer_hold_time",
      help = "Minimum amount of time to hold a resource offer before declining.")
  @NotNegative
  private static final Arg<Amount<Integer, Time>> MIN_OFFER_HOLD_TIME =
      Arg.create(Amount.of(5, Time.MINUTES));

  @CmdLine(name = "offer_hold_jitter_window",
      help = "Maximum amount of random jitter to add to the offer hold time window.")
  @NotNegative
  private static final Arg<Amount<Integer, Time>> OFFER_HOLD_JITTER_WINDOW =
      Arg.create(Amount.of(1, Time.MINUTES));

  @CmdLine(name = "offer_filter_duration",
      help = "Duration after which we expect Mesos to re-offer unused resources. A short duration "
          + "improves scheduling performance in smaller clusters, but might lead to resource "
          + "starvation for other frameworks if you run many frameworks in your cluster.")
  private static final Arg<Amount<Long, Time>> OFFER_FILTER_DURATION =
      Arg.create(Amount.of(5L, Time.SECONDS));

  @CmdLine(name = "unavailability_threshold",
      help = "Threshold time, when running tasks should be drained from a host, before a host "
          + "becomes unavailable. Should be greater than min_offer_hold_time + "
          + "offer_hold_jitter_window.")
  private static final Arg<Amount<Long, Time>> UNAVAILABILITY_THRESHOLD =
      Arg.create(Amount.of(6L, Time.MINUTES));

  @CmdLine(name = "offer_order",
      help = "Iteration order for offers, to influence task scheduling. Multiple orderings will be "
          + "compounded together. E.g. CPU,MEMORY,RANDOM would sort first by cpus offered, then "
          + " memory and finally would randomize any equal offers.")
  private static final Arg<List<OfferOrder>> OFFER_ORDER =
      Arg.create(ImmutableList.of(OfferOrder.RANDOM));

  @CmdLine(name = "offer_order_modules",
      help = "Custom Guice module to provide an offer ordering.")
  private static final Arg<Set<Module>> OFFER_ORDER_MODULES = Arg.create(
      ImmutableSet.of(MoreModules.lazilyInstantiated(OfferOrderModule.class)));

  public static class OfferOrderModule extends AbstractModule {
    @Override
    protected void configure() {
      bind(new TypeLiteral<Ordering<HostOffer>>() { })
          .toInstance(OfferOrderBuilder.create(OFFER_ORDER.get()));
    }
  }

  /**
   * Binding annotation for the threshold to veto tasks with unavailability.
   */
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface UnavailabilityThreshold { }

  @Override
  protected void configure() {
    long offerHoldTime = OFFER_HOLD_JITTER_WINDOW.get().as(Time.SECONDS)
        + MIN_OFFER_HOLD_TIME.get().as(Time.SECONDS);
    if (UNAVAILABILITY_THRESHOLD.get().as(Time.SECONDS) < offerHoldTime) {
      LOG.warn("unavailability_threshold ({}) is less than the sum of min_offer_hold_time ({}) and"
          + "offer_hold_jitter_window ({}). This creates risks of races between launching and"
          + "draining",
          UNAVAILABILITY_THRESHOLD.get(),
          MIN_OFFER_HOLD_TIME.get(),
          OFFER_HOLD_JITTER_WINDOW.get());
    }

    for (Module module: OFFER_ORDER_MODULES.get()) {
      install(module);
    }

    bind(new TypeLiteral<Amount<Long, Time>>() { })
        .annotatedWith(UnavailabilityThreshold.class)
        .toInstance(UNAVAILABILITY_THRESHOLD.get());

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(OfferManager.class).to(OfferManager.OfferManagerImpl.class);
        bind(OfferManager.OfferManagerImpl.class).in(Singleton.class);
        expose(OfferManager.class);
      }
    });
    PubsubEventModule.bindSubscriber(binder(), OfferManager.class);
  }

  @Provides
  @Singleton
  OfferSettings provideOfferSettings(Ordering<HostOffer> offerOrdering) {
    return new OfferSettings(
        OFFER_FILTER_DURATION.get(),
        new RandomJitterReturnDelay(
            MIN_OFFER_HOLD_TIME.get().as(Time.MILLISECONDS),
            OFFER_HOLD_JITTER_WINDOW.get().as(Time.MILLISECONDS),
            Random.Util.newDefaultRandom()),
        offerOrdering);
  }
}
