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

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.args.constraints.NotNegative;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.Random;

import org.apache.aurora.scheduler.events.PubsubEventModule;

/**
 * Binding module for resource offer management.
 */
public class OffersModule extends AbstractModule {

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

  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(OfferManager.OfferReturnDelay.class).toInstance(
            new RandomJitterReturnDelay(
                MIN_OFFER_HOLD_TIME.get().as(Time.MILLISECONDS),
                OFFER_HOLD_JITTER_WINDOW.get().as(Time.MILLISECONDS),
                Random.Util.newDefaultRandom()));
        bind(OfferManager.class).to(OfferManager.OfferManagerImpl.class);
        bind(OfferManager.OfferManagerImpl.class).in(Singleton.class);
        expose(OfferManager.class);
      }
    });
    PubsubEventModule.bindSubscriber(binder(), OfferManager.class);
  }
}
