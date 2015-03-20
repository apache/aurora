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
package org.apache.aurora.scheduler.async.preemptor;

import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import org.apache.aurora.scheduler.async.preemptor.PreemptionSlotFinder.PreemptionSlotFinderImpl;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.filter.AttributeAggregate;

import static org.apache.aurora.scheduler.base.AsyncUtil.singleThreadLoggingScheduledExecutor;

public class PreemptorModule extends AbstractModule {

  private static final Logger LOG = Logger.getLogger(PreemptorModule.class.getName());

  @CmdLine(name = "enable_preemptor",
      help = "Enable the preemptor and preemption")
  private static final Arg<Boolean> ENABLE_PREEMPTOR = Arg.create(true);

  @CmdLine(name = "preemption_delay",
      help = "Time interval after which a pending task becomes eligible to preempt other tasks")
  private static final Arg<Amount<Long, Time>> PREEMPTION_DELAY =
      Arg.create(Amount.of(10L, Time.MINUTES));

  @CmdLine(name = "preemption_slot_hold_time",
      help = "Time to hold a preemption slot found before it is discarded.")
  private static final Arg<Amount<Long, Time>> PREEMPTION_SLOT_HOLD_TIME =
      Arg.create(Amount.of(3L, Time.MINUTES));

  private final boolean enablePreemptor;

  @VisibleForTesting
  PreemptorModule(boolean enablePreemptor) {
    this.enablePreemptor = enablePreemptor;
  }

  public PreemptorModule() {
    this(ENABLE_PREEMPTOR.get());
  }

  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        if (enablePreemptor) {
          LOG.info("Preemptor Enabled.");
          bind(ScheduledExecutorService.class)
              .annotatedWith(PreemptorImpl.PreemptionExecutor.class)
              .toInstance(singleThreadLoggingScheduledExecutor("PreemptorProcessor-%d", LOG));
          bind(PreemptorMetrics.class).in(Singleton.class);
          bind(PreemptionSlotFinder.class).to(PreemptionSlotFinderImpl.class);
          bind(PreemptionSlotFinderImpl.class).in(Singleton.class);
          bind(Preemptor.class).to(PreemptorImpl.class);
          bind(PreemptorImpl.class).in(Singleton.class);
          bind(new TypeLiteral<Amount<Long, Time>>() { })
              .annotatedWith(PreemptorImpl.PreemptionDelay.class)
              .toInstance(PREEMPTION_DELAY.get());
          bind(new TypeLiteral<Amount<Long, Time>>() { })
              .annotatedWith(PreemptionSlotCache.PreemptionSlotHoldDuration.class)
              .toInstance(PREEMPTION_SLOT_HOLD_TIME.get());
          bind(PreemptionSlotCache.class).in(Singleton.class);
          bind(ClusterState.class).to(ClusterStateImpl.class);
          bind(ClusterStateImpl.class).in(Singleton.class);
          expose(ClusterStateImpl.class);
        } else {
          bind(Preemptor.class).toInstance(NULL_PREEMPTOR);
          LOG.warning("Preemptor Disabled.");
        }

        expose(Preemptor.class);
      }
    });

    // We can't do this in the private module due to the known conflict between multibindings
    // and private modules due to multiple injectors.  We accept the added complexity here to keep
    // the other bindings private.
    PubsubEventModule.bindSubscriber(binder(), ClusterStateImpl.class);
  }

  private static final Preemptor NULL_PREEMPTOR = new Preemptor() {
    @Override
    public Optional<String> attemptPreemptionFor(
        String taskId,
        AttributeAggregate attributeAggregate) {

      return Optional.absent();
    }
  };
}
