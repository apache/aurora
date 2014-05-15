/**
 * Copyright 2014 Apache Software Foundation
 *
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
package org.apache.aurora.scheduler.sla;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.Subscribe;
import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.Singleton;

import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.Positive;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.SchedulerActive;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.sla.MetricCalculator.MetricCalculatorSettings;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Binding module for the sla processor.
 */
public class SlaModule extends AbstractModule {

  private static final Logger LOG = Logger.getLogger(SlaModule.class.getName());

  @Positive
  @CmdLine(name = "sla_stat_refresh_interval", help = "The SLA stat refresh interval.")
  private static final Arg<Amount<Long, Time>> SLA_REFRESH_INTERVAL =
      Arg.create(Amount.of(1L, Time.MINUTES));

  @VisibleForTesting
  @BindingAnnotation
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface SlaExecutor { }

  private final Amount<Long, Time> refreshInterval;

  @VisibleForTesting
  SlaModule(Amount<Long, Time> refreshInterval) {
    this.refreshInterval = refreshInterval;
  }

  public SlaModule() {
    this(SLA_REFRESH_INTERVAL.get());
  }

  @Override
  protected void configure() {
    bind(MetricCalculatorSettings.class)
        .toInstance(new MetricCalculatorSettings(refreshInterval.as(Time.MILLISECONDS)));

    bind(MetricCalculator.class).in(Singleton.class);
    bind(ScheduledExecutorService.class)
        .annotatedWith(SlaExecutor.class)
        .toInstance(AsyncUtil.singleThreadLoggingScheduledExecutor("SlaStat-%d", LOG));

    PubsubEventModule.bindSubscriber(binder(), SlaUpdater.class);
    LifecycleModule.bindStartupAction(binder(), SlaUpdater.class);
  }

  static class SlaUpdater implements Command, EventSubscriber {
    private final ScheduledExecutorService executor;
    private final MetricCalculator calculator;
    private final MetricCalculatorSettings settings;

    @Inject
    SlaUpdater(
        @SlaExecutor ScheduledExecutorService executor,
        MetricCalculator calculator,
        MetricCalculatorSettings settings) {

      this.executor = checkNotNull(executor);
      this.calculator = checkNotNull(calculator);
      this.settings = checkNotNull(settings);
    }

    @Subscribe
    public void schedulerActive(SchedulerActive event) {
      long interval = settings.getRefreshRateMs();
      executor.scheduleAtFixedRate(calculator, interval, interval, TimeUnit.MILLISECONDS);
      LOG.info(String.format("Scheduled SLA calculation with %d msec interval.", interval));
    }

    @Override
    public void execute() throws RuntimeException {
      // Execution scheduled on SchedulerActive event.
    }
  }
}
