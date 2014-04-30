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

import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.Singleton;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import org.apache.aurora.scheduler.base.AsyncUtil;
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

  @CmdLine(name = "sla_stat_refresh_rate", help = "The SLA stat refresh rate.")
  private static final Arg<Amount<Long, Time>> SLA_REFRESH_RATE =
      Arg.create(Amount.of(1L, Time.MINUTES));

  @BindingAnnotation
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  private @interface SlaExecutor { }

  @Override
  protected void configure() {
    bind(MetricCalculatorSettings.class).toInstance(
        new MetricCalculatorSettings(SLA_REFRESH_RATE.get().as(Time.MILLISECONDS)));

    bind(MetricCalculator.class).in(Singleton.class);
    bind(ScheduledExecutorService.class)
        .annotatedWith(SlaExecutor.class)
        .toInstance(AsyncUtil.singleThreadLoggingScheduledExecutor("SlaStat-%d", LOG));

    LifecycleModule.bindStartupAction(binder(), SlaUpdater.class);
  }

  static class SlaUpdater implements Command {
    private final ScheduledExecutorService executor;
    private final MetricCalculator calculator;

    @Inject
    SlaUpdater(@SlaExecutor ScheduledExecutorService executor, MetricCalculator calculator) {
      this.executor = checkNotNull(executor);
      this.calculator = checkNotNull(calculator);
    }

    @Override
    public void execute() throws RuntimeException {
      long interval = SLA_REFRESH_RATE.get().as(Time.SECONDS);
      executor.scheduleAtFixedRate(calculator, interval, interval, TimeUnit.SECONDS);
    }
  }
}
