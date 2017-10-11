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
package org.apache.aurora.scheduler.reconciliation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.ScheduledExecutorService;

import javax.inject.Qualifier;
import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.BackoffStrategy;
import org.apache.aurora.common.util.TruncatedBinaryBackoff;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.config.validators.PositiveAmount;
import org.apache.aurora.scheduler.config.validators.PositiveNumber;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.reconciliation.TaskReconciler.TaskReconcilerSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Binding module for state reconciliation and retry logic.
 */
public class ReconciliationModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(ReconciliationModule.class);

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-transient_task_state_timeout",
        description =
            "The amount of time after which to treat a task stuck in a transient state as LOST.")
    public TimeAmount transientTaskStateTimeout = new TimeAmount(5L, Time.MINUTES);

    @Parameter(names = "-initial_task_kill_retry_interval",
        description = "When killing a task, retry after this delay if mesos has not responded,"
            + " backing off up to transient_task_state_timeout")
    public TimeAmount initialTaskKillRetryInterval = new TimeAmount(15L, Time.SECONDS);

    // Reconciliation may create a big surge of status updates in a large cluster. Setting the
    // default initial delay to 1 minute to ease up storage contention during scheduler start up.
    @Parameter(names = "-reconciliation_initial_delay",
        description =
            "Initial amount of time to delay task reconciliation after scheduler start up.")
    public TimeAmount reconciliationInitialDelay = new TimeAmount(1L, Time.MINUTES);

    @Parameter(names = "-reconciliation_explicit_interval",
        validateValueWith = PositiveAmount.class,
        description = "Interval on which scheduler will ask Mesos for status updates of all"
            + "non-terminal tasks known to scheduler.")
    public TimeAmount reconciliationExplicitInterval = new TimeAmount(60L, Time.MINUTES);

    @Parameter(names = "-reconciliation_implicit_interval",
        validateValueWith = PositiveAmount.class,
        description = "Interval on which scheduler will ask Mesos for status updates of all"
            + "non-terminal tasks known to Mesos.")
    public TimeAmount reconciliationImplicitInterval = new TimeAmount(60L, Time.MINUTES);

    @Parameter(names = "-reconciliation_schedule_spread",
        description =
            "Difference between explicit and implicit reconciliation intervals intended to "
                + "create a non-overlapping task reconciliation schedule.")
    public TimeAmount reconciliationScheduleSpread = new TimeAmount(30L, Time.MINUTES);

    @Parameter(names = "-reconciliation_explicit_batch_size",
        validateValueWith = PositiveNumber.class,
        description =
            "Number of tasks in a single batch request sent to Mesos for explicit reconciliation.")
    public int reconciliationBatchSize = 1000;

    @Parameter(names = "-reconciliation_explicit_batch_interval",
        validateValueWith = PositiveAmount.class,
        description = "Interval between explicit batch reconciliation requests.")
    public TimeAmount reconciliationBatchInterval = new TimeAmount(5L, Time.SECONDS);
  }

  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface BackgroundWorker { }

  private final Options options;

  public ReconciliationModule(Options options) {
    this.options = options;
  }

  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(new TypeLiteral<Amount<Long, Time>>() { })
            .toInstance(options.transientTaskStateTimeout);

        bind(TaskTimeout.class).in(Singleton.class);
        expose(TaskTimeout.class);
      }
    });
    PubsubEventModule.bindSubscriber(binder(), TaskTimeout.class);
    SchedulerServicesModule.addSchedulerActiveServiceBinding(binder()).to(TaskTimeout.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(BackoffStrategy.class).toInstance(
            new TruncatedBinaryBackoff(
                options.initialTaskKillRetryInterval,
                options.transientTaskStateTimeout));
        bind(KillRetry.class).in(Singleton.class);
        expose(KillRetry.class);
      }
    });
    PubsubEventModule.bindSubscriber(binder(), KillRetry.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(TaskReconcilerSettings.class).toInstance(new TaskReconcilerSettings(
            options.reconciliationInitialDelay,
            options.reconciliationExplicitInterval,
            options.reconciliationImplicitInterval,
            options.reconciliationScheduleSpread,
            options.reconciliationBatchInterval,
            options.reconciliationBatchSize));
        bind(ScheduledExecutorService.class).annotatedWith(BackgroundWorker.class)
            .toInstance(AsyncUtil.loggingScheduledExecutor(1, "TaskReconciler-%d", LOG));
        bind(TaskReconciler.class).in(Singleton.class);
        expose(TaskReconciler.class);
      }
    });
    SchedulerServicesModule.addSchedulerActiveServiceBinding(binder()).to(TaskReconciler.class);
  }
}
