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

import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.args.constraints.Positive;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.BackoffStrategy;
import org.apache.aurora.common.util.TruncatedBinaryBackoff;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.base.AsyncUtil;
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

  @CmdLine(name = "transient_task_state_timeout",
      help = "The amount of time after which to treat a task stuck in a transient state as LOST.")
  private static final Arg<Amount<Long, Time>> TRANSIENT_TASK_STATE_TIMEOUT =
      Arg.create(Amount.of(5L, Time.MINUTES));

  @CmdLine(name = "initial_task_kill_retry_interval",
      help = "When killing a task, retry after this delay if mesos has not responded,"
          + " backing off up to transient_task_state_timeout")
  private static final Arg<Amount<Long, Time>> INITIAL_TASK_KILL_RETRY_INTERVAL =
      Arg.create(Amount.of(5L, Time.SECONDS));

  // Reconciliation may create a big surge of status updates in a large cluster. Setting the default
  // initial delay to 1 minute to ease up storage contention during scheduler start up.
  @CmdLine(name = "reconciliation_initial_delay",
      help = "Initial amount of time to delay task reconciliation after scheduler start up.")
  private static final Arg<Amount<Long, Time>> RECONCILIATION_INITIAL_DELAY =
      Arg.create(Amount.of(1L, Time.MINUTES));

  @Positive
  @CmdLine(name = "reconciliation_explicit_interval",
      help = "Interval on which scheduler will ask Mesos for status updates of all non-terminal "
          + "tasks known to scheduler.")
  private static final Arg<Amount<Long, Time>> RECONCILIATION_EXPLICIT_INTERVAL =
      Arg.create(Amount.of(60L, Time.MINUTES));

  @Positive
  @CmdLine(name = "reconciliation_implicit_interval",
      help = "Interval on which scheduler will ask Mesos for status updates of all non-terminal "
          + "tasks known to Mesos.")
  private static final Arg<Amount<Long, Time>> RECONCILIATION_IMPLICIT_INTERVAL =
      Arg.create(Amount.of(60L, Time.MINUTES));

  @CmdLine(name = "reconciliation_schedule_spread",
      help = "Difference between explicit and implicit reconciliation intervals intended to "
          + "create a non-overlapping task reconciliation schedule.")
  private static final Arg<Amount<Long, Time>> RECONCILIATION_SCHEDULE_SPREAD =
      Arg.create(Amount.of(30L, Time.MINUTES));

  interface Params {
    Amount<Long, Time> transientTaskStateTimeout();

    Amount<Long, Time> initialTaskKillRetryInterval();

    Amount<Long, Time> reconciliationInitialDelay();

    Amount<Long, Time> reconciliationExplicitInterval();

    Amount<Long, Time> reconciliationImplicitInterval();

    Amount<Long, Time> reconciliationScheduleSpread();
  }

  private final Params params;

  public ReconciliationModule() {
    this.params = new Params() {
      @Override
      public Amount<Long, Time> transientTaskStateTimeout() {
        return TRANSIENT_TASK_STATE_TIMEOUT.get();
      }

      @Override
      public Amount<Long, Time> initialTaskKillRetryInterval() {
        return INITIAL_TASK_KILL_RETRY_INTERVAL.get();
      }

      @Override
      public Amount<Long, Time> reconciliationInitialDelay() {
        return RECONCILIATION_INITIAL_DELAY.get();
      }

      @Override
      public Amount<Long, Time> reconciliationExplicitInterval() {
        return RECONCILIATION_EXPLICIT_INTERVAL.get();
      }

      @Override
      public Amount<Long, Time> reconciliationImplicitInterval() {
        return RECONCILIATION_IMPLICIT_INTERVAL.get();
      }

      @Override
      public Amount<Long, Time> reconciliationScheduleSpread() {
        return RECONCILIATION_SCHEDULE_SPREAD.get();
      }
    };
  }

  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface BackgroundWorker { }

  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(new TypeLiteral<Amount<Long, Time>>() { })
            .toInstance(params.transientTaskStateTimeout());

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
                params.initialTaskKillRetryInterval(),
                params.transientTaskStateTimeout()));
        bind(KillRetry.class).in(Singleton.class);
        expose(KillRetry.class);
      }
    });
    PubsubEventModule.bindSubscriber(binder(), KillRetry.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(TaskReconcilerSettings.class).toInstance(new TaskReconcilerSettings(
            params.reconciliationInitialDelay(),
            params.reconciliationExplicitInterval(),
            params.reconciliationImplicitInterval(),
            params.reconciliationScheduleSpread()));
        bind(ScheduledExecutorService.class).annotatedWith(BackgroundWorker.class)
            .toInstance(AsyncUtil.loggingScheduledExecutor(1, "TaskReconciler-%d", LOG));
        bind(TaskReconciler.class).in(Singleton.class);
        expose(TaskReconciler.class);
      }
    });
    SchedulerServicesModule.addSchedulerActiveServiceBinding(binder()).to(TaskReconciler.class);
  }
}
