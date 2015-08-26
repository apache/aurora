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
package org.apache.aurora.scheduler.async;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Qualifier;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.base.AsyncUtil;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

/**
 * Binding module for async task management.
 */
public class AsyncModule extends AbstractModule {
  private static final Logger LOG = Logger.getLogger(AsyncModule.class.getName());

  @CmdLine(name = "async_worker_threads",
      help = "The number of worker threads to process async task operations with.")
  private static final Arg<Integer> ASYNC_WORKER_THREADS = Arg.create(8);
  private final ScheduledThreadPoolExecutor afterTransaction;

  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface AsyncExecutor { }

  public AsyncModule() {
    // Don't worry about clean shutdown, these can be daemon and cleanup-free.
    // TODO(wfarner): Should we use a bounded caching thread pool executor instead?
    this(AsyncUtil.loggingScheduledExecutor(ASYNC_WORKER_THREADS.get(), "AsyncProcessor-%d", LOG));
  }

  @VisibleForTesting
  public AsyncModule(ScheduledThreadPoolExecutor executor) {
    this.afterTransaction = requireNonNull(executor);
  }

  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(ScheduledThreadPoolExecutor.class).toInstance(afterTransaction);
        bind(ScheduledExecutorService.class).toInstance(afterTransaction);

        bind(GatedDelayExecutor.class).in(Singleton.class);
        expose(GatedDelayExecutor.class);

        bind(RegisterGauges.class).in(Singleton.class);
        expose(RegisterGauges.class);
      }
    });
    SchedulerServicesModule.addAppStartupServiceBinding(binder()).to(RegisterGauges.class);

    bind(Executor.class).annotatedWith(AsyncExecutor.class).to(GatedDelayExecutor.class);
    bind(DelayExecutor.class).annotatedWith(AsyncExecutor.class).to(GatedDelayExecutor.class);
    bind(FlushableWorkQueue.class).annotatedWith(AsyncExecutor.class).to(GatedDelayExecutor.class);
  }

  static class RegisterGauges extends AbstractIdleService {
    @VisibleForTesting
    static final String TIMEOUT_QUEUE_GAUGE = "timeout_queue_size";

    @VisibleForTesting
    static final String ASYNC_TASKS_GAUGE = "async_tasks_completed";

    @VisibleForTesting
    static final String DELAY_QUEUE_GAUGE = "delay_executor_queue_size";

    private final StatsProvider statsProvider;
    private final ScheduledThreadPoolExecutor executor;
    private final GatedDelayExecutor delayExecutor;

    @Inject
    RegisterGauges(
        StatsProvider statsProvider,
        ScheduledThreadPoolExecutor executor,
        GatedDelayExecutor delayExecutor) {

      this.statsProvider = requireNonNull(statsProvider);
      this.executor = requireNonNull(executor);
      this.delayExecutor = requireNonNull(delayExecutor);
    }

    @Override
    protected void startUp() {
      statsProvider.makeGauge(TIMEOUT_QUEUE_GAUGE, () -> executor.getQueue().size());
      statsProvider.makeGauge(ASYNC_TASKS_GAUGE, executor::getCompletedTaskCount);
      // Using a lambda rather than method ref to sidestep a bug in PMD that makes it think
      // delayExecutor is unused.
      statsProvider.makeGauge(DELAY_QUEUE_GAUGE, () -> delayExecutor.getQueueSize());
    }

    @Override
    protected void shutDown() {
      // Nothing to do - await VM shutdown.
    }
  }
}
