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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.stats.StatsProvider;

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
  private static final Arg<Integer> ASYNC_WORKER_THREADS = Arg.create(1);

  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface AsyncExecutor { }

  @VisibleForTesting
  static final String TIMEOUT_QUEUE_GAUGE = "timeout_queue_size";

  @VisibleForTesting
  static final String ASYNC_TASKS_GAUGE = "async_tasks_completed";

  @Override
  protected void configure() {
    // Don't worry about clean shutdown, these can be daemon and cleanup-free.
    final ScheduledThreadPoolExecutor executor =
        AsyncUtil.loggingScheduledExecutor(ASYNC_WORKER_THREADS.get(), "AsyncProcessor-%d", LOG);
    bind(ScheduledThreadPoolExecutor.class).annotatedWith(AsyncExecutor.class).toInstance(executor);
    bind(ScheduledExecutorService.class).annotatedWith(AsyncExecutor.class).toInstance(executor);
    bind(ExecutorService.class).annotatedWith(AsyncExecutor.class).toInstance(executor);
    SchedulerServicesModule.addAppStartupServiceBinding(binder()).to(RegisterGauges.class);
  }

  static class RegisterGauges extends AbstractIdleService {
    private final StatsProvider statsProvider;
    private final ScheduledThreadPoolExecutor executor;

    @Inject
    RegisterGauges(
        StatsProvider statsProvider,
        @AsyncExecutor ScheduledThreadPoolExecutor executor) {

      this.statsProvider = requireNonNull(statsProvider);
      this.executor = requireNonNull(executor);
    }

    @Override
    protected void startUp() {
      statsProvider.makeGauge(
          TIMEOUT_QUEUE_GAUGE,
          new Supplier<Integer>() {
            @Override
            public Integer get() {
              return executor.getQueue().size();
            }
          });
      statsProvider.makeGauge(
          ASYNC_TASKS_GAUGE,
          new Supplier<Long>() {
            @Override
            public Long get() {
              return executor.getCompletedTaskCount();
            }
          }
      );
    }

    @Override
    protected void shutDown() {
      // Nothing to do - await VM shutdown.
    }
  }
}
