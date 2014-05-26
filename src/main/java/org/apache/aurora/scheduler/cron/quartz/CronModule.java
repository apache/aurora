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
package org.apache.aurora.scheduler.cron.quartz;

import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.inject.Singleton;

import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.BackoffHelper;

import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CronScheduler;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;

/**
 * Provides a {@link CronJobManager} with a Quartz backend. While Quartz itself supports
 * persistence, the scheduler exposed by this module does not persist any state - it simply
 * creates tasks from a {@link org.apache.aurora.gen.JobConfiguration} template on a cron-style
 * schedule.
 */
public class CronModule extends AbstractModule {
  private static final Logger LOG = Logger.getLogger(CronModule.class.getName());

  @CmdLine(name = "cron_scheduler_num_threads",
      help = "Number of threads to use for the cron scheduler thread pool.")
  private static final Arg<Integer> NUM_THREADS = Arg.create(100);

  @CmdLine(name = "cron_timezone", help = "TimeZone to use for cron predictions.")
  private static final Arg<String> CRON_TIMEZONE = Arg.create("GMT");

  @CmdLine(name = "cron_start_initial_backoff", help =
      "Initial backoff delay while waiting for a previous cron run to be killed.")
  public static final Arg<Amount<Long, Time>> CRON_START_INITIAL_BACKOFF =
      Arg.create(Amount.of(1L, Time.SECONDS));

  @CmdLine(name = "cron_start_max_backoff", help =
      "Max backoff delay while waiting for a previous cron run to be killed.")
  public static final Arg<Amount<Long, Time>> CRON_START_MAX_BACKOFF =
      Arg.create(Amount.of(1L, Time.MINUTES));

  // Global per-JVM ID number generator for the provided Quartz Scheduler.
  private static final AtomicLong ID_GENERATOR = new AtomicLong();

  @Override
  protected void configure() {
    bind(CronPredictor.class).to(CronPredictorImpl.class);
    bind(CronPredictorImpl.class).in(Singleton.class);

    bind(CronJobManager.class).to(CronJobManagerImpl.class);
    bind(CronJobManagerImpl.class).in(Singleton.class);

    bind(CronScheduler.class).to(CronSchedulerImpl.class);
    bind(CronSchedulerImpl.class).in(Singleton.class);

    bind(AuroraCronJobFactory.class).in(Singleton.class);

    bind(AuroraCronJob.class).in(Singleton.class);
    bind(AuroraCronJob.Config.class).toInstance(new AuroraCronJob.Config(
        new BackoffHelper(CRON_START_INITIAL_BACKOFF.get(), CRON_START_MAX_BACKOFF.get())));

    bind(CronLifecycle.class).in(Singleton.class);
    PubsubEventModule.bindSubscriber(binder(), CronLifecycle.class);
  }

  @Provides
  private TimeZone provideTimeZone() {
    TimeZone timeZone = TimeZone.getTimeZone(CRON_TIMEZONE.get());
    TimeZone systemTimeZone = TimeZone.getDefault();
    if (!timeZone.equals(systemTimeZone)) {
      LOG.warning("Cron schedules are configured to fire according to timezone "
          + timeZone.getDisplayName()
          + " but system timezone is set to "
          + systemTimeZone.getDisplayName());
    }
    return timeZone;
  }

  /*
   * NOTE: Quartz implements DirectSchedulerFactory as a mutable global singleton in a static
   * variable. While the Scheduler instances it produces are independent we synchronize here to
   * avoid an initialization race across injectors. In practice this only shows up during testing;
   * production Aurora instances will only have one object graph at a time.
   */
  @Provides
  @Singleton
  private static synchronized Scheduler provideScheduler(AuroraCronJobFactory jobFactory) {
    SimpleThreadPool threadPool = new SimpleThreadPool(NUM_THREADS.get(), Thread.NORM_PRIORITY);
    threadPool.setMakeThreadsDaemons(true);

    DirectSchedulerFactory schedulerFactory = DirectSchedulerFactory.getInstance();
    String schedulerName = "aurora-cron-" + ID_GENERATOR.incrementAndGet();
    try {
      schedulerFactory.createScheduler(schedulerName, schedulerName, threadPool, new RAMJobStore());
      Scheduler scheduler = schedulerFactory.getScheduler(schedulerName);
      scheduler.setJobFactory(jobFactory);
      return scheduler;
    } catch (SchedulerException e) {
      LOG.severe("Error initializing Quartz cron scheduler: " + e);
      throw Throwables.propagate(e);
    }
  }
}
