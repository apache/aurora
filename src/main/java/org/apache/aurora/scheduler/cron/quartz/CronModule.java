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

import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.BackoffHelper;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CronScheduler;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.SimpleThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.quartz.impl.StdSchedulerFactory.PROP_SCHED_INSTANCE_ID;
import static org.quartz.impl.StdSchedulerFactory.PROP_SCHED_MAKE_SCHEDULER_THREAD_DAEMON;
import static org.quartz.impl.StdSchedulerFactory.PROP_SCHED_NAME;
import static org.quartz.impl.StdSchedulerFactory.PROP_THREAD_POOL_CLASS;
import static org.quartz.impl.StdSchedulerFactory.PROP_THREAD_POOL_PREFIX;

/**
 * Provides a {@link CronJobManager} with a Quartz backend. While Quartz itself supports
 * persistence, the scheduler exposed by this module does not persist any state - it simply
 * creates tasks from a {@link org.apache.aurora.gen.JobConfiguration} template on a cron-style
 * schedule.
 */
public class CronModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(CronModule.class);

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

  interface Params {
    int cronSchedulerNumThreads();

    String cronTimezone();

    Amount<Long, Time> cronStartInitialBackoff();

    Amount<Long, Time> cronStartMaxBackoff();
  }

  private final Params params;

  public CronModule() {
    this.params = new Params() {
      @Override
      public int cronSchedulerNumThreads() {
        return NUM_THREADS.get();
      }

      @Override
      public String cronTimezone() {
        return CRON_TIMEZONE.get();
      }

      @Override
      public Amount<Long, Time> cronStartInitialBackoff() {
        return CRON_START_INITIAL_BACKOFF.get();
      }

      @Override
      public Amount<Long, Time> cronStartMaxBackoff() {
        return CRON_START_MAX_BACKOFF.get();
      }
    };
  }

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
        new BackoffHelper(params.cronStartInitialBackoff(), params.cronStartMaxBackoff())));

    bind(CronLifecycle.class).in(Singleton.class);
    SchedulerServicesModule.addSchedulerActiveServiceBinding(binder()).to(CronLifecycle.class);
  }

  @Provides
  TimeZone provideTimeZone() {
    TimeZone timeZone = TimeZone.getTimeZone(params.cronTimezone());
    TimeZone systemTimeZone = TimeZone.getDefault();
    if (!timeZone.equals(systemTimeZone)) {
      LOG.warn("Cron schedules are configured to fire according to timezone "
          + timeZone.getDisplayName()
          + " but system timezone is set to "
          + systemTimeZone.getDisplayName());
    }
    return timeZone;
  }

  // Global per-JVM ID number generator for the provided Quartz Scheduler.
  private static final AtomicLong ID_GENERATOR = new AtomicLong();

  @Provides
  @Singleton
  Scheduler provideScheduler(AuroraCronJobFactory jobFactory) throws SchedulerException {
    // There are several ways to create a quartz Scheduler instance.  This path was chosen as the
    // simplest to create a Scheduler that uses a *daemon* QuartzSchedulerThread instance.
    Properties props = new Properties();
    String name = "aurora-cron-" + ID_GENERATOR.incrementAndGet();
    props.setProperty(PROP_SCHED_NAME, name);
    props.setProperty(PROP_SCHED_INSTANCE_ID, name);
    props.setProperty(PROP_THREAD_POOL_CLASS, SimpleThreadPool.class.getCanonicalName());
    props.setProperty(
        PROP_THREAD_POOL_PREFIX + ".threadCount",
        String.valueOf(params.cronSchedulerNumThreads()));
    props.setProperty(PROP_THREAD_POOL_PREFIX + ".makeThreadsDaemons", Boolean.TRUE.toString());

    props.setProperty(PROP_SCHED_MAKE_SCHEDULER_THREAD_DAEMON, Boolean.TRUE.toString());
    Scheduler scheduler = new StdSchedulerFactory(props).getScheduler();
    scheduler.setJobFactory(jobFactory);
    return scheduler;
  }
}
