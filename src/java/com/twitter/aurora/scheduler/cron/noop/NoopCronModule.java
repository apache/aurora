package com.twitter.aurora.scheduler.cron.noop;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

import com.twitter.aurora.scheduler.cron.CronPredictor;
import com.twitter.aurora.scheduler.cron.CronScheduler;

/**
 * A Module to wire up a cron scheduler that does not actually schedule cron jobs.
 *
 * This class exists as a short term hack to get around a license compatibility issue - Real
 * Implementation (TM) coming soon.
 */
public class NoopCronModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(CronScheduler.class).to(NoopCronScheduler.class);
    bind(NoopCronScheduler.class).in(Singleton.class);

    bind(CronPredictor.class).to(NoopCronPredictor.class);
    bind(NoopCronPredictor.class).in(Singleton.class);
  }
}
