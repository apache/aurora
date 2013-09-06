package com.twitter.aurora.scheduler.cron.noop;

import com.google.inject.Guice;
import com.google.inject.Injector;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.scheduler.cron.CronPredictor;
import com.twitter.aurora.scheduler.cron.CronScheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class NoopCronIT {
  private static final String SCHEDULE = "* * * * *";

  private CronScheduler cronScheduler;
  private CronPredictor cronPredictor;

  @Before
  public void setUp() {
    Injector injector = Guice.createInjector(new NoopCronModule());
    cronScheduler = injector.getInstance(CronScheduler.class);
    cronPredictor = injector.getInstance(CronPredictor.class);
  }

  @Test
  public void testLifecycle() throws Exception {
    cronScheduler.start();
    cronScheduler.stop();
  }

  @Test
  public void testSchedule() throws Exception {
    cronScheduler.schedule(SCHEDULE, new Runnable() {
      @Override public void run() {
        // No-op.
      }
    });

    assertEquals(SCHEDULE, cronScheduler.getSchedule(SCHEDULE).orNull());

    cronScheduler.deschedule(SCHEDULE);

    assertNull(cronScheduler.getSchedule(SCHEDULE).orNull());
  }
}
