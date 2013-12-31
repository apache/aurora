/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.cron.noop;

import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CronScheduler;

import org.junit.Before;
import org.junit.Test;

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
