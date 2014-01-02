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
package org.apache.aurora.scheduler.cron.testing;

import java.util.concurrent.CountDownLatch;

import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CronScheduler;
import org.junit.Test;

import static org.apache.aurora.gen.test.testConstants.VALID_CRON_SCHEDULES;
import static org.junit.Assert.assertTrue;

/**
 * Abstract test to verify conformance with the {@link CronScheduler} interface.
 */
public abstract class AbstractCronIT extends EasyMockTest {
  /**
   * Child should return an instance of the {@link CronScheduler} test under test here.
   */
  protected abstract CronScheduler makeCronScheduler() throws Exception;

  /**
   * Child should configure expectations for a scheduler start.
   */
  protected abstract void expectStartCronScheduler();

  /**
   * Child should configure expectations for a scheduler stop.
   */
  protected abstract void expectStopCronScheduler();

  /**
   * Child should return an instance of the {@link CronPredictor} under test here.
   */
  protected abstract CronPredictor makeCronPredictor() throws Exception;

  @Test
  public void testCronSchedulerLifecycle() throws Exception {
    CronScheduler scheduler = makeCronScheduler();

    expectStartCronScheduler();
    expectStopCronScheduler();

    control.replay();

    scheduler.start();
    final CountDownLatch cronRan = new CountDownLatch(1);
    scheduler.schedule("* * * * *", new Runnable() {
      @Override public void run() {
        cronRan.countDown();
      }
    });
    cronRan.await();
    scheduler.stop();
  }

  @Test
  public void testCronPredictorAcceptsValidSchedules() throws Exception {
    control.replay();

    CronPredictor cronPredictor = makeCronPredictor();
    for (String schedule : VALID_CRON_SCHEDULES) {
      cronPredictor.predictNextRun(schedule);
    }
  }

  @Test
  public void testCronScheduleValidatorAcceptsValidSchedules() throws Exception {
    CronScheduler cron = makeCronScheduler();

    control.replay();

    for (String schedule : VALID_CRON_SCHEDULES) {
      assertTrue(String.format("Cron schedule %s should validate.", schedule),
          cron.isValidSchedule(schedule));
    }
  }
}
