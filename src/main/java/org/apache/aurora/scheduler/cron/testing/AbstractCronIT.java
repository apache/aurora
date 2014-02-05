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

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CronScheduler;
import org.junit.Test;

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
   * Child should return cron expressions that are expected to pass validation.
   */
  protected abstract Collection<String> getValidCronSchedules();

  /**
   * Child should return a "wildcard" cron expression that executes at every possible moment.
   */
  protected abstract String getWildcardCronSchedule();

  /**
   * Child should return an instance of the {@link CronPredictor} under test here.
   */
  protected abstract CronPredictor makeCronPredictor() throws Exception;

  @Test
  public void testCronSchedulerLifecycle() throws Exception {
    CronScheduler scheduler = makeCronScheduler();

    control.replay();

    scheduler.startAsync().awaitRunning();
    final CountDownLatch cronRan = new CountDownLatch(1);
    scheduler.schedule(getWildcardCronSchedule(), new Runnable() {
      @Override public void run() {
        cronRan.countDown();
      }
    });
    cronRan.await();
    scheduler.stopAsync().awaitTerminated();
  }

  @Test
  public void testCronPredictorAcceptsValidSchedules() throws Exception {
    control.replay();

    CronPredictor cronPredictor = makeCronPredictor();
    for (String schedule : getValidCronSchedules()) {
      cronPredictor.predictNextRun(schedule);
    }
  }

  @Test
  public void testCronScheduleValidatorAcceptsValidSchedules() throws Exception {
    CronScheduler cron = makeCronScheduler();

    control.replay();

    for (String schedule : getValidCronSchedules()) {
      assertTrue(String.format("Cron schedule %s should validate.", schedule),
          cron.isValidSchedule(schedule));
    }
  }
}
