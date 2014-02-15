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

import java.io.InputStreamReader;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.twitter.common.util.Clock;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CronScheduler;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Abstract test to verify conformance with the {@link CronScheduler} interface.
 */
public abstract class AbstractCronIT {
  private static final String WILDCARD_SCHEDULE = "* * * * *";

  /**
   * Child should return an instance of the {@link CronScheduler} test under test here.
   */
  protected abstract CronScheduler makeCronScheduler() throws Exception;

  /**
   * Child should return an instance of the {@link CronPredictor} under test here.
   *
   * @param clock The clock the predictor should use.
   */
  protected abstract CronPredictor makeCronPredictor(Clock clock) throws Exception;

  @Test
  public void testCronSchedulerLifecycle() throws Exception {
    CronScheduler scheduler = makeCronScheduler();

    scheduler.startAsync().awaitRunning();
    final CountDownLatch cronRan = new CountDownLatch(1);
    scheduler.schedule(WILDCARD_SCHEDULE, new Runnable() {
      @Override public void run() {
        cronRan.countDown();
      }
    });
    cronRan.await();
    scheduler.stopAsync().awaitTerminated();
  }

  @Test
  public void testCronPredictorConforms() throws Exception {
    FakeClock clock = new FakeClock();
    CronPredictor cronPredictor = makeCronPredictor(clock);

    for (TriggerPrediction triggerPrediction : getExpectedTriggerPredictions()) {
      List<Long> results = Lists.newArrayList();
      clock.setNowMillis(0);
      for (int i = 0; i < triggerPrediction.getTriggerTimes().size(); i++) {
        Date nextTriggerTime = cronPredictor.predictNextRun(triggerPrediction.getSchedule());
        results.add(nextTriggerTime.getTime());
        clock.setNowMillis(nextTriggerTime.getTime());
      }
      assertEquals("Cron schedule "
          + triggerPrediction.getSchedule()
          + " should have have predicted trigger times "
          + triggerPrediction.getTriggerTimes()
          + " but predicted "
          + results
          + " instead.", triggerPrediction.getTriggerTimes(), results);
    }
  }

  @Test
  public void testCronScheduleValidatorAcceptsValidSchedules() throws Exception {
    CronScheduler cron = makeCronScheduler();

    for (TriggerPrediction triggerPrediction : getExpectedTriggerPredictions()) {
      assertTrue("Cron schedule " + triggerPrediction.getSchedule() + " should pass validation.",
          cron.isValidSchedule(triggerPrediction.getSchedule()));
    }
  }

  private static List<TriggerPrediction> getExpectedTriggerPredictions() {
    return new Gson()
        .fromJson(
            new InputStreamReader(
                AbstractCronIT.class.getResourceAsStream("cron-schedule-predictions.json")),
            new TypeToken<List<TriggerPrediction>>() { }.getType());
  }

  /**
   * A schedule and the expected iteratively-applied prediction results.
   */
  public static class TriggerPrediction {
    private String schedule;
    private List<Long> triggerTimes;

    private TriggerPrediction() {
      // GSON constructor.
    }

    public TriggerPrediction(String schedule, List<Long> triggerTimes) {
      this.schedule = schedule;
      this.triggerTimes = triggerTimes;
    }

    public String getSchedule() {
      return schedule;
    }

    public List<Long> getTriggerTimes() {
      return ImmutableList.copyOf(triggerTimes);
    }
  }
}
