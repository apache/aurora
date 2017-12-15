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

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

import com.google.common.collect.Lists;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CrontabEntry;
import org.apache.aurora.scheduler.cron.ExpectedPrediction;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CronPredictorImplTest {
  private static final TimeZone TIME_ZONE = TimeZone.getTimeZone("GMT");
  public static final CrontabEntry CRONTAB_ENTRY = CrontabEntry.parse("* * * * *");

  private CronPredictor cronPredictor;

  private FakeClock clock;

  @Before
  public void setUp() {
    clock = new FakeClock();
    cronPredictor = new CronPredictorImpl(clock, TIME_ZONE);
  }

  @Test
  public void testValidSchedule() {
    clock.advance(Amount.of(1L, Time.DAYS));
    Date expectedPrediction = new Date(Amount.of(1L, Time.DAYS).as(Time.MILLISECONDS)
            + Amount.of(1L, Time.MINUTES).as(Time.MILLISECONDS));
    assertEquals(
        Optional.of(expectedPrediction),
        cronPredictor.predictNextRun(CrontabEntry.parse("* * * * *")));
  }

  @Test
  public void testCronExpressions() {
    assertEquals("0 * * ? * 1,2,3,4,5,6,7",
        Quartz.cronExpression(CRONTAB_ENTRY, TIME_ZONE).getCronExpression());
  }

  @Test
  public void testInvalidPrediction() {
    // Too far in the future to represent as a Date.
    clock.advance(Amount.of(Long.MAX_VALUE, Time.DAYS));
    assertEquals(Optional.empty(), cronPredictor.predictNextRun(CrontabEntry.parse("* * * * *")));
  }

  @Test
  public void testCronPredictorConforms() throws Exception {
    for (ExpectedPrediction expectedPrediction : ExpectedPrediction.getAll()) {
      List<Date> results = Lists.newArrayList();
      clock.setNowMillis(0);
      for (int i = 0; i < expectedPrediction.getTriggerTimes().size(); i++) {
        Optional<Date> nextTriggerTime =
            cronPredictor.predictNextRun(expectedPrediction.parseCrontabEntry());
        assertTrue(nextTriggerTime.isPresent());

        Date triggerTime = nextTriggerTime.get();
        results.add(triggerTime);
        clock.setNowMillis(triggerTime.getTime());
      }
      assertEquals(
          "Cron schedule " + expectedPrediction.getSchedule() + " made unexpected predictions.",
          Lists.transform(
              expectedPrediction.getTriggerTimes(),
              Date::new
          ),
          results);
    }
  }
}
