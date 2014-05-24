/**
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
package org.apache.aurora.scheduler.cron.quartz;

import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CrontabEntry;

import org.apache.aurora.scheduler.cron.ExpectedPrediction;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
    assertEquals(expectedPrediction, cronPredictor.predictNextRun(CrontabEntry.parse("* * * * *")));
  }

  @Test
  public void testCronExpressions() {
    assertEquals("0 * * ? * 1,2,3,4,5,6,7",
        Quartz.cronExpression(CRONTAB_ENTRY, TIME_ZONE).getCronExpression());
  }

  @Test
  public void testCronPredictorConforms() throws Exception {
    for (ExpectedPrediction expectedPrediction : ExpectedPrediction.getAll()) {
      List<Date> results = Lists.newArrayList();
      clock.setNowMillis(0);
      for (int i = 0; i < expectedPrediction.getTriggerTimes().size(); i++) {
        Date nextTriggerTime = cronPredictor.predictNextRun(expectedPrediction.parseCrontabEntry());
        results.add(nextTriggerTime);
        clock.setNowMillis(nextTriggerTime.getTime());
      }
      assertEquals(
          "Cron schedule " + expectedPrediction.getSchedule() + " made unexpected predictions.",
          Lists.transform(
              expectedPrediction.getTriggerTimes(),
              new Function<Long, Date>() {
                @Override
                public Date apply(Long time) {
                  return new Date(time);
                }
              }
          ),
          results);
    }
  }
}
