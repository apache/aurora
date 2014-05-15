/**
 * Copyright 2014 Apache Software Foundation
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

import java.text.ParseException;
import java.util.List;
import java.util.TimeZone;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.cron.CrontabEntry;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.TriggerBuilder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Utilities for converting Aurora datatypes to Quartz datatypes.
 */
final class Quartz {
  private Quartz() {
    // Utility class.
  }

  /**
   * Convert an Aurora CrontabEntry to a Quartz CronExpression.
   */
  static CronExpression cronExpression(CrontabEntry entry, TimeZone timeZone) {
    String dayOfMonth;
    if (entry.hasWildcardDayOfMonth()) {
      dayOfMonth = "?"; // special quartz token meaning "don't care"
    } else {
      dayOfMonth = entry.getDayOfMonthAsString();
    }
    String dayOfWeek;
    if (entry.hasWildcardDayOfWeek() && !entry.hasWildcardDayOfMonth()) {
      dayOfWeek = "?";
    } else {
      List<Integer> daysOfWeek = Lists.newArrayList();
      for (Range<Integer> range : entry.getDayOfWeek().asRanges()) {
        for (int i : ContiguousSet.create(range, DiscreteDomain.integers())) {
          daysOfWeek.add(i + 1); // Quartz has an off-by-one with what the "standard" defines.
        }
      }
      dayOfWeek = Joiner.on(",").join(daysOfWeek);
    }

    String rawCronExpresion = Joiner.on(" ").join(
        "0",
        entry.getMinuteAsString(),
        entry.getHourAsString(),
        dayOfMonth,
        entry.getMonthAsString(),
        dayOfWeek);
    CronExpression cronExpression;
    try {
      cronExpression = new CronExpression(rawCronExpresion);
    } catch (ParseException e) {
      throw Throwables.propagate(e);
    }
    cronExpression.setTimeZone(timeZone);
    return cronExpression;
  }

  /**
   * Convert a Quartz JobKey to an Aurora IJobKey.
   */
  static IJobKey auroraJobKey(org.quartz.JobKey jobKey) {
    return JobKeys.parse(jobKey.getName());
  }

  /**
   * Convert an Aurora IJobKey to a Quartz JobKey.
   */
  static JobKey jobKey(IJobKey jobKey) {
    return JobKey.jobKey(JobKeys.canonicalString(jobKey));
  }

  static CronTrigger cronTrigger(CrontabEntry schedule, TimeZone timeZone) {
    return TriggerBuilder.newTrigger()
        .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression(schedule, timeZone)))
        .withDescription(schedule.toString())
        .build();
  }

  static JobDetail jobDetail(IJobKey jobKey, Class<? extends Job> jobClass) {
    checkNotNull(jobKey);
    checkNotNull(jobClass);

    return JobBuilder.newJob(jobClass)
        .withIdentity(jobKey(jobKey))
        .build();
  }

  static CrontabEntry crontabEntry(CronTrigger cronTrigger) {
    return CrontabEntry.parse(cronTrigger.getDescription());
  }
}
