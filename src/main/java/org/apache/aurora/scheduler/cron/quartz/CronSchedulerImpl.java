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

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.twitter.common.base.Function;

import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.cron.CronScheduler;
import org.apache.aurora.scheduler.cron.CrontabEntry;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.quartz.CronTrigger;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.apache.aurora.scheduler.cron.quartz.Quartz.jobKey;

class CronSchedulerImpl implements CronScheduler {
  private static final Logger LOG = Logger.getLogger(CronSchedulerImpl.class.getName());

  private final Scheduler scheduler;

  @Inject
  CronSchedulerImpl(Scheduler scheduler) {
    this.scheduler = checkNotNull(scheduler);
  }

  @Override
  public Optional<CrontabEntry> getSchedule(IJobKey jobKey) throws IllegalStateException {
    checkNotNull(jobKey);

    try {
      return Optional.of(Iterables.getOnlyElement(
          FluentIterable.from(scheduler.getTriggersOfJob(jobKey(jobKey)))
              .filter(CronTrigger.class)
              .transform(new Function<CronTrigger, CrontabEntry>() {
                @Override
                public CrontabEntry apply(CronTrigger trigger) {
                  return Quartz.crontabEntry(trigger);
                }
              })));
    } catch (SchedulerException e) {
      LOG.log(Level.SEVERE,
          "Error reading job " + JobKeys.canonicalString(jobKey) + " cronExpression Quartz: " + e,
          e);
      return Optional.absent();
    }
  }
}
