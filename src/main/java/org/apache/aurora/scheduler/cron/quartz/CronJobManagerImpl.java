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

import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;

import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.CrontabEntry;
import org.apache.aurora.scheduler.cron.SanitizedCronJob;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.quartz.CronTrigger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.matchers.GroupMatcher;

import static java.util.Objects.requireNonNull;

/**
 * NOTE: The source of truth for whether a cron job exists or not is always the JobStore. If state
 * somehow becomes inconsistent (i.e. a job key is scheduled for execution but its underlying
 * JobConfiguration does not exist in storage the execution of the job will log a warning and
 * exit).
 */
class CronJobManagerImpl implements CronJobManager {
  private static final Logger LOG = Logger.getLogger(CronJobManagerImpl.class.getName());

  private final Storage storage;
  private final Scheduler scheduler;
  private final TimeZone timeZone;

  @Inject
  CronJobManagerImpl(Storage storage, Scheduler scheduler, TimeZone timeZone) {
    this.storage = requireNonNull(storage);
    this.scheduler = requireNonNull(scheduler);
    this.timeZone = requireNonNull(timeZone);
  }

  @Override
  public void startJobNow(final IJobKey jobKey) throws CronException {
    requireNonNull(jobKey);

    storage.read(new Work<Void, CronException>() {
      @Override
      public Void apply(Storage.StoreProvider storeProvider) throws CronException {
        checkCronExists(jobKey, storeProvider.getCronJobStore());
        triggerJob(jobKey);
        return null;
      }
    });
  }

  private void triggerJob(IJobKey jobKey) throws CronException {
    try {
      scheduler.triggerJob(Quartz.jobKey(jobKey));
    } catch (SchedulerException e) {
      throw new CronException(e);
    }
    LOG.info(formatMessage("Triggered cron job for %s.", jobKey));
  }

  private static void checkNoRunOverlap(SanitizedCronJob cronJob) throws CronException {
    // NOTE: We check at create and update instead of in SanitizedCronJob to allow existing jobs
    // but reject new ones.
    if (CronCollisionPolicy.RUN_OVERLAP.equals(cronJob.getCronCollisionPolicy())) {
      throw new CronException(
          "The RUN_OVERLAP collision policy has been removed (AURORA-38).");
    }
  }

  @Override
  public void updateJob(final SanitizedCronJob config) throws CronException {
    requireNonNull(config);
    checkNoRunOverlap(config);

    final IJobKey jobKey = config.getSanitizedConfig().getJobConfig().getKey();
    storage.write(new MutateWork.NoResult<CronException>() {
      @Override
      public void execute(Storage.MutableStoreProvider storeProvider) throws CronException {
        checkCronExists(jobKey, storeProvider.getCronJobStore());

        removeJob(jobKey, storeProvider.getCronJobStore());
        descheduleJob(jobKey);
        saveJob(config, storeProvider.getCronJobStore());
        scheduleJob(config.getCrontabEntry(), jobKey);
      }
    });
  }

  @Override
  public void createJob(final SanitizedCronJob cronJob) throws CronException {
    requireNonNull(cronJob);
    checkNoRunOverlap(cronJob);

    final IJobKey jobKey = cronJob.getSanitizedConfig().getJobConfig().getKey();
    storage.write(new MutateWork.NoResult<CronException>() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider) throws CronException {
        checkNotExists(jobKey, storeProvider.getCronJobStore());

        saveJob(cronJob, storeProvider.getCronJobStore());
        scheduleJob(cronJob.getCrontabEntry(), jobKey);
      }
    });
  }

  private void checkNotExists(IJobKey jobKey, CronJobStore cronJobStore) throws CronException {
    if (cronJobStore.fetchJob(jobKey).isPresent()) {
      throw new CronException(formatMessage("Job already exists for %s.", jobKey));
    }
  }

  private void checkCronExists(IJobKey jobKey, CronJobStore cronJobStore) throws CronException {
    if (!cronJobStore.fetchJob(jobKey).isPresent()) {
      throw new CronException(formatMessage("No cron job found for %s.", jobKey));
    }
  }

  private void removeJob(IJobKey jobKey, CronJobStore.Mutable jobStore) {
    jobStore.removeJob(jobKey);
    LOG.info(formatMessage("Deleted cron job %s from storage.", jobKey));
  }

  private void saveJob(SanitizedCronJob cronJob, CronJobStore.Mutable jobStore) {
    IJobConfiguration config = cronJob.getSanitizedConfig().getJobConfig();

    jobStore.saveAcceptedJob(config);
    LOG.info(formatMessage("Saved new cron job %s to storage.", config.getKey()));
  }

  // TODO(ksweeney): Consider exposing this in the interface and making caller responsible.
  void scheduleJob(CrontabEntry crontabEntry, IJobKey jobKey) throws CronException {
    try {
      scheduler.scheduleJob(
          Quartz.jobDetail(jobKey, AuroraCronJob.class),
          Quartz.cronTrigger(crontabEntry, timeZone));
    } catch (SchedulerException e) {
      throw new CronException(e);
    }
    LOG.info(formatMessage("Scheduled job %s with schedule %s.", jobKey, crontabEntry));
  }

  @Override
  public boolean deleteJob(final IJobKey jobKey) {
    requireNonNull(jobKey);

    return storage.write(new MutateWork.Quiet<Boolean>() {
      @Override
      public Boolean apply(Storage.MutableStoreProvider storeProvider) {
        if (!storeProvider.getCronJobStore().fetchJob(jobKey).isPresent()) {
          return false;
        }

        removeJob(jobKey, storeProvider.getCronJobStore());
        descheduleJob(jobKey);
        return true;
      }
    });
  }

  private void descheduleJob(IJobKey jobKey) {
    try {
      // TODO(ksweeney): Consider interrupting the running job here.
      // There's a race here where an old running job could fail to find the old config. That's
      // fine given that the behavior of AuroraCronJob is to log an error and exit if it's unable
      // to find a job for its key.
      scheduler.deleteJob(Quartz.jobKey(jobKey));
      LOG.info(formatMessage("Successfully descheduled %s.", jobKey));
    } catch (SchedulerException e) {
      LOG.log(Level.WARNING, formatMessage("Error descheduling %s: %s", jobKey, e), e);
    }
  }

  @Override
  public Map<IJobKey, CrontabEntry> getScheduledJobs() {
    // NOTE: no synchronization is needed here since this is just a dump of internal quartz state
    // for debugging.
    ImmutableMap.Builder<IJobKey, CrontabEntry> scheduledJobs = ImmutableMap.builder();
    try {
      for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.<JobKey>anyGroup())) {
        // The quartz API allows jobs to have multiple triggers. We don't use that feature but
        // we're defensive here since this function is used for debugging.
        Optional<CronTrigger> trigger = FluentIterable.from(scheduler.getTriggersOfJob(jobKey))
            .filter(CronTrigger.class)
            .first();
        if (trigger.isPresent()) {
          scheduledJobs.put(
              Quartz.auroraJobKey(jobKey),
              Quartz.crontabEntry(trigger.get()));
        }
      }
    } catch (SchedulerException e) {
      throw Throwables.propagate(e);
    }
    return scheduledJobs.build();
  }

  private static String formatMessage(String format, IJobKey jobKey, Object... args) {
    return String.format(format, JobKeys.canonicalString(jobKey), args);
  }
}
