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
package org.apache.aurora.scheduler.storage.mem;

import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;

/**
 * An in-memory cron job store.
 */
class MemCronJobStore implements CronJobStore.Mutable {
  @VisibleForTesting
  static final String CRON_JOBS_SIZE = "mem_storage_cron_size";

  private final Map<IJobKey, IJobConfiguration> jobs = Maps.newConcurrentMap();

  @Inject
  MemCronJobStore(StatsProvider statsProvider) {
    statsProvider.makeGauge(CRON_JOBS_SIZE, () -> jobs.size());
  }

  @Timed("mem_storage_cron_save_accepted_job")
  @Override
  public void saveAcceptedJob(IJobConfiguration jobConfig) {
    IJobKey key = JobKeys.assertValid(jobConfig.getKey());
    jobs.put(key, jobConfig);
  }

  @Timed("mem_storage_cron_remove_job")
  @Override
  public void removeJob(IJobKey jobKey) {
    jobs.remove(jobKey);
  }

  @Timed("mem_storage_cron_delete_jobs")
  @Override
  public void deleteJobs() {
    jobs.clear();
  }

  @Timed("mem_storage_cron_fetch_jobs")
  @Override
  public Iterable<IJobConfiguration> fetchJobs() {
    return ImmutableSet.copyOf(jobs.values());
  }

  @Timed("mem_storage_cron_fetch_job")
  @Override
  public Optional<IJobConfiguration> fetchJob(IJobKey jobKey) {
    return Optional.ofNullable(jobs.get(jobKey));
  }
}
