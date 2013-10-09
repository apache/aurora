/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.aurora.scheduler.quota;

import javax.inject.Inject;

import com.google.common.collect.Iterables;

import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.state.JobFilter;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.entities.IJobConfiguration;
import com.twitter.aurora.scheduler.storage.entities.IQuota;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A filter that fails production jobs for roles that do not have sufficient quota to run them.
 */
class QuotaFilter implements JobFilter {
  private final QuotaManager quotaManager;
  private final Storage storage;

  @Inject
  QuotaFilter(QuotaManager quotaManager, Storage storage) {
    this.quotaManager = checkNotNull(quotaManager);
    this.storage = checkNotNull(storage);
  }

  @Override
  public synchronized JobFilterResult filter(final IJobConfiguration job) {
    ITaskConfig template = job.getTaskConfig();
    if (!template.isProduction()) {
      return JobFilterResult.pass();
    }

    IQuota currentUsage = Quotas.fromProductionTasks(
        Iterables.transform(
            Storage.Util.consistentFetchTasks(storage, Query.jobScoped(job.getKey()).active()),
        Tasks.SCHEDULED_TO_INFO));

    IQuota additionalRequested = Quotas.subtract(Quotas.fromJob(job), currentUsage);
    if (!quotaManager.hasRemaining(job.getKey().getRole(), additionalRequested)) {
      return JobFilterResult.fail("Insufficient resource quota.");
    }

    return JobFilterResult.pass();
  }
}
