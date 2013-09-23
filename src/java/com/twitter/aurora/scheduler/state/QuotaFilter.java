package com.twitter.aurora.scheduler.state;

import javax.inject.Inject;

import com.google.common.collect.Iterables;

import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.quota.QuotaManager;
import com.twitter.aurora.scheduler.quota.Quotas;
import com.twitter.aurora.scheduler.storage.Storage;

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
  public synchronized JobFilterResult filter(final JobConfiguration job) {
    TaskConfig template = job.getTaskConfig();
    if (!template.isProduction()) {
      return JobFilterResult.pass();
    }

    Quota currentUsage = Quotas.fromProductionTasks(
        Iterables.transform(
            Storage.Util.consistentFetchTasks(storage, Query.jobScoped(job.getKey()).active()),
        Tasks.SCHEDULED_TO_INFO));

    Quota additionalRequested = Quotas.subtract(Quotas.fromJob(job), currentUsage);
    if (!quotaManager.hasRemaining(job.getKey().getRole(), additionalRequested)) {
      return JobFilterResult.fail("Insufficient resource quota.");
    }

    return JobFilterResult.pass();
  }
}
