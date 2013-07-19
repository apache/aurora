package com.twitter.aurora.scheduler.state;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.storage.Storage;

/**
 * Job scheduler that accepts any job and executes it immediately.
 */
class ImmediateJobManager extends JobManager {

  private final Storage storage;

  @Inject
  ImmediateJobManager(Storage storage) {
    this.storage = Preconditions.checkNotNull(storage);
  }

  @Override
  public String getUniqueKey() {
    return "IMMEDIATE";
  }

  @Override
  public boolean receiveJob(JobConfiguration job) {
    schedulerCore.runJob(job);
    return true;
  }

  @Override
  public boolean hasJob(final JobKey jobKey) {
    return !Storage.Util.consistentFetchTasks(storage, Query.jobScoped(jobKey).active()).isEmpty();
  }
}
