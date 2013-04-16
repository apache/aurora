package com.twitter.mesos.scheduler;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.scheduler.storage.Storage;

/**
 * Job scheduler that accepts any job and executes it immediately.
 */
public class ImmediateJobManager extends JobManager {

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
  public boolean hasJob(final String role, final String job) {
    return !Storage.Util.fetchTasks(storage, Query.jobScoped(role, job).active()).isEmpty();
  }
}
