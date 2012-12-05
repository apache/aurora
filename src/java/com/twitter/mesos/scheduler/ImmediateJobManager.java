package com.twitter.mesos.scheduler;

import com.twitter.mesos.gen.JobConfiguration;

/**
 * Job scheduler that accepts any job and executes it immediately.
 */
public class ImmediateJobManager extends JobManager {

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
  public boolean hasJob(String role, String job) {
    return !schedulerCore.getTasks(Query.activeQuery(role, job)).isEmpty();
  }
}
