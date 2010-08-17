package com.twitter.mesos.scheduler;

import com.twitter.mesos.gen.JobConfiguration;

/**
 * Job scheduler that accepts any job and executes it immediately.
 *
 * @author wfarner
 */
public class ImmediateJobManager extends JobManager {
  @Override
  public boolean receiveJob(JobConfiguration job) {
    schedulerCore.runJob(job);
    return true;
  }
}
