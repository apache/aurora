package com.twitter.nexus.scheduler;

import com.twitter.nexus.gen.JobConfiguration;

/**
 * Job scheduler that accepts any job and executes it immediately.
 *
 * @author wfarner
 */
public class ImmediateJobScheduler extends JobScheduler {
  @Override
  public boolean receiveJob(JobConfiguration job) {
    jobRunner.execute(job);
    return true;
  }
}
