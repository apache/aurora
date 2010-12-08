package com.twitter.mesos.scheduler;

import com.twitter.mesos.gen.JobConfiguration;

/**
 * Function to initiate a job updater for an existing job.
 *
 * @author wfarner
 */
public interface JobUpdateLauncher {

  void launchUpdater(JobConfiguration job) throws ScheduleException;

  static class JobUpdateLauncherImpl implements JobUpdateLauncher {

    @Override public void launchUpdater(JobConfiguration job) throws ScheduleException {
      // TODO(wfarner): Implement.
      throw new ScheduleException("Job updates requiring restarts not yet implemented.");
    }
  }
}
