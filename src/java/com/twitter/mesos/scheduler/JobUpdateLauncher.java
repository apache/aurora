package com.twitter.mesos.scheduler;

/**
 * Function to initiate a job updater for an existing job.
 *
 * @author wfarner
 */
public interface JobUpdateLauncher {

  void launchUpdater(String updateToken) throws ScheduleException;

  static class JobUpdateLauncherImpl implements JobUpdateLauncher {

    @Override public void launchUpdater(String updateToken) throws ScheduleException {
      // TODO(wfarner): Implement.
      throw new ScheduleException("Job updates requiring restarts not yet implemented.");
    }
  }
}
