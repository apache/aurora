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
package com.twitter.aurora.scheduler.state;

import java.util.Collections;

import com.google.inject.Inject;

import com.twitter.aurora.scheduler.base.ScheduleException;
import com.twitter.aurora.scheduler.configuration.ParsedConfiguration;
import com.twitter.aurora.scheduler.storage.entities.IJobConfiguration;
import com.twitter.aurora.scheduler.storage.entities.IJobKey;

/**
 * Interface for a job manager.  A job manager is responsible for deciding whether and when to
 * trigger execution of a job.
 */
public abstract class JobManager {

  // TODO(Bill Farner): Remove this. It is only used since the CronJobManager and SchedulerCoreImpl
  // have a circular dependency.
  @Inject
  protected SchedulerCore schedulerCore;

  /**
   * Gets a key that uniquely identifies this manager type, to distinguish from other schedulers.
   * These keys end up being persisted, so they must be considered permanently immutable.
   *
   * @return Job manager key.
   */
  public abstract String getUniqueKey();

  /**
   * Submits a job to the manager.  The job may be submitted to the job runner before this method
   * returns or at any point in the future.  This method will return false if the manager will not
   * execute the job.
   *
   * @param config The job to schedule.
   * @return {@code true} If the manager accepted the job, {@code false} otherwise.
   * @throws ScheduleException If there is a problem with scheduling the job.
   */
  public abstract boolean receiveJob(ParsedConfiguration config) throws ScheduleException;

  /**
   * Fetches the configured jobs that this manager is storing.
   *
   * @return Jobs stored by this job manager.
   */
  // TODO(ksweeney): Consider adding a Map<JobKey, JobConfiguration> to complement this.
  public Iterable<IJobConfiguration> getJobs() {
    return Collections.emptyList();
  }

  /**
   * Checks whether this manager is storing a job with the given key.
   *
   * @param jobKey Job key.
   * @return {@code true} if the manager has a matching job, {@code false} otherwise.
   */
  public abstract boolean hasJob(IJobKey jobKey);

  /**
   * Instructs the manager to delete any jobs with the given key.
   *
   * @param jobKey Job key.
   * @return {@code true} if a matching job was deleted.
   */
  public boolean deleteJob(IJobKey jobKey) {
    // Optionally overridden by implementing class.
    return false;
  }
}
