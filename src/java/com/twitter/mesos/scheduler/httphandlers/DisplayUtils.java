package com.twitter.mesos.scheduler.http;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;

import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.scheduler.http.SchedulerzHome.Role;
import com.twitter.mesos.scheduler.http.SchedulerzRole.Job;

/**
 * Utility class to hold common display helper functions.
 */
public final class DisplayUtils {

  public static final Ordering<Role> ROLE_ORDERING = Ordering.natural().onResultOf(
      new Function<Role, String>() {
        @Override public String apply(Role role) {
          return role.getRole();
        }
      });

  public static final Ordering<Job> JOB_ORDERING = Ordering.natural().onResultOf(
      new Function<Job, String>() {
        @Override public String apply(Job job) {
          return job.getName();
        }
      });

  public static final Ordering<JobConfiguration> JOB_CONFIG_ORDERING = Ordering.natural()
      .onResultOf(new Function<JobConfiguration, String>() {
        @Override public String apply(JobConfiguration job) {
          return job.getName();
        }
      });

  private DisplayUtils() {
    // Utility class.
  }
}
