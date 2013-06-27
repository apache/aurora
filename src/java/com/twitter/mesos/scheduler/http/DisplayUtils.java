package com.twitter.mesos.scheduler.http;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.scheduler.MesosTaskFactory.MesosTaskFactoryImpl;
import com.twitter.mesos.scheduler.http.SchedulerzHome.Role;
import com.twitter.mesos.scheduler.http.SchedulerzRole.Job;

/**
 * Utility class to hold common display helper functions.
 */
public final class DisplayUtils {

  @CmdLine(name = "viz_job_url_prefix", help = "URL prefix for job container stats.")
  private static final Arg<String> VIZ_JOB_URL_PREFIX = Arg.create("");

  private DisplayUtils() {
    // Utility class.
  }

  static final Ordering<Role> ROLE_ORDERING = Ordering.natural().onResultOf(
      new Function<Role, String>() {
        @Override public String apply(Role role) {
          return role.getRole();
        }
      });

  static final Ordering<Job> JOB_ORDERING = Ordering.natural().onResultOf(
      new Function<Job, String>() {
        @Override public String apply(Job job) {
          return job.getName();
        }
      });

  static final Ordering<JobConfiguration> JOB_CONFIG_ORDERING = Ordering.natural().onResultOf(
      new Function<JobConfiguration, String>() {
        @Override public String apply(JobConfiguration job) {
          return job.getName();
        }
      });

  static String getJobDashboardUrl(String role, String env, String jobName) {
    return VIZ_JOB_URL_PREFIX.get() + MesosTaskFactoryImpl.getJobSourceName(role, env, jobName);
  }
}
