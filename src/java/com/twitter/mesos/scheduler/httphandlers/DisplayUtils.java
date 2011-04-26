package com.twitter.mesos.scheduler.httphandlers;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;

import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzHome.Role;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzRole.Job;

/**
 * Utility class to hold common display helper functions.
 *
 * @author William Farner
 */
public class DisplayUtils {

  public static <T> List<T> sort(Iterable<T> values, Comparator<T> comparator) {
    List<T> copy = Lists.newArrayList(values);
    Collections.sort(copy, comparator);
    return copy;
  }

  public static final Comparator<Role> SORT_USERS_BY_NAME = new Comparator<Role>() {
      @Override public int compare(Role roleA, Role roleB) {
        return roleA.role.compareTo(roleB.role);
      }
    };

  public static final Comparator<Job> SORT_JOB_BY_NAME = new Comparator<Job>() {
      @Override public int compare(Job jobA, Job jobB) {
        return jobA.getName().compareTo(jobB.getName());
      }
    };

  public static final Comparator<JobConfiguration> SORT_JOB_CONFIG_BY_NAME =
      new Comparator<JobConfiguration>() {
        @Override public int compare(JobConfiguration jobA, JobConfiguration jobB) {
          return jobA.getName().compareTo(jobB.getName());
        }
      };

  private DisplayUtils() {
    // Utility class.
  }
}
