package com.twitter.mesos.scheduler.httphandlers;

import com.google.common.collect.Lists;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzHome.User;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzUser.Job;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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

  public static final Comparator<User> SORT_USERS_BY_NAME = new Comparator<User>() {
      @Override public int compare(User userA, User userB) {
        return userA.name.compareTo(userB.name);
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
