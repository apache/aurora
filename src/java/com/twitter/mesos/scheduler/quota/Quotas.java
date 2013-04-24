package com.twitter.mesos.scheduler.quota;

import com.google.common.collect.Iterables;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.TwitterTaskInfo;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Convenience class for normalizing resource measures between tasks and offers.
 */
public final class Quotas {

  public static final Quota NO_QUOTA = new Quota(0, 0, 0);

  private Quotas() {
    // Utility class.
  }

  /**
   * Determines the amount of quota required for a job.
   *
   * @param job Job to count quota from.
   * @return Quota requirement to run {@code job}.
   */
  public static Quota fromJob(JobConfiguration job) {
    checkNotNull(job);
    return fromTasks(job.getTaskConfigs());
  }

  /**
   * Determines the amount of quota required for tasks.
   *
   * @param tasks Tasks to count quota from.
   * @return Quota requirement to run {@code tasks}.
   */
  public static Quota fromTasks(Iterable<TwitterTaskInfo> tasks) {
    checkNotNull(tasks);

    double cpu = 0;
    int ramMb = 0;
    int diskMb = 0;
    for (TwitterTaskInfo task : Iterables.filter(tasks, Tasks.IS_PRODUCTION)) {
      cpu += task.getNumCpus();
      ramMb += task.getRamMb();
      diskMb += task.getDiskMb();
    }

    return new Quota()
        .setNumCpus(cpu)
        .setRamMb(ramMb)
        .setDiskMb(diskMb);
  }

  /**
   * a >= b
   */
  public static boolean geq(Quota a, Quota b) {
    return (a.getNumCpus() >= b.getNumCpus())
        && (a.getRamMb() >= b.getRamMb())
        && (a.getDiskMb() >= b.getDiskMb());
  }

  /**
   * a > b
   */
  public static boolean greaterThan(Quota a, Quota b) {
    return (a.getNumCpus() > b.getNumCpus())
        && (a.getRamMb() > b.getRamMb())
        && (a.getDiskMb() > b.getDiskMb());
  }

  /**
   * a + b
   */
  public static Quota add(Quota a, Quota b) {
    return new Quota()
        .setNumCpus(a.getNumCpus() + b.getNumCpus())
        .setRamMb(a.getRamMb() + b.getRamMb())
        .setDiskMb(a.getDiskMb() + b.getDiskMb());
  }

  /**
   * a - b
   */
  public static Quota subtract(Quota a, Quota b) throws IllegalStateException {
    return new Quota()
        .setNumCpus(a.getNumCpus() - b.getNumCpus())
        .setRamMb(a.getRamMb() - b.getRamMb())
        .setDiskMb(a.getDiskMb() - b.getDiskMb());
  }
}
