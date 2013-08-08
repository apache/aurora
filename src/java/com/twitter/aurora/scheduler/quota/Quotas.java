package com.twitter.aurora.scheduler.quota;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.scheduler.base.Tasks;

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
    Quota quota = fromTasks(ImmutableSet.of(job.getTaskConfig()));
    quota.setNumCpus(quota.getNumCpus() * job.getShardCount());
    quota.setRamMb(quota.getRamMb() * job.getShardCount());
    quota.setDiskMb(quota.getDiskMb() * job.getShardCount());
    return quota;
  }

  /**
   * Determines the amount of quota required for tasks.
   *
   * @param tasks Tasks to count quota from.
   * @return Quota requirement to run {@code tasks}.
   */
  public static Quota fromTasks(Iterable<TaskConfig> tasks) {
    checkNotNull(tasks);

    double cpu = 0;
    int ramMb = 0;
    int diskMb = 0;
    for (TaskConfig task : Iterables.filter(tasks, Tasks.IS_PRODUCTION)) {
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
