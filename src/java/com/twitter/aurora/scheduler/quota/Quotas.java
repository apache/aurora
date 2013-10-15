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
package com.twitter.aurora.scheduler.quota;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.storage.entities.IJobConfiguration;
import com.twitter.aurora.scheduler.storage.entities.IQuota;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Convenience class for normalizing resource measures between tasks and offers.
 */
public final class Quotas {
  private static final IQuota NO_QUOTA = IQuota.build(new Quota(0, 0, 0));

  private Quotas() {
    // Utility class.
  }

  /**
   * Returns a quota with all resource vectors zeroed.
   *
   * @return A quota with all resource vectors zeroed.
   */
  public static IQuota noQuota() {
    return NO_QUOTA;
  }

  /**
   * Determines the amount of quota required for a job.
   *
   * @param job Job to count quota from.
   * @return Quota requirement to run {@code job}.
   */
  public static IQuota fromJob(IJobConfiguration job) {
    return scale(fromProductionTasks(ImmutableSet.of(job.getTaskConfig())), job.getInstanceCount());
  }

  // TODO(Suman Karumuri): Refactor this function in to a new class.
  // TODO(Suman Karumuri): Rename Quota to something more meaningful (ex: ResourceAggregate)
  /**
   * Determines the amount of quota required for production tasks among {@code tasks}.
   *
   * @param tasks Tasks to count quota from.
   * @return Quota requirement to run {@code tasks}.
   */
  public static IQuota fromProductionTasks(Iterable<ITaskConfig> tasks) {
    checkNotNull(tasks);

    return fromTasks(Iterables.filter(tasks, Tasks.IS_PRODUCTION));
  }

  /**
   * Determines the amount of quota required for the given tasks.
   *
   * @param tasks Tasks to count quota from.
   * @return Quota requirement to run {@code tasks}.
   */
  public static IQuota fromTasks(Iterable<ITaskConfig> tasks) {
    double cpu = 0;
    int ramMb = 0;
    int diskMb = 0;
    for (ITaskConfig task : tasks) {
      cpu += task.getNumCpus();
      ramMb += task.getRamMb();
      diskMb += task.getDiskMb();
    }

    return IQuota.build(new Quota()
        .setNumCpus(cpu)
        .setRamMb(ramMb)
        .setDiskMb(diskMb));
  }

  /**
   * a >= b
   */
  public static boolean geq(IQuota a, IQuota b) {
    return (a.getNumCpus() >= b.getNumCpus())
        && (a.getRamMb() >= b.getRamMb())
        && (a.getDiskMb() >= b.getDiskMb());
  }

  /**
   * a > b
   */
  public static boolean greaterThan(IQuota a, IQuota b) {
    return (a.getNumCpus() > b.getNumCpus())
        && (a.getRamMb() > b.getRamMb())
        && (a.getDiskMb() > b.getDiskMb());
  }

  /**
   * a + b
   */
  public static IQuota add(IQuota a, IQuota b) {
    return IQuota.build(new Quota()
        .setNumCpus(a.getNumCpus() + b.getNumCpus())
        .setRamMb(a.getRamMb() + b.getRamMb())
        .setDiskMb(a.getDiskMb() + b.getDiskMb()));
  }

  /**
   * a - b
   */
  public static IQuota subtract(IQuota a, IQuota b) {
    return IQuota.build(new Quota()
        .setNumCpus(a.getNumCpus() - b.getNumCpus())
        .setRamMb(a.getRamMb() - b.getRamMb())
        .setDiskMb(a.getDiskMb() - b.getDiskMb()));
  }

  /**
   * a * m
   */
  public static IQuota scale(IQuota a, int m) {
    return IQuota.build(new Quota()
        .setNumCpus(a.getNumCpus() * m)
        .setRamMb(a.getRamMb() * m)
        .setDiskMb(a.getDiskMb() * m));
  }

  /**
   * a / b
   * <p>
   * This calculates how many times {@code b} "fits into" {@code a}.  Behavior is undefined when
   * {@code b} contains resources with a value of zero.
   */
  public static int divide(IQuota a, IQuota b) {
    return Ordering.natural().min(
        a.getNumCpus() / b.getNumCpus(),
        (double) a.getRamMb() / b.getRamMb(),
        (double) a.getDiskMb() / b.getDiskMb()
    ).intValue();
  }

  /**
   * sum(qs)
   */
  public static IQuota sum(Iterable<IQuota> qs) {
    IQuota sum = noQuota();
    for (IQuota q : qs) {
      sum = Quotas.add(sum, q);
    }
    return sum;
  }
}
