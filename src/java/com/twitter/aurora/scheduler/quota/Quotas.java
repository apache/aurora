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

import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.storage.entities.IJobConfiguration;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Convenience class for normalizing resource measures between tasks and offers.
 */
public final class Quotas {
  private static final Quota NO_QUOTA = new Quota(0, 0, 0);

  private Quotas() {
    // Utility class.
  }

  /**
   * Returns a quota with all resource vectors zeroed.
   *
   * @return A quota with all resource vectors zeroed.
   */
  public static Quota noQuota() {
    return NO_QUOTA.deepCopy();
  }

  /**
   * Determines the amount of quota required for a job.
   *
   * @param job Job to count quota from.
   * @return Quota requirement to run {@code job}.
   */
  public static Quota fromJob(IJobConfiguration job) {
    checkNotNull(job);
    Quota quota = fromProductionTasks(ImmutableSet.of(job.getTaskConfig()));
    quota.setNumCpus(quota.getNumCpus() * job.getShardCount());
    quota.setRamMb(quota.getRamMb() * job.getShardCount());
    quota.setDiskMb(quota.getDiskMb() * job.getShardCount());
    return quota;
  }

  // TODO(Suman Karumuri): Refactor this function in to a new class.
  // TODO(Suman Karumuri): Rename Quota to something more meaningful (ex: ResourceAggregate)
  /**
   * Determines the amount of quota required for production tasks among {@code tasks}.
   *
   * @param tasks Tasks to count quota from.
   * @return Quota requirement to run {@code tasks}.
   */
  public static Quota fromProductionTasks(Iterable<ITaskConfig> tasks) {
    checkNotNull(tasks);

    return fromTasks(Iterables.filter(tasks, Tasks.IS_PRODUCTION));
  }

  /**
   * Determines the amount of quota required for the given tasks.
   *
   * @param tasks Tasks to count quota from.
   * @return Quota requirement to run {@code tasks}.
   */
  public static Quota fromTasks(Iterable<ITaskConfig> tasks) {
    double cpu = 0;
    int ramMb = 0;
    int diskMb = 0;
    for (ITaskConfig task : tasks) {
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
