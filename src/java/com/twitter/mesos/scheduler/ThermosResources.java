package com.twitter.mesos.scheduler;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import com.twitter.mesos.Tasks;

import com.twitter.mesos.gen.TwitterTaskInfo;

/**
 * Constants for Thermos executor resources.
 */
public final class ThermosResources {
  /**
   * CPU allocated for each Thermos executor.  TODO(wickman) Consider lowing
   * this number if deemed sufficient.
   */
  public static final double CPUS = 0.25;

  /**
   * RAM required for the Thermos executor.  Thermos executors in the wild
   * have been observed using 48-54MB RSS, setting to 128MB to be extra
   * vigilant initially.
   */
  public static final Amount<Double, Data> RAM = Amount.of(128d, Data.MB);

  private ThermosResources() {
    // Utility class
  }

  /**
   * Return the total CPU consumption of this task including the executor, if it's a Thermos
   * task.
   *
   * @param task TwitterTaskInfo of this task
   */
  public static double getTotalTaskCpus(TwitterTaskInfo task) {
    if (Tasks.IS_THERMOS_TASK.apply(task)) {
      return task.getNumCpus() + CPUS;
    } else {
      return task.getNumCpus();
    }
  }

  /**
   * Return the total RAM consumption of this task including the executor, if it's a Thermos
   * task.
   *
   * @param task TwitterTaskInfo of this task
   */
  public static Amount<Double, Data> getTotalTaskRam(TwitterTaskInfo task) {
    if (Tasks.IS_THERMOS_TASK.apply(task)) {
      return Amount.of(task.getRamMb() + RAM.as(Data.MB), Data.MB);
    } else {
      return Amount.of((double) task.getRamMb(), Data.MB);
    }
  }
}
