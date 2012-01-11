package com.twitter.mesos.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.twitter.common.collections.Pair;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.gen.ScheduleStatus;

/**
 * StateManager variables.
 */
class StateManagerVars {

  /**
   * Mutable state of the StateManager.
   */
  static class MutableState {
    final Vars vars = new Vars();
  }

  /**
   * Custom stat class so that we can delay stat export.  This allows us to prevent
   * false values reported while the database is being loaded.
   */
  private static class Var extends StatImpl<Long> {
    private volatile long value = 0;

    public Var(String name) {
      super(name);
    }

    @Override public Long read() {
      return value;
    }

    void increment() {
      value++;
    }

    void decrement() {
      value--;
    }
  }

  static final class Vars {
    private volatile boolean exporting = false;
    private final Function<Var, Var> maybeExport = new Function<Var, Var>() {
      @Override public Var apply(Var var) {
        if (exporting) {
          Stats.export(var);
        }
        return var;
      }
    };

    private final LoadingCache<Pair<String, ScheduleStatus>, Var> varsByJobKeyAndStatus =
        CacheBuilder.newBuilder().build(
            CacheLoader.from(Functions.compose(maybeExport,
                new Function<Pair<String, ScheduleStatus>, Var>() {
                  @Override public Var apply(Pair<String, ScheduleStatus> jobAndStatus) {
                    String jobKey = jobAndStatus.getFirst();
                    ScheduleStatus status = jobAndStatus.getSecond();
                    return new Var(getVarName(jobKey, status));
                  }
                })));

    private final LoadingCache<ScheduleStatus, Var> varsByStatus = CacheBuilder.newBuilder().build(
        CacheLoader.from(Functions.compose(maybeExport,
            new Function<ScheduleStatus, Var>() {
              @Override public Var apply(ScheduleStatus status) {
                return new Var(getVarName(status));
              }
            })));

    Vars() {
      // Initialize by-status counters.
      for (ScheduleStatus status : ScheduleStatus.values()) {
        varsByStatus.getUnchecked(status);
      }
    }

    @VisibleForTesting
    String getVarName(String jobKey, ScheduleStatus status) {
      return "job_" + jobKey + "_tasks_" + status.name();
    }

    @VisibleForTesting
    String getVarName(ScheduleStatus status) {
      return "task_store_" + status;
    }

    public void incrementCount(String jobKey, ScheduleStatus status) {
      varsByStatus.getUnchecked(status).increment();
      varsByJobKeyAndStatus.getUnchecked(Pair.of(jobKey, status)).increment();
    }

    public void decrementCount(String jobKey, ScheduleStatus status) {
      varsByStatus.getUnchecked(status).decrement();
      varsByJobKeyAndStatus.getUnchecked(Pair.of(jobKey, status)).decrement();
    }

    public void adjustCount(String jobKey, ScheduleStatus oldStatus, ScheduleStatus newStatus) {
      decrementCount(jobKey, oldStatus);
      incrementCount(jobKey, newStatus);
    }

    public void beginExporting() {
      Preconditions.checkState(!exporting, "Exporting has already started.");
      exporting = true;
      for (Var var : varsByStatus.asMap().values()) {
        Stats.export(var);
      }
      for (Var var : varsByJobKeyAndStatus.asMap().values()) {
        Stats.export(var);
      }
    }
  }
}
