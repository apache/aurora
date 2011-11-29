package com.twitter.mesos.scheduler;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Maps;

import com.twitter.common.collections.Pair;
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
    // Maps from task ID to assigned host name.
    final Map<String, String> taskHosts = Maps.newHashMap();
    final Vars vars = new Vars();
  }

  static final class Vars {
    private final Cache<Pair<String, ScheduleStatus>, AtomicLong> countersByJobKeyAndStatus =
        CacheBuilder.newBuilder().build(
            new CacheLoader<Pair<String, ScheduleStatus>, AtomicLong>() {
              @Override public AtomicLong load(Pair<String, ScheduleStatus> jobAndStatus) {
                String jobKey = jobAndStatus.getFirst();
                ScheduleStatus status = jobAndStatus.getSecond();
                return Stats.exportLong("job_" + jobKey + "_tasks_" + status.name());
              }
            });

    private final Cache<ScheduleStatus, AtomicLong> countersByStatus =
        CacheBuilder.newBuilder().build(new CacheLoader<ScheduleStatus, AtomicLong>() {
          @Override public AtomicLong load(ScheduleStatus status) {
            return Stats.exportLong("task_store_" + status);
          }
        });

    Vars() {
      // Initialize by-status counters.
      for (ScheduleStatus status : ScheduleStatus.values()) {
        countersByStatus.getUnchecked(status);
      }
    }

    public void incrementCount(String jobKey, ScheduleStatus status) {
      countersByStatus.getUnchecked(status).incrementAndGet();
      getCounter(jobKey, status).incrementAndGet();
    }

    public void decrementCount(String jobKey, ScheduleStatus status) {
      countersByStatus.getUnchecked(status).decrementAndGet();
      getCounter(jobKey, status).decrementAndGet();
    }

    public void adjustCount(String jobKey, ScheduleStatus oldStatus, ScheduleStatus newStatus) {
      decrementCount(jobKey, oldStatus);
      incrementCount(jobKey, newStatus);
    }

    public AtomicLong getCounter(String jobKey, ScheduleStatus status) {
      return countersByJobKeyAndStatus.getUnchecked(Pair.of(jobKey, status));
    }
  }
}
