package com.twitter.aurora.scheduler.stats;

import java.util.logging.Logger;

import javax.inject.Inject;

import com.twitter.aurora.scheduler.stats.AsyncStatsModule.StatUpdater;
import com.twitter.aurora.scheduler.stats.ResourceCounter.GlobalMetric;
import com.twitter.aurora.scheduler.stats.ResourceCounter.Metric;
import com.twitter.aurora.scheduler.storage.Storage.StorageException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Calculates and exports aggregate stats about resources consumed by active tasks.
 */
class TaskStatCalculator implements Runnable {
  private static final Logger LOG = Logger.getLogger(StatUpdater.class.getName());

  private final CachedCounters counters;
  private final ResourceCounter resourceCounter;

  @Inject
  TaskStatCalculator(ResourceCounter resourceCounter, CachedCounters counters) {
    this.resourceCounter = checkNotNull(resourceCounter);
    this.counters = checkNotNull(counters);
  }

  private void update(String prefix, Metric metric) {
    counters.get(prefix + "_cpu").set(metric.getCpu());
    counters.get(prefix + "_ram_gb").set(metric.getRamGb());
    counters.get(prefix + "_disk_gb").set(metric.getDiskGb());
  }

  @Override
  public void run() {
    try {
      for (GlobalMetric metric : resourceCounter.computeConsumptionTotals()) {
        update("resources_" + metric.type.name().toLowerCase(), metric);
      }
      update("resources_allocated_quota", resourceCounter.computeQuotaAllocationTotals());
    } catch (StorageException e) {
      LOG.fine("Unable to fetch metrics, storage is likely not ready.");
    }
  }
}
