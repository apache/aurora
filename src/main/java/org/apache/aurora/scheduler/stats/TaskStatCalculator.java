/**
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
package org.apache.aurora.scheduler.stats;

import javax.inject.Inject;

import org.apache.aurora.scheduler.stats.ResourceCounter.GlobalMetric;
import org.apache.aurora.scheduler.stats.ResourceCounter.Metric;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Calculates and exports aggregate stats about resources consumed by active tasks.
 */
class TaskStatCalculator implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(TaskStatCalculator.class);

  private final CachedCounters counters;
  private final ResourceCounter resourceCounter;

  @Inject
  TaskStatCalculator(ResourceCounter resourceCounter, CachedCounters counters) {
    this.resourceCounter = requireNonNull(resourceCounter);
    this.counters = requireNonNull(counters);
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
      LOG.debug("Unable to fetch metrics, storage is likely not ready.");
    }
  }
}
