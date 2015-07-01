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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;

import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * Computes aggregate metrics about resource allocation and consumption in the scheduler.
 */
public class ResourceCounter {
  private final Storage storage;

  @Inject
  ResourceCounter(Storage storage) {
    this.storage = Objects.requireNonNull(storage);
  }

  private Iterable<ITaskConfig> getTasks(Query.Builder query) throws StorageException {
    return Iterables.transform(
        Storage.Util.fetchTasks(storage, query),
        Tasks.SCHEDULED_TO_INFO);
  }

  private static final Function<MetricType, GlobalMetric> TO_GLOBAL_METRIC =
      new Function<MetricType, GlobalMetric>() {
        @Override
        public GlobalMetric apply(MetricType type) {
          return new GlobalMetric(type);
        }
      };

  /**
   * Computes totals for each of the {@link MetricType}s.
   *
   * @return aggregates for each global metric type.
   * @throws StorageException if there was a problem fetching tasks from storage.
   */
  public List<GlobalMetric> computeConsumptionTotals() throws StorageException {
    List<GlobalMetric> counts = FluentIterable.from(Arrays.asList(MetricType.values()))
        .transform(TO_GLOBAL_METRIC)
        .toList();

    for (ITaskConfig task : getTasks(Query.unscoped().active())) {
      for (GlobalMetric count : counts) {
        count.accumulate(task);
      }
    }
    return counts;
  }

  /**
   * Computes total quota allocations.
   *
   * @return Total allocated quota.
   * @throws StorageException if there was a problem fetching quotas from storage.
   */
  public Metric computeQuotaAllocationTotals() throws StorageException {
    return storage.read(new Work.Quiet<Metric>() {
      @Override
      public Metric apply(StoreProvider storeProvider) {
        Metric allocation = new Metric();
        for (IResourceAggregate quota : storeProvider.getQuotaStore().fetchQuotas().values()) {
          allocation.accumulate(quota);
        }
        return allocation;
      }
    });
  }

  /**
   * Computes arbitrary resource aggregates based on a query, a filter, and a grouping function.
   *
   * @param query Query to select tasks for aggregation.
   * @param filter Filter to apply on query result tasks.
   * @param keyFunction Function to define aggregation groupings.
   * @param <K> Key type.
   * @return A map from the keys to their aggregates based on the tasks fetched.
   * @throws StorageException if there was a problem fetching tasks from storage.
   */
  public <K> Map<K, Metric> computeAggregates(
      Query.Builder query,
      Predicate<ITaskConfig> filter,
      Function<ITaskConfig, K> keyFunction) throws StorageException {

    LoadingCache<K, Metric> metrics = CacheBuilder.newBuilder()
        .build(new CacheLoader<K, Metric>() {
          @Override
          public Metric load(K key) {
            return new Metric();
          }
        });
    for (ITaskConfig task : Iterables.filter(getTasks(query), filter)) {
      metrics.getUnchecked(keyFunction.apply(task)).accumulate(task);
    }
    return metrics.asMap();
  }

  public enum MetricType {
    TOTAL_CONSUMED(Predicates.alwaysTrue()),
    DEDICATED_CONSUMED(new Predicate<ITaskConfig>() {
      @Override
      public boolean apply(ITaskConfig task) {
        return ConfigurationManager.isDedicated(task.getConstraints());
      }
    }),
    QUOTA_CONSUMED(new Predicate<ITaskConfig>() {
      @Override
      public boolean apply(ITaskConfig task) {
        return task.isProduction();
      }
    }),
    FREE_POOL_CONSUMED(new Predicate<ITaskConfig>() {
      @Override
      public boolean apply(ITaskConfig task) {
        return !ConfigurationManager.isDedicated(task.getConstraints()) && !task.isProduction();
      }
    });

    public final Predicate<ITaskConfig> filter;

    MetricType(Predicate<ITaskConfig> filter) {
      this.filter = filter;
    }
  }

  public static class GlobalMetric extends Metric {
    public final MetricType type;

    public GlobalMetric(MetricType type) {
      this(type, 0, 0, 0);
    }

    @VisibleForTesting
    GlobalMetric(MetricType type, long cpu, long ramMb, long diskMb) {
      super(cpu, ramMb, diskMb);
      this.type = type;
    }

    @Override
    protected void accumulate(ITaskConfig task) {
      if (type.filter.apply(task)) {
        super.accumulate(task);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof GlobalMetric)) {
        return false;
      }

      GlobalMetric other = (GlobalMetric) o;
      return super.equals(other)
          && Objects.equals(other.type, this.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), type);
    }
  }

  public static class Metric {
    private long cpu = 0;
    private long ramMb = 0;
    private long diskMb = 0;

    public Metric() {
      this(0, 0, 0);
    }

    public Metric(Metric copy) {
      this(copy.cpu, copy.ramMb, copy.diskMb);
    }

    @VisibleForTesting
    Metric(long cpu, long ramMb, long diskMb) {
      this.cpu = cpu;
      this.ramMb = ramMb;
      this.diskMb = diskMb;
    }

    protected void accumulate(ITaskConfig task) {
      cpu += task.getNumCpus();
      ramMb += task.getRamMb();
      diskMb += task.getDiskMb();
    }

    protected void accumulate(IResourceAggregate quota) {
      cpu += quota.getNumCpus();
      ramMb += quota.getRamMb();
      diskMb += quota.getDiskMb();
    }

    public long getCpu() {
      return cpu;
    }

    public long getRamGb() {
      return ramMb / 1024;
    }

    public long getDiskGb() {
      return diskMb / 1024;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Metric)) {
        return false;
      }

      Metric other = (Metric) o;
      return getCpu() == other.getCpu()
          && getRamGb() == other.getRamGb()
          && getDiskGb() == other.getDiskGb();
    }

    @Override
    public int hashCode() {
      return Objects.hash(cpu, ramMb, diskMb);
    }
  }
}
