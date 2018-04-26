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
import java.util.stream.Collectors;

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
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static org.apache.aurora.scheduler.quota.QuotaManager.DEDICATED;
import static org.apache.aurora.scheduler.quota.QuotaManager.NON_PROD_SHARED;
import static org.apache.aurora.scheduler.quota.QuotaManager.PROD_SHARED;
import static org.apache.aurora.scheduler.quota.QuotaManager.QUOTA_RESOURCES;

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
        Tasks::getConfig);
  }

  private static final Function<MetricType, Metric> TO_METRIC = Metric::new;

  /**
   * Computes totals for each of the {@link MetricType}s.
   *
   * @return aggregates for each metric type.
   * @throws StorageException if there was a problem fetching tasks from storage.
   */
  public List<Metric> computeConsumptionTotals() throws StorageException {
    List<Metric> counts = FluentIterable.from(Arrays.asList(MetricType.values()))
        .transform(TO_METRIC)
        .toList();

    for (ITaskConfig task : getTasks(Query.unscoped().active())) {
      for (Metric count : counts) {
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
    return storage.read(storeProvider -> {
      Metric allocation = new Metric();
      storeProvider.getQuotaStore().fetchQuotas().values().forEach(allocation::accumulate);
      return allocation;
    });
  }

  /**
   * Returns quota allocations by role (as metrics).
   *
   * @return A map of role to allocated quota metrics.
   * @throws StorageException if there was a problem fetching quotas from storage.
   */
  public Map<String, Metric> computeQuotaAllocationByRole() throws StorageException {
    return storage.read(storeProvider -> {
      return storeProvider.getQuotaStore().fetchQuotas().entrySet().stream()
          .collect(Collectors.toMap(e -> e.getKey(), e -> {
            Metric allocation = new Metric();
            allocation.accumulate(e.getValue());
            return allocation;
          }));
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
    TOTAL_CONSUMED(Predicates.<ITaskConfig>alwaysTrue()),
    DEDICATED_CONSUMED(DEDICATED),
    QUOTA_CONSUMED(PROD_SHARED),
    FREE_POOL_CONSUMED(NON_PROD_SHARED);

    public final Predicate<ITaskConfig> filter;

    MetricType(Predicate<ITaskConfig> filter) {
      this.filter = filter;
    }
  }

  public static class Metric {
    public final MetricType type;
    private ResourceBag bag;

    public Metric() {
      this(MetricType.TOTAL_CONSUMED);
    }

    public Metric(MetricType type) {
      this(type, ResourceBag.EMPTY);
    }

    public Metric(Metric copy) {
      this(copy.type, copy.bag);
    }

    @VisibleForTesting
    Metric(MetricType type, ResourceBag bag) {
      this.type = type;
      this.bag = bag;
    }

    void accumulate(ITaskConfig task) {
      if (type.filter.apply(task)) {
        bag = bag.add(QUOTA_RESOURCES.apply(task));
      }
    }

    void accumulate(IResourceAggregate aggregate) {
      bag = bag.add(ResourceManager.bagFromAggregate(aggregate));
    }

    public ResourceBag getBag() {
      return bag;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Metric)) {
        return false;
      }

      Metric other = (Metric) o;
      return Objects.equals(other.type, this.type)
          && Objects.equals(other.bag, this.bag);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, bag);
    }
  }
}
