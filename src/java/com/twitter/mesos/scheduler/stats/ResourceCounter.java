package com.twitter.mesos.scheduler.stats;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StorageException;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

/**
 * Computes aggregate metrics about resource allocation and consumption in the scheduler.
 */
public class ResourceCounter {
  private final Storage storage;

  @Inject
  ResourceCounter(Storage storage) {
    this.storage = Preconditions.checkNotNull(storage);
  }

  private Iterable<TwitterTaskInfo> getTasks(Query.Builder query) throws StorageException {
    return Iterables.transform(Storage.Util.fetchTasks(storage, query), Tasks.SCHEDULED_TO_INFO);
  }

  /**
   * Computes totals for each of the {@link MetricType}s.
   *
   * @return aggregates for each global metric type.
   * @throws StorageException if there was a problem fetching tasks from storage.
   */
  public List<GlobalMetric> computeConsumptionTotals() throws StorageException {
    List<GlobalMetric> counts = Arrays.asList(
        new GlobalMetric(MetricType.TOTAL_CONSUMED),
        new GlobalMetric(MetricType.DEDICATED_CONSUMED),
        new GlobalMetric(MetricType.QUOTA_CONSUMED),
        new GlobalMetric(MetricType.FREE_POOL_CONSUMED));

    for (TwitterTaskInfo task : getTasks(Query.unscoped().active())) {
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
    return storage.consistentRead(new Work.Quiet<Metric>() {
      @Override public Metric apply(StoreProvider storeProvider) {
        Metric allocation = new Metric();
        for (Quota quota : storeProvider.getQuotaStore().fetchQuotas().values()) {
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
      Predicate<TwitterTaskInfo> filter,
      Function<TwitterTaskInfo, K> keyFunction) throws StorageException {

    LoadingCache<K, Metric> metrics = CacheBuilder.newBuilder()
        .build(new CacheLoader<K, Metric>() {
          @Override public Metric load(K key) {
            return new Metric();
          }
        });
    for (TwitterTaskInfo task : Iterables.filter(getTasks(query), filter)) {
      metrics.getUnchecked(keyFunction.apply(task)).accumulate(task);
    }
    return metrics.asMap();
  }

  public enum MetricType {
    TOTAL_CONSUMED(Predicates.<TwitterTaskInfo>alwaysTrue()),
    DEDICATED_CONSUMED(new Predicate<TwitterTaskInfo>() {
      @Override public boolean apply(@Nullable TwitterTaskInfo task) {
        return ConfigurationManager.isDedicated(task);
      }
    }),
    QUOTA_CONSUMED(new Predicate<TwitterTaskInfo>() {
      @Override public boolean apply(TwitterTaskInfo task) {
        return task.isProduction();
      }
    }),
    FREE_POOL_CONSUMED(new Predicate<TwitterTaskInfo>() {
      @Override public boolean apply(TwitterTaskInfo task) {
        return !ConfigurationManager.isDedicated(task) && !task.isProduction();
      }
    });

    public final Predicate<TwitterTaskInfo> filter;

    MetricType(Predicate<TwitterTaskInfo> filter) {
      this.filter = filter;
    }
  }

  public static class GlobalMetric extends Metric {
    public final MetricType type;

    public GlobalMetric(MetricType type) {
      this.type = type;
    }

    @Override
    protected void accumulate(TwitterTaskInfo task) {
      if (type.filter.apply(task)) {
        super.accumulate(task);
      }
    }
  }

  public static class Metric {
    private long cpu = 0;
    private long ramMb = 0;
    private long diskMb = 0;

    public Metric() {
      this.cpu = 0;
      this.ramMb = 0;
      this.diskMb = 0;
    }

    public Metric(Metric copy) {
      this.cpu = copy.cpu;
      this.ramMb = copy.ramMb;
      this.diskMb = copy.diskMb;
    }

    protected void accumulate(TwitterTaskInfo task) {
      cpu += task.numCpus;
      ramMb += task.ramMb;
      diskMb += task.diskMb;
    }

    protected void accumulate(Quota quota) {
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
  }
}
