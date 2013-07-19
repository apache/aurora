package com.twitter.aurora.scheduler.storage.mem;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.storage.JobStore;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An in-memory job store.
 */
class MemJobStore implements JobStore.Mutable {

  private static final Function<JobConfiguration, JobConfiguration> DEEP_COPY = Util.deepCopier();

  private final LoadingCache<String, Manager> managers = CacheBuilder.newBuilder()
      .build(new CacheLoader<String, Manager>() {
        @Override public Manager load(String key) {
          return new Manager();
        }
      });

  @Override
  public void saveAcceptedJob(String managerId, JobConfiguration jobConfig) {
    checkNotNull(managerId);
    checkNotNull(jobConfig);

    JobKey key = JobKeys.assertValid(jobConfig.getKey()).deepCopy();
    managers.getUnchecked(managerId).jobs.put(key, DEEP_COPY.apply(jobConfig));
  }

  @Override
  public void removeJob(JobKey jobKey) {
    checkNotNull(jobKey);

    for (Manager manager : managers.asMap().values()) {
      manager.jobs.remove(jobKey);
    }
  }

  @Override
  public void deleteJobs() {
    managers.invalidateAll();
  }

  @Override
  public Iterable<JobConfiguration> fetchJobs(String managerId) {
    checkNotNull(managerId);

    @Nullable Manager manager = managers.getIfPresent(managerId);
    if (manager == null) {
      return ImmutableSet.of();
    }

    return FluentIterable.from(manager.jobs.values()).transform(DEEP_COPY).toSet();
  }

  @Override
  public Optional<JobConfiguration> fetchJob(String managerId, JobKey jobKey) {
    checkNotNull(managerId);
    checkNotNull(jobKey);

    Optional<Manager> manager = Optional.fromNullable(managers.getIfPresent(managerId));
    if (!manager.isPresent()) {
      return Optional.absent();
    } else {
      return Optional.fromNullable(manager.get().jobs.get(jobKey)).transform(DEEP_COPY);
    }
  }

  @Override
  public Set<String> fetchManagerIds() {
    return ImmutableSet.copyOf(managers.asMap().keySet());
  }

  private static class Manager {
    private final Map<JobKey, JobConfiguration> jobs = Maps.newConcurrentMap();
  }
}
