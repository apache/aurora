package com.twitter.mesos.scheduler.storage.mem;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.scheduler.storage.JobStore;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An in-memory job store.
 */
public class MemJobStore implements JobStore.Mutable {

  private static final Function<JobConfiguration, JobConfiguration> DEEP_COPY =
      Util.deepCopier();

  private final Map<String, Manager> managers = Maps.newHashMap();

  private Manager getOrCreate(String managerId) {
    Manager manager = managers.get(managerId);
    if (manager == null) {
      manager = new Manager();
      managers.put(managerId, manager);
    }
    return manager;
  }

  @Override
  public void saveAcceptedJob(String managerId, JobConfiguration jobConfig) {
    checkNotNull(managerId);
    checkNotNull(jobConfig);

    getOrCreate(managerId).jobs.put(Tasks.jobKey(jobConfig), DEEP_COPY.apply(jobConfig));
  }

  @Override
  public void removeJob(String jobKey) {
    checkNotNull(jobKey);

    for (Manager manager : managers.values()) {
      manager.jobs.remove(jobKey);
    }
  }

  @Override
  public void deleteJobs() {
    managers.clear();
  }

  @Override
  public Iterable<JobConfiguration> fetchJobs(String managerId) {
    checkNotNull(managerId);

    @Nullable Manager manager = managers.get(managerId);
    if (manager == null) {
      return ImmutableSet.of();
    }

    return FluentIterable.from(manager.jobs.values())
        .transform(DEEP_COPY)
        .toSet();
  }

  @Nullable
  @Override
  public JobConfiguration fetchJob(String managerId, String jobKey) {
    checkNotNull(managerId);
    checkNotNull(jobKey);

    @Nullable Manager manager = managers.get(managerId);
    if (manager == null) {
      return null;
    }

    return DEEP_COPY.apply(manager.jobs.get(jobKey));
  }

  @Override
  public Set<String> fetchManagerIds() {
    return ImmutableSet.copyOf(managers.keySet());
  }

  private static class Manager {
    private final Map<String, JobConfiguration> jobs = Maps.newHashMap();
  }
}
