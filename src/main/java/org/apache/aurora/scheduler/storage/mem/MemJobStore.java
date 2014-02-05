/**
 * Copyright 2013 Apache Software Foundation
 *
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
package org.apache.aurora.scheduler.storage.mem;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.JobStore;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An in-memory job store.
 */
class MemJobStore implements JobStore.Mutable {

  private final LoadingCache<String, Manager> managers = CacheBuilder.newBuilder()
      .build(new CacheLoader<String, Manager>() {
        @Override
        public Manager load(String key) {
          return new Manager();
        }
      });

  @Override
  public void saveAcceptedJob(String managerId, IJobConfiguration jobConfig) {
    checkNotNull(managerId);
    checkNotNull(jobConfig);

    IJobKey key = JobKeys.assertValid(jobConfig.getKey());
    managers.getUnchecked(managerId).jobs.put(key, jobConfig);
  }

  @Override
  public void removeJob(IJobKey jobKey) {
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
  public Iterable<IJobConfiguration> fetchJobs(String managerId) {
    checkNotNull(managerId);

    @Nullable Manager manager = managers.getIfPresent(managerId);
    if (manager == null) {
      return ImmutableSet.of();
    }

    synchronized (manager.jobs) {
      return ImmutableSet.copyOf(manager.jobs.values());
    }
  }

  @Override
  public Optional<IJobConfiguration> fetchJob(String managerId, IJobKey jobKey) {
    checkNotNull(managerId);
    checkNotNull(jobKey);

    Optional<Manager> manager = Optional.fromNullable(managers.getIfPresent(managerId));
    if (!manager.isPresent()) {
      return Optional.absent();
    } else {
      return Optional.fromNullable(manager.get().jobs.get(jobKey));
    }
  }

  @Override
  public Set<String> fetchManagerIds() {
    return ImmutableSet.copyOf(managers.asMap().keySet());
  }

  private static class Manager {
    private final Map<IJobKey, IJobConfiguration> jobs = Maps.newConcurrentMap();
  }
}
