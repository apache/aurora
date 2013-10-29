/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.aurora.scheduler.storage.mem;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.twitter.aurora.gen.JobUpdateConfiguration;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.storage.UpdateStore;
import com.twitter.aurora.scheduler.storage.entities.IJobKey;
import com.twitter.aurora.scheduler.storage.entities.ILock;
import com.twitter.aurora.scheduler.storage.entities.ILockKey;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An in-memory update store.
 */
class MemUpdateStore implements UpdateStore.Mutable {

  private static final Function<JobUpdateConfiguration, JobUpdateConfiguration> DEEP_COPY_CONFIG =
      Util.deepCopier();

  private final Map<IJobKey, JobUpdateConfiguration> configs = Maps.newConcurrentMap();
  private final Map<ILockKey, ILock> locks = Maps.newConcurrentMap();

  private IJobKey key(IJobKey jobKey) {
    return JobKeys.assertValid(jobKey);
  }

  private IJobKey key(JobUpdateConfiguration config) {
    checkNotNull(config);

    return key(IJobKey.build(config.getJobKey()));
  }

  @Override
  public void saveJobUpdateConfig(JobUpdateConfiguration config) {
    configs.put(key(config), DEEP_COPY_CONFIG.apply(config));
  }

  @Override
  public void removeShardUpdateConfigs(IJobKey jobKey) {
    configs.remove(jobKey);
  }

  @Override
  public void deleteShardUpdateConfigs() {
    configs.clear();
  }

  @Override
  public void saveLock(ILock lock) {
    locks.put(lock.getKey(), lock);
  }

  @Override
  public void removeLock(ILockKey lockKey) {
    locks.remove(lockKey);
  }

  @Override
  public void deleteLocks() {
    locks.clear();
  }

  @Override
  public Optional<JobUpdateConfiguration> fetchJobUpdateConfig(IJobKey jobKey) {
    return Optional.fromNullable(configs.get(key(jobKey)))
        .transform(DEEP_COPY_CONFIG);
  }

  @Override
  public Set<JobUpdateConfiguration> fetchUpdateConfigs(String role) {
    return FluentIterable.from(configs.values())
        .filter(hasRole(role))
        .transform(DEEP_COPY_CONFIG)
        .toSet();
  }

  @Override
  public Set<String> fetchUpdatingRoles() {
    return FluentIterable.from(configs.values())
        .transform(GET_ROLE)
        .toSet();
  }

  @Override
  public Set<ILock> fetchLocks() {
    return ImmutableSet.copyOf(locks.values());
  }

  @Override
  public Optional<ILock> fetchLock(ILockKey lockKey) {
    return Optional.fromNullable(locks.get(lockKey));
  }

  private static final Function<JobUpdateConfiguration, String> GET_ROLE =
      new Function<JobUpdateConfiguration, String>() {
        @Override public String apply(JobUpdateConfiguration config) {
          return config.getJobKey().getRole();
        }
      };

  private static Predicate<JobUpdateConfiguration> hasRole(String role) {
    checkNotNull(role);

    return Predicates.compose(Predicates.equalTo(role), GET_ROLE);
  }
}
