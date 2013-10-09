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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.twitter.aurora.scheduler.storage.QuotaStore;
import com.twitter.aurora.scheduler.storage.entities.IQuota;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An in-memory quota store.
 */
class MemQuotaStore implements QuotaStore.Mutable {

  private final Map<String, IQuota> quotas = Maps.newConcurrentMap();

  @Override
  public void deleteQuotas() {
    quotas.clear();
  }

  @Override
  public void removeQuota(String role) {
    checkNotNull(role);

    quotas.remove(role);
  }

  @Override
  public void saveQuota(String role, IQuota quota) {
    checkNotNull(role);
    checkNotNull(quota);

    quotas.put(role, quota);
  }

  @Override
  public Optional<IQuota> fetchQuota(String role) {
    checkNotNull(role);
    return Optional.fromNullable(quotas.get(role));
  }

  @Override
  public Map<String, IQuota> fetchQuotas() {
    return ImmutableMap.copyOf(quotas);
  }
}
