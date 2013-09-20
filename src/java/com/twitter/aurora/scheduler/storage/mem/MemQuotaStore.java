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

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.scheduler.storage.QuotaStore;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An in-memory quota store.
 */
class MemQuotaStore implements QuotaStore.Mutable {

  private static final Function<Quota, Quota> DEEP_COPY = Util.deepCopier();

  private final Map<String, Quota> quotas = Maps.newConcurrentMap();

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
  public void saveQuota(String role, Quota quota) {
    checkNotNull(role);
    checkNotNull(quota);

    quotas.put(role, DEEP_COPY.apply(quota));
  }

  @Override
  public Optional<Quota> fetchQuota(String role) {
    checkNotNull(role);

    @Nullable Quota quota = quotas.get(role);
    return (quota == null) ? Optional.<Quota>absent() : Optional.of(DEEP_COPY.apply(quota));
  }

  @Override
  public Map<String, Quota> fetchQuotas() {
    return ImmutableMap.copyOf(Maps.transformValues(quotas, DEEP_COPY));
  }
}
