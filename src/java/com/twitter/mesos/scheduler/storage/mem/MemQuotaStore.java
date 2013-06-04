package com.twitter.mesos.scheduler.storage.mem;

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.scheduler.storage.QuotaStore;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An in-memory quota store.
 */
public class MemQuotaStore implements QuotaStore.Mutable {

  private static final Function<Quota, Quota> DEEP_COPY = Util.deepCopier();

  private final Map<String, Quota> quotas = Maps.newHashMap();

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
