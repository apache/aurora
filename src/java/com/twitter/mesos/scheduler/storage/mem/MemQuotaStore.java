package com.twitter.mesos.scheduler.storage.mem;

import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.scheduler.storage.QuotaStore;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An in-memory quota store.
 */
public class MemQuotaStore implements QuotaStore.Mutable.Transactioned {

  private static final Function<Quota, Quota> DEEP_COPY = Util.deepCopier(Quota.class);

  private final TransactionalMap<String, Quota> quotas =
      TransactionalMap.wrap(Maps.<String, Quota>newHashMap());

  @Override
  public void commit() {
    quotas.commit();
  }

  @Override
  public void rollback() {
    quotas.rollback();
  }

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
  public Set<String> fetchQuotaRoles() {
    return ImmutableSet.copyOf(quotas.keySet());
  }
}
