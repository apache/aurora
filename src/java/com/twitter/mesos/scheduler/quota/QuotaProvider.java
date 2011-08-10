package com.twitter.mesos.scheduler.quota;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import com.twitter.common.base.MorePreconditions;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.StorageRole;
import com.twitter.mesos.scheduler.storage.StorageRole.Role;

/**
 * A read-only interface for fetching quotas.
 *
 * @author William Farner
 */
public interface QuotaProvider {

  /**
   * Fetches the quota associated with a role.
   *
   * @param role Role to fetch quotas for.
   * @return Resource quota associated with {@code role}.
   */
  Quota getQuota(String role);

  /**
   * Quota provider that stores quotas in the canonical {@link Storage} system.
   */
  public static class QuotaProviderImpl implements QuotaProvider {

    private final Storage storage;

    @Inject
    public QuotaProviderImpl(@StorageRole(Role.Primary) Storage storage) {
      this.storage = Preconditions.checkNotNull(storage);
    }

    @Override
    public Quota getQuota(final String role) {
      MorePreconditions.checkNotBlank(role);

      Quota quota = storage.doInTransaction(new Work.Quiet<Quota>() {
        @Override public Quota apply(StoreProvider storeProvider) {
          return storeProvider.getQuotaStore().fetchQuota(role);
        }
      });

      // If this user doesn't have a quota record, return non-null empty quota.
      return quota == null ? new Quota() : quota;
    }
  }
}
