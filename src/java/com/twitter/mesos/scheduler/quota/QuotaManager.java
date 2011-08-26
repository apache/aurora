package com.twitter.mesos.scheduler.quota;

import java.util.Collection;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.Shards;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.StorageRole;
import com.twitter.mesos.scheduler.storage.StorageRole.Role;
import com.twitter.mesos.scheduler.storage.UpdateStore.ShardUpdateConfiguration;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * Allows access to resource quotas, and tracks quota consumption.
 *
 * @author William Farner
 */
public interface QuotaManager {

  /**
   * Fetches the quota associated with a role.
   *
   * @param role Role to fetch quotas for.
   * @return Resource quota associated with {@code role}.
   */
  Quota getQuota(String role);

  /**
   * Fetches the current resource usage for the role.
   *
   * @param role to fetch quota usage for.
   * @return Resource quota used by {@code role}.
   */
  Quota getConsumption(String role);

  /**
   * Assigns quota to a user, overwriting any previously-assigned quota.
   *
   * @param role Role to assign quota for.
   * @param quota Quota to allocate for the role.
   */
  void setQuota(String role, Quota quota);

  /**
   * Tests whether the role has at least the specified amount of quota available.
   *
   * @param role Role to consume quota for.
   * @param quota Quota amount to check for availability.
   * @return {@code true} if the role currently has at least {@code quota} quota remaining,
   *     {@code false} otherwise.
   */
  boolean hasRemaining(String role, Quota quota);

  /**
   * Quota provider that stores quotas in the canonical {@link Storage} system.
   */
  static class QuotaManagerImpl implements QuotaManager {

    private final Storage storage;

    @Inject
    public QuotaManagerImpl(@StorageRole(Role.Primary) Storage storage) {
      this.storage = checkNotNull(storage);
    }

    @Override
    public Quota getQuota(final String role) {
      checkNotBlank(role);

      Quota quota = storage.doInTransaction(new Work.Quiet<Quota>() {
        @Override public Quota apply(StoreProvider storeProvider) {
          return storeProvider.getQuotaStore().fetchQuota(role);
        }
      });

      // If this user doesn't have a quota record, return non-null empty quota.
      return quota == null ? noQuota() : quota;
    }

    private static Quota getUpdateQuota(Collection<ShardUpdateConfiguration> configs,
        Function<ShardUpdateConfiguration, TwitterTaskInfo> taskExtractor) {
      return Quotas.fromTasks(Iterables.filter(Iterables.transform(configs, taskExtractor),
          Predicates.notNull()));
    }

    @Override
    public Quota getConsumption(final String role) {
      checkNotBlank(role);

      final Query query = new Query(new TaskQuery()
          .setOwner(new Identity().setRole(role))
          .setStatuses(Tasks.ACTIVE_STATES));

      return storage.doInTransaction(
          new Work.Quiet<Quota>() {
            @Override
            public Quota apply(StoreProvider storeProvider) {
              Quota quota = Quotas.fromTasks(Iterables.transform(
                  storeProvider.getTaskStore().fetchTasks(query), Tasks.SCHEDULED_TO_INFO));

              Multimap<String, ShardUpdateConfiguration> activeUpdates =
                  storeProvider.getUpdateStore().fetchShardUpdateConfigs(role);
              for (Collection<ShardUpdateConfiguration> shardUpdateConfigs
                  : activeUpdates.asMap().values()) {

                // If the user is performing an update that increases the quota for the job,
                // bill them for the updated job.
                Quota additionalQuota = Quotas.subtract(
                    getUpdateQuota(shardUpdateConfigs, Shards.GET_NEW_CONFIG),
                    getUpdateQuota(shardUpdateConfigs, Shards.GET_ORIGINAL_CONFIG)
                );
                if (Quotas.greaterThan(additionalQuota, Quotas.NO_QUOTA)) {
                  quota = Quotas.add(quota, additionalQuota);
                }
              }

              return quota;
            }
          });
    }

    @Override
    public void setQuota(final String role, final Quota quota) {
      checkNotBlank(role);
      checkNotNull(quota);

      storage.doInTransaction(new Work.NoResult.Quiet() {
        @Override public void execute(StoreProvider storeProvider) {
          storeProvider.getQuotaStore().saveQuota(role, quota);
        }
      });
    }

    @Override
    public synchronized boolean hasRemaining(String role, Quota quota) {
      checkNotBlank(role);
      checkNotNull(quota);

      return Quotas.geq(getQuota(role), Quotas.add(getConsumption(role), quota));
    }

    private static Quota noQuota() {
      return Quotas.NO_QUOTA.deepCopy();
    }
  }
}
