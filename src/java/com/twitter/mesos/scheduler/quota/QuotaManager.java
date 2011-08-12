package com.twitter.mesos.scheduler.quota;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.google.inject.Inject;

import org.springframework.scheduling.SchedulingException;

import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.Storage.Work.NoResult;
import com.twitter.mesos.scheduler.storage.StorageRole;
import com.twitter.mesos.scheduler.storage.StorageRole.Role;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
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
   * Assigns quota to a user, overwriting any previously-assigned quota.
   *
   * @param role Role to assign quota for.
   * @param quota Quota to allocate for the role.
   */
  void setQuota(String role, Quota quota);

  /**
   * Consumes all or a portion of a role's quota.
   *
   * @param role Role to consume quota for.
   * @param quota Quota amount to consume.
   * @throws InsufficientQuotaException If the role does not have sufficient remaining quota to
   *     consume the amount requested.
   */
  void consumeQuota(String role, Quota quota) throws InsufficientQuotaException;

  /**
   * Releases quota that was consumed by a role.
   *
   * @param role Role to release quota for.
   * @param quota Quota to release for the role.
   * @throws IllegalStateException If the release will cause the consumed quota to become negative,
   *     indicating an accounting error.
   */
  void releaseQuota(String role, Quota quota) throws IllegalStateException;

  /**
   * Thrown when an attempt is made to consume more quota than a role has available.
   */
  public static class InsufficientQuotaException extends SchedulingException {
    public InsufficientQuotaException(String msg) {
      super(msg);
    }

    public InsufficientQuotaException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  /**
   * Quota provider that stores quotas in the canonical {@link Storage} system.
   */
  public static class QuotaManagerImpl implements QuotaManager {

    private final Storage storage;
    private final Map<String, Quota> consumedQuotaByRole = new MapMaker().makeComputingMap(
        new Function<String, Quota>() {
          @Override public Quota apply(String role) {
            return new Quota();
          }
        });

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
      return quota == null ? new Quota() : quota;
    }

    @Override
    public void setQuota(final String role, Quota quota) {
      checkNotBlank(role);
      checkNotNull(quota);

      // Don't modify the argument.
      final Quota stored = new Quota(quota);
      if (!stored.isSetNumCpus()) {
        stored.setNumCpus(0);
      }
      if (!stored.isSetRamMb()) {
        stored.setRamMb(0);
      }
      if (!stored.isSetDiskMb()) {
        stored.setDiskMb(0);
      }

      storage.doInTransaction(new NoResult.Quiet() {
        @Override protected void execute(StoreProvider storeProvider) {
          storeProvider.getQuotaStore().saveQuota(role, stored);
        }
      });
    }

    @Override
    public synchronized void consumeQuota(String role, Quota quota) throws InsufficientQuotaException {
      checkNotBlank(role);
      checkNotNull(quota);

      Quota roleQuota = getQuota(role);
      Quota postConsumption = add(consumedQuotaByRole.get(role), quota);
      if (postConsumption.getNumCpus() > roleQuota.getNumCpus()) {
        throw new InsufficientQuotaException("Insufficient CPU quota.");
      } else if(postConsumption.getRamMb() > roleQuota.getRamMb()) {
        throw new InsufficientQuotaException("Insufficient RAM quota.");
      } else if (postConsumption.getDiskMb() > roleQuota.getDiskMb()) {
        throw new InsufficientQuotaException("Insufficient disk quota.");
      }
      consumedQuotaByRole.put(role, postConsumption);
    }

    @Override
    public synchronized void releaseQuota(String role, Quota quota) throws IllegalStateException {
      checkNotBlank(role);
      checkNotNull(quota);

      consumedQuotaByRole.put(role, subtract(consumedQuotaByRole.get(role), quota));
    }

    /**
     * a + b
     */
    private static Quota add(Quota a, Quota b) {
      return new Quota()
          .setNumCpus(a.getNumCpus() + b.getNumCpus())
          .setRamMb(a.getRamMb() + b.getRamMb())
          .setDiskMb(a.getDiskMb() + b.getDiskMb());
    }

    /**
     * a - b
     */
    private static Quota subtract(Quota a, Quota b) throws IllegalStateException {
      Quota result = new Quota()
          .setNumCpus(a.getNumCpus() - b.getNumCpus())
          .setRamMb(a.getRamMb() - b.getRamMb())
          .setDiskMb(a.getDiskMb() - b.getDiskMb());
      checkState(result.getNumCpus() >= 0, "cpus cannot be negative");
      checkState(result.getRamMb() >= 0, "ram MB cannot be negative");
      checkState(result.getDiskMb() >= 0, "disk MB cannot be negative");
      return result;
    }
  }
}
