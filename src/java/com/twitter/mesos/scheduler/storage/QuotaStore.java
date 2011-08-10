package com.twitter.mesos.scheduler.storage;

import javax.annotation.Nullable;

import com.twitter.mesos.gen.Quota;

/**
 * Point of storage for quota records.
 *
 * @author William Farner
 */
public interface QuotaStore {

  /**
   * Deletes quota for a role.
   *
   * @param role Role to remove quota record for.
   */
  void removeQuota(String role);

  /**
   * Saves a quota record for a role.
   *
   * @param role Role to create or update a quota record for.
   * @param quota Quota to save.
   */
  void saveQuota(String role, Quota quota);

  /**
   * Fetches the existing quota record for a role.
   *
   * @param role Role to fetch quota for.
   * @return Quota associated with {@code role}, or {@code null} if there is no entry for the role.
   */
  @Nullable
  Quota fetchQuota(String role);
}
