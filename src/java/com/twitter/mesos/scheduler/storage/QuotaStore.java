package com.twitter.mesos.scheduler.storage;

import java.util.Set;

import com.google.common.base.Optional;

import com.twitter.mesos.gen.Quota;

/**
 * Point of storage for quota records.
 *
 * @author William Farner
 */
public interface QuotaStore {

  /**
   * Deletes all quotas.
   */
  void deleteQuotas();

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
   * @return Optional quota associated with {@code role}.
   */
  Optional<Quota> fetchQuota(String role);

  /**
   * Fetches all roles that have been assigned quotas.
   *
   * @return All roles with quota.
   */
  Set<String> fetchQuotaRoles();
}
