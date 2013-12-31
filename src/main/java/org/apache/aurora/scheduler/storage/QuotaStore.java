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
package com.twitter.aurora.scheduler.storage;

import java.util.Map;

import com.google.common.base.Optional;

import com.twitter.aurora.scheduler.storage.entities.IQuota;

/**
 * Point of storage for quota records.
 */
public interface QuotaStore {
  /**
   * Fetches the existing quota record for a role.
   *
   * @param role Role to fetch quota for.
   * @return Optional quota associated with {@code role}.
   */
  Optional<IQuota> fetchQuota(String role);

  /**
   * Fetches all allocated quotas.
   *
   * @return All allocated quotas.
   */
  Map<String, IQuota> fetchQuotas();

  public interface Mutable extends QuotaStore {

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
    void saveQuota(String role, IQuota quota);
  }
}
