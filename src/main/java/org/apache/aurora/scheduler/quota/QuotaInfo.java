/**
 * Copyright 2014 Apache Software Foundation
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
package org.apache.aurora.scheduler.quota;

import org.apache.aurora.scheduler.storage.entities.IQuota;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps allocated quota and consumption details.
 */
public class QuotaInfo {
  private final IQuota quota;
  private final IQuota prodConsumption;
  private final IQuota nonProdConsumption;

  QuotaInfo(IQuota quota, IQuota prodConsumption, IQuota nonProdConsumption) {
    this.quota = checkNotNull(quota);
    this.prodConsumption = checkNotNull(prodConsumption);
    this.nonProdConsumption = checkNotNull(nonProdConsumption);
  }

  /**
   * Total quota available.
   *
   * @return Available quota.
   */
  public IQuota guota() {
    return quota;
  }

  /**
   * Quota consumed by production jobs.
   *
   * @return Production job consumption.
   */
  public IQuota prodConsumption() {
    return prodConsumption;
  }

  /**
   * Quota consumed by non-production jobs.
   *
   * @return Non production job consumption.
   */
  public IQuota nonProdConsumption() {
    return nonProdConsumption;
  }
}
