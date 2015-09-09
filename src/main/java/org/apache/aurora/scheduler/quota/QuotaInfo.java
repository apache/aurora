/**
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

import java.util.Objects;

import com.google.common.base.MoreObjects;

import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;

import static java.util.Objects.requireNonNull;

/**
 * Wraps allocated quota and consumption details.
 */
public class QuotaInfo {
  private final IResourceAggregate quota;
  private final IResourceAggregate prodSharedConsumption;
  private final IResourceAggregate prodDedicatedConsumption;
  private final IResourceAggregate nonProdSharedConsumption;
  private final IResourceAggregate nonProdDedicatedConsumption;

  QuotaInfo(
      IResourceAggregate quota,
      IResourceAggregate prodSharedConsumption,
      IResourceAggregate prodDedicatedConsumption,
      IResourceAggregate nonProdSharedConsumption,
      IResourceAggregate nonProdDedicatedConsumption) {

    this.quota = requireNonNull(quota);
    this.prodSharedConsumption = requireNonNull(prodSharedConsumption);
    this.prodDedicatedConsumption = requireNonNull(prodDedicatedConsumption);
    this.nonProdSharedConsumption = requireNonNull(nonProdSharedConsumption);
    this.nonProdDedicatedConsumption = requireNonNull(nonProdDedicatedConsumption);
  }

  /**
   * Total quota available.
   *
   * @return Available quota.
   */
  public IResourceAggregate getQuota() {
    return quota;
  }

  /**
   * Quota consumed by production jobs from a shared resource pool.
   *
   * @return Production job consumption.
   */
  public IResourceAggregate getProdSharedConsumption() {
    return prodSharedConsumption;
  }

  /**
   * Resources consumed by production jobs from a dedicated resource pool.
   *
   * @return Production dedicated job consumption.
   */
  public IResourceAggregate getProdDedicatedConsumption() {
    return prodDedicatedConsumption;
  }

  /**
   * Resources consumed by non-production jobs from a shared resource pool.
   *
   * @return Non production job consumption.
   */
  public IResourceAggregate getNonProdSharedConsumption() {
    return nonProdSharedConsumption;
  }

  /**
   * Resources consumed by non-production jobs from a dedicated resource pool.
   *
   * @return Non production dedicated job consumption.
   */
  public IResourceAggregate getNonProdDedicatedConsumption() {
    return nonProdDedicatedConsumption;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof QuotaInfo)) {
      return false;
    }

    QuotaInfo other = (QuotaInfo) o;

    return Objects.equals(quota, other.quota)
        && Objects.equals(prodSharedConsumption, other.prodSharedConsumption)
        && Objects.equals(prodDedicatedConsumption, other.prodDedicatedConsumption)
        && Objects.equals(nonProdSharedConsumption, other.nonProdSharedConsumption)
        && Objects.equals(nonProdDedicatedConsumption, other.nonProdDedicatedConsumption);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        quota,
        prodSharedConsumption,
        prodDedicatedConsumption,
        nonProdSharedConsumption,
        nonProdDedicatedConsumption);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("quota", quota)
        .add("prodSharedConsumption", prodSharedConsumption)
        .add("prodDedicatedConsumption", prodDedicatedConsumption)
        .add("nonProdSharedConsumption", nonProdSharedConsumption)
        .add("nonProdDedicatedConsumption", nonProdDedicatedConsumption)
        .toString();
  }
}
