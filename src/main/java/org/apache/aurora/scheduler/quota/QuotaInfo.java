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
  private final IResourceAggregate prodConsumption;
  private final IResourceAggregate nonProdConsumption;

  QuotaInfo(
      IResourceAggregate quota,
      IResourceAggregate prodConsumption,
      IResourceAggregate nonProdConsumption) {

    this.quota = requireNonNull(quota);
    this.prodConsumption = requireNonNull(prodConsumption);
    this.nonProdConsumption = requireNonNull(nonProdConsumption);
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
   * Quota consumed by production jobs.
   *
   * @return Production job consumption.
   */
  public IResourceAggregate getProdConsumption() {
    return prodConsumption;
  }

  /**
   * Quota consumed by non-production jobs.
   *
   * @return Non production job consumption.
   */
  public IResourceAggregate getNonProdConsumption() {
    return nonProdConsumption;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof QuotaInfo)) {
      return false;
    }

    QuotaInfo other = (QuotaInfo) o;

    return Objects.equals(quota, other.quota)
        && Objects.equals(prodConsumption, other.prodConsumption)
        && Objects.equals(nonProdConsumption, other.nonProdConsumption);
  }

  @Override
  public int hashCode() {
    return Objects.hash(quota, prodConsumption, nonProdConsumption);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("quota", quota)
        .add("prodConsumption", prodConsumption)
        .add("nonProdConsumption", nonProdConsumption)
        .toString();
  }
}
