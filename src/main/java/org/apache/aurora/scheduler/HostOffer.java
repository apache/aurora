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
package org.apache.aurora.scheduler;

import java.time.Instant;
import java.util.Objects;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.apache.aurora.scheduler.base.Conversions;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.resources.ResourceManager.bagFromMesosResources;
import static org.apache.aurora.scheduler.resources.ResourceManager.getOfferResources;
import static org.apache.mesos.v1.Protos.Offer;

/**
 * An available resource in the cluster.
 */
public class HostOffer {
  private final Offer offer;
  private final IHostAttributes hostAttributes;
  private final LoadingCache<TierInfo, ResourceBag> resourceBagCache;

  // Offers lacking CPU or mem are flagged so that they may be efficiently ignored during
  // scheduling.  However, they are retained for other purposes such as preemption and cluster
  // stats.
  // When nonZeroCpuAndMem=true, it means that _any_ CPU or mem resource is available, regardless
  // of whether the resource is revocable.
  private final boolean nonZeroCpuAndMem;

  public HostOffer(Offer offer, IHostAttributes hostAttributes) {
    this.offer = requireNonNull(offer);
    this.hostAttributes = requireNonNull(hostAttributes);
    this.nonZeroCpuAndMem = offerHasCpuAndMem(offer);
    this.resourceBagCache = CacheBuilder.newBuilder().build(
        new CacheLoader<TierInfo, ResourceBag>() {
          @Override
          public ResourceBag load(TierInfo tierInfo) {
            return bagFromMesosResources(getOfferResources(offer, tierInfo));
          }
        });
  }

  private static boolean offerHasCpuAndMem(Offer offer) {
    ResourceBag resources = bagFromMesosResources(offer.getResourcesList());
    return resources.valueOf(ResourceType.CPUS) > 0.0
        && resources.valueOf(ResourceType.RAM_MB) > 0.0;
  }

  public Offer getOffer() {
    return offer;
  }

  public IHostAttributes getAttributes() {
    return hostAttributes;
  }

  public boolean hasCpuAndMem() {
    return nonZeroCpuAndMem;
  }

  public ResourceBag getResourceBag(TierInfo tierInfo) {
    return resourceBagCache.getUnchecked(tierInfo);
  }

  public Optional<Instant> getUnavailabilityStart() {
    if (offer.hasUnavailability()) {
      return Optional.of(Conversions.getStart(offer.getUnavailability()));
    } else {
      return Optional.absent();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof HostOffer)) {
      return false;
    }
    HostOffer other = (HostOffer) o;
    return Objects.equals(offer, other.offer)
        && Objects.equals(hostAttributes, other.hostAttributes)
        && nonZeroCpuAndMem == other.nonZeroCpuAndMem;
  }

  @Override
  public int hashCode() {
    return Objects.hash(offer, hostAttributes);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("offer", offer)
        .add("hostAttributes", hostAttributes)
        .add("nonZeroCpuAndMem", nonZeroCpuAndMem)
        .toString();
  }
}
