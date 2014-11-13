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

import java.util.Objects;

import org.apache.aurora.scheduler.storage.entities.IHostAttributes;

import static java.util.Objects.requireNonNull;

import static org.apache.mesos.Protos.Offer;

/**
 * An available resource in the cluster.
 */
public class HostOffer {
  private final Offer offer;
  private final IHostAttributes hostAttributes;

  public HostOffer(Offer offer, IHostAttributes hostAttributes) {
    this.offer = requireNonNull(offer);
    this.hostAttributes = requireNonNull(hostAttributes);
  }

  public Offer getOffer() {
    return offer;
  }

  public IHostAttributes getAttributes() {
    return hostAttributes;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof HostOffer)) {
      return false;
    }
    HostOffer other = (HostOffer) o;
    return Objects.equals(offer, other.offer)
        && Objects.equals(hostAttributes, other.hostAttributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(offer, hostAttributes);
  }

  @Override
  public String toString() {
    return com.google.common.base.Objects.toStringHelper(this)
        .add("offer", offer)
        .add("hostAttributes", hostAttributes)
        .toString();
  }
}
