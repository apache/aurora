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
package org.apache.aurora.benchmark;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.mesos.Protos;

/**
 * Offer factory.
 */
final class Offers {
  private Offers() {
    // Utility class.
  }

  /**
   * Saves offers into the {@link OfferManager}.
   *
   * @param offerManager {@link OfferManager} to save into.
   * @param offers Offers to save.
   */
  static void addOffers(OfferManager offerManager, Iterable<HostOffer> offers) {
    for (HostOffer offer : offers) {
      offerManager.addOffer(offer);
    }
  }

  /**
   * Builds offers for the specified configuration.
   */
  static final class Builder {
    private static final String OFFER_ID_FORMAT = "offer-%s";
    private static final String FRAMEWORK_ID = "framework_id";

    private double cpu = 8.0;
    private Amount<Long, Data> ram = Amount.of(16L, Data.GB);
    private Amount<Long, Data> disk = Amount.of(256L, Data.GB);
    private int ports = 1024;

    Builder setCpu(double newCpu) {
      cpu = newCpu;
      return this;
    }

    Builder setRam(Amount<Long, Data> newRam) {
      ram = newRam;
      return this;
    }

    Builder setDisk(Amount<Long, Data> newDisk) {
      disk = newDisk;
      return this;
    }

    Builder setPorts(int newPorts) {
      ports = newPorts;
      return this;
    }

    /**
     * Builds a set of {@link HostOffer} for the current configuration.
     *
     * @param hostAttributes Host attributes to initialize offers from.
     * @return Set of offers.
     */
    Set<HostOffer> build(Set<IHostAttributes> hostAttributes) {
      ImmutableSet.Builder<HostOffer> offers = ImmutableSet.builder();
      int id = 0;
      for (IHostAttributes attributes : hostAttributes) {
        Protos.Offer offer = Protos.Offer.newBuilder()
            .addAllResources(new Resources(cpu, ram, disk, ports).toResourceList())
            .setId(Protos.OfferID.newBuilder().setValue(String.format(OFFER_ID_FORMAT, id++)))
            .setFrameworkId(Protos.FrameworkID.newBuilder().setValue(FRAMEWORK_ID))
            .setSlaveId(Protos.SlaveID.newBuilder().setValue(attributes.getSlaveId()))
            .setHostname(String.format(attributes.getHost()))
            .build();

        offers.add(new HostOffer(offer, attributes));
      }

      return offers.build();
    }
  }
}
