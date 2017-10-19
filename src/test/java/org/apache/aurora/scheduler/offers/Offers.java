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
package org.apache.aurora.scheduler.offers;

import com.google.common.collect.ImmutableList;

import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.mesos.v1.Protos.AgentID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.Offer;
import org.apache.mesos.v1.Protos.OfferID;

import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;

/**
 * Utility class for creating resource offers in unit tests.
 */
public final class Offers {
  private Offers() {
    // Utility class.
  }

  public static final String DEFAULT_HOST = "hostname";

  public static Offer makeOffer(String offerId) {
    return Offers.makeOffer(offerId, DEFAULT_HOST);
  }

  public static Offer makeOffer(String offerId, String hostName) {
    return Offer.newBuilder()
        .setId(OfferID.newBuilder().setValue(offerId))
        .setFrameworkId(FrameworkID.newBuilder().setValue("framework_id"))
        .setAgentId(AgentID.newBuilder().setValue("slave_id-" + offerId))
        .setHostname(hostName)
        .addAllResources(ImmutableList.of(
            mesosScalar(ResourceType.CPUS, 10),
            mesosScalar(ResourceType.RAM_MB, 1024)))
        .build();
  }
}
