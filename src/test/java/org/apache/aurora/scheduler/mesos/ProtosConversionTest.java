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
package org.apache.aurora.scheduler.mesos;

import org.apache.mesos.Protos;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProtosConversionTest {

  @Test
  public void testOfferIDRoundTrip() {
    Protos.OfferID offerID = Protos.OfferID.newBuilder().setValue("offer-id").build();

    assertEquals(offerID, ProtosConversion.convert(ProtosConversion.convert(offerID)));
  }

  @Test
  public void testOfferRoundTrip() {
    Protos.Offer offer = Protos.Offer.newBuilder()
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework-id").build())
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave-id").build())
        .setHostname("hostname")
        .setId(Protos.OfferID.newBuilder().setValue("offer-id").build())
        .build();

    assertEquals(offer, ProtosConversion.convert(ProtosConversion.convert(offer)));
  }
}
