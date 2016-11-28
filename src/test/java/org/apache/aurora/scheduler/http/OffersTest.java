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
package org.apache.aurora.scheduler.http;

import java.util.List;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.mesos.Protos;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OffersTest extends EasyMockTest {
  private Offers offers;
  private OfferManager offerManager;

  @Before
  public void setUp() {
    offerManager = createMock(OfferManager.class);
    offers = new Offers(offerManager);
  }

  @Test
  public void testNoOffers() throws Exception {
    expect(offerManager.getOffers()).andReturn(ImmutableSet.of());

    control.replay();

    Response response = offers.getOffers();
    assertEquals(HttpServletResponse.SC_OK, response.getStatus());
    assertTrue(toList(response.getEntity().toString()).isEmpty());
  }

  @Test
  public void testOneOffer() throws Exception {
    HostOffer offer = new HostOffer(
        Protos.Offer.newBuilder()
            .setId(Protos.OfferID.newBuilder().setValue("offer_id"))
            .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework_id"))
            .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave_id"))
            .setHostname("host_name")
            .addResources(Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(1.0).build())
                .setReservation(Protos.Resource.ReservationInfo.newBuilder()
                    .setLabels(Protos.Labels.newBuilder()
                        .addLabels(Protos.Label.newBuilder()
                            .setKey("key")
                            .setValue("value"))
                        .build())
                    .build())
                .build())
            .addResources(Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(128.0).build())
                .setReservation(Protos.Resource.ReservationInfo.newBuilder().build())
                .build())
            .addResources(Protos.Resource.newBuilder()
                .setName("disk")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(128.0).build())
                .setReservation(Protos.Resource.ReservationInfo.newBuilder()
                    .setLabels(Protos.Labels.newBuilder()
                        .addLabels(Protos.Label.newBuilder()
                            .setKey("key"))
                        .build())
                    .build())
                .setDisk(Protos.Resource.DiskInfo.newBuilder()
                    .setPersistence(Protos.Resource.DiskInfo.Persistence.newBuilder()
                        .setId("volume")
                        .build())
                    .setVolume(Protos.Volume.newBuilder()
                        .setContainerPath("path")
                        .setMode(Protos.Volume.Mode.RW)
                        .setImage(Protos.Image.newBuilder()
                            .setType(Protos.Image.Type.DOCKER)
                            .setDocker(Protos.Image.Docker.newBuilder()
                                .setName("image")
                                .build())
                            .build())
                        .build())
                    .setSource(Protos.Resource.DiskInfo.Source.newBuilder()
                        .setType(Protos.Resource.DiskInfo.Source.Type.PATH)
                        .setPath(Protos.Resource.DiskInfo.Source.Path.newBuilder()
                            .setRoot("root")
                            .build())
                        .build())
                    .build())
                .build())
            .addResources(Protos.Resource.newBuilder()
                .setName("gpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(4.0).build())
                .build())
            .addResources(Protos.Resource.newBuilder()
                .setName("ports")
                .setType(Protos.Value.Type.RANGES)
                .setRanges(Protos.Value.Ranges.newBuilder()
                    .addRange(Protos.Value.Range.newBuilder()
                        .setBegin(31000)
                        .setEnd(32000)
                        .build())
                    .build())
                .build())
            .build(),
        IHostAttributes.build(new HostAttributes().setMode(NONE)));

    expect(offerManager.getOffers()).andReturn(ImmutableSet.of(offer));

    control.replay();

    Response response = offers.getOffers();
    assertEquals(HttpServletResponse.SC_OK, response.getStatus());
    ObjectMapper mapper = new ObjectMapper()
        .registerModule(new ProtobufModule())
        .setPropertyNamingStrategy(
            PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    assertEquals(
        offer.getOffer(),
        mapper.readValue(response.getEntity().toString(), Protos.Offer.class));
  }

  private static List toList(String json) {
    return new Gson().fromJson(json, List.class);
  }
}
