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
package org.apache.aurora.scheduler.stats;

import com.google.common.collect.ImmutableList;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.stats.SlotSizeCounter.MachineResource;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.mesos.Protos;
import org.junit.Test;

import static org.apache.aurora.scheduler.stats.AsyncStatsModule.OfferAdapter;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class AsyncStatsModuleTest extends EasyMockTest {
  @Test
  public void testOfferAdapter() {
    OfferManager offerManager = createMock(OfferManager.class);
    expect(offerManager.getOffers()).andReturn(ImmutableList.of(
        new HostOffer(Protos.Offer.newBuilder()
            .setId(Protos.OfferID.newBuilder().setValue("offerId"))
            .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("frameworkId"))
            .setSlaveId(Protos.SlaveID.newBuilder().setValue("slaveId"))
            .setHostname("hostName")
            .addResources(getCpuResource(true, 2.0))
            .addResources(getCpuResource(false, 4.0))
            .build(),
            IHostAttributes.build(new HostAttributes()))));

    control.replay();

    OfferAdapter adapter = new OfferAdapter(offerManager);

    assertEquals(ImmutableList.of(resource(true, 2.0), resource(false, 4.0)), adapter.get());
  }

  private static MachineResource resource(boolean revocable, double cpu) {
    return new MachineResource(
        IResourceAggregate.build(new ResourceAggregate(cpu, 0, 0)), false, revocable);
  }

  private static Protos.Resource getCpuResource(boolean revocable, double value) {
    Protos.Resource.Builder builder = Protos.Resource.newBuilder()
        .setName(ResourceType.CPUS.getName())
        .setType(Protos.Value.Type.SCALAR)
        .setScalar(Protos.Value.Scalar.newBuilder().setValue(value));

    if (revocable) {
      builder.setRevocable(Protos.Resource.RevocableInfo.newBuilder());
    }

    return builder.build();
  }
}
