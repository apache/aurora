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

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.twitter.common.collections.Pair;

import org.apache.aurora.scheduler.Resources.InsufficientResourcesException;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Type;
import org.junit.Test;

import static org.apache.aurora.scheduler.ResourceType.PORTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ResourcesTest {
  @Test
  public void testPortRangeExact() {
    Resource portsResource = createPortRange(Pair.of(1, 5));
    Set<Integer> ports = Resources.getPorts(createOffer(portsResource), 5);
    assertEquals(5, ports.size());
  }

  @Test
  public void testOnePortAvailable() {
    Resource portsResource = createPortRange(Pair.of(3, 3));
    Set<Integer> ports = Resources.getPorts(createOffer(portsResource), 1);
    assertEquals(1, ports.size());
  }

  @Test
  public void testPortRangeAbundance() {
    Resource portsResource = createPortRange(Pair.of(1, 10));
    Set<Integer> ports = Resources.getPorts(createOffer(portsResource), 5);
    assertEquals(5, ports.size());
  }

  @Test
  public void testPortRangeExhaust() {
    Resource portsResource = createPortRanges(Pair.of(1, 2), Pair.of(10, 15));

    Set<Integer> ports = Resources.getPorts(createOffer(portsResource), 7);
    assertEquals(7, ports.size());

    ports = Resources.getPorts(createOffer(portsResource), 8);
    assertEquals(8, ports.size());

    try {
      Resources.getPorts(createOffer(portsResource), 9);
      fail("Ports should not have been sufficient");
    } catch (InsufficientResourcesException e) {
      // Expected.
    }
  }

  @Test
  public void testGetNoPorts() {
    Resource portsResource = createPortRange(Pair.of(1, 5));
    assertEquals(ImmutableSet.of(), Resources.getPorts(createOffer(portsResource), 0));
  }

  @Test(expected = Resources.InsufficientResourcesException.class)
  public void testPortRangeScarcity() {
    Resource portsResource = createPortRange(Pair.of(1, 2));
    Resources.getPorts(createOffer(portsResource), 5);
  }

  private Resource createPortRange(Pair<Integer, Integer> range) {
    return createPortRanges(ImmutableSet.of(range));
  }

  private Resource createPortRanges(Pair<Integer, Integer> rangeA, Pair<Integer, Integer> rangeB) {
    return createPortRanges(
        ImmutableSet.<Pair<Integer, Integer>>builder().add(rangeA).add(rangeB).build());
  }

  private Resource createPortRanges(Set<Pair<Integer, Integer>> ports) {
    Ranges.Builder ranges = Ranges.newBuilder();
    for (Pair<Integer, Integer> range : ports) {
      ranges.addRange(Range.newBuilder().setBegin(range.getFirst()).setEnd(range.getSecond()));
    }

    return Resource.newBuilder()
        .setName(PORTS.getName())
        .setType(Type.RANGES)
        .setRanges(ranges)
        .build();
  }

  private Protos.Offer createOffer(Resource resources) {
    return Protos.Offer.newBuilder()
        .setId(Protos.OfferID.newBuilder().setValue("offer-id"))
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework-id"))
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave-id"))
        .setHostname("hostname")
        .addResources(resources).build();
  }
}
