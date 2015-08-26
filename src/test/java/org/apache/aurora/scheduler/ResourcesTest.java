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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.collections.Pair;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.scheduler.Resources.InsufficientResourcesException;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.junit.Test;

import static org.apache.aurora.common.quantity.Data.MB;

import static org.apache.aurora.scheduler.ResourceSlot.makeMesosResource;
import static org.apache.aurora.scheduler.ResourceType.CPUS;
import static org.apache.aurora.scheduler.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.ResourceType.PORTS;
import static org.apache.aurora.scheduler.ResourceType.RAM_MB;
import static org.apache.mesos.Protos.Value.Type.RANGES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ResourcesTest {
  @Test
  public void testPortRangeExact() {
    Resource portsResource = createPortRange(Pair.of(1, 5));
    Set<Integer> ports = Resources.from(createOffer(portsResource)).getPorts(5);
    assertEquals(5, ports.size());
  }

  @Test
  public void testOnePortAvailable() {
    Resource portsResource = createPortRange(Pair.of(3, 3));
    Set<Integer> ports = Resources.from(createOffer(portsResource)).getPorts(1);
    assertEquals(1, ports.size());
  }

  @Test
  public void testPortRangeAbundance() {
    Resource portsResource = createPortRange(Pair.of(1, 10));
    Set<Integer> ports = Resources.from(createOffer(portsResource)).getPorts(5);
    assertEquals(5, ports.size());
  }

  @Test
  public void testPortRangeExhaust() {
    Resource portsResource = createPortRanges(Pair.of(1, 2), Pair.of(10, 15));

    Set<Integer> ports = Resources.from(createOffer(portsResource)).getPorts(7);
    assertEquals(7, ports.size());

    ports = Resources.from(createOffer(portsResource)).getPorts(8);
    assertEquals(8, ports.size());

    try {
      Resources.from(createOffer(portsResource)).getPorts(9);
      fail("Ports should not have been sufficient");
    } catch (InsufficientResourcesException e) {
      // Expected.
    }
  }

  @Test
  public void testGetNoPorts() {
    Resource portsResource = createPortRange(Pair.of(1, 5));
    assertEquals(ImmutableSet.of(), Resources.from(createOffer(portsResource)).getPorts(0));
  }

  @Test(expected = Resources.InsufficientResourcesException.class)
  public void testPortRangeScarcity() {
    Resource portsResource = createPortRange(Pair.of(1, 2));
    Resources.from(createOffer(portsResource)).getPorts(5);
  }

  @Test
  public void testGetSlot() {
    ImmutableList<Resource> resources = ImmutableList.<Resource>builder()
        .add(makeMesosResource(CPUS, 8.0, false))
        .add(makeMesosResource(RAM_MB, 1024, false))
        .add(makeMesosResource(DISK_MB, 2048, false))
        .add(createPortRange(Pair.of(1, 10)))
        .build();

    ResourceSlot expected = new ResourceSlot(8.0, Amount.of(1024L, MB), Amount.of(2048L, MB), 10);
    assertEquals(expected, Resources.from(createOffer(resources)).slot());
  }

  @Test
  public void testMissingResourcesHandledGracefully() {
    ImmutableList<Resource> resources = ImmutableList.<Resource>builder().build();
    assertEquals(ResourceSlot.NONE, Resources.from(createOffer(resources)).slot());
  }

  @Test
  public void testFilter() {
    ImmutableList<Resource> resources = ImmutableList.<Resource>builder()
        .add(makeMesosResource(CPUS, 8.0, true))
        .add(makeMesosResource(RAM_MB, 1024, false))
        .build();

    assertEquals(
        new ResourceSlot(8.0, Amount.of(1024L, MB), Amount.of(0L, MB), 0),
        Resources.from(createOffer(resources)).filter(Resources.REVOCABLE).slot());
  }

  @Test
  public void testFilterByTier() {
    ImmutableList<Resource> resources = ImmutableList.<Resource>builder()
        .add(makeMesosResource(CPUS, 8.0, true))
        .add(makeMesosResource(CPUS, 8.0, false))
        .add(makeMesosResource(RAM_MB, 1024, false))
        .build();

    assertEquals(
        new ResourceSlot(8.0, Amount.of(1024L, MB), Amount.of(0L, MB), 0),
        Resources.from(createOffer(resources)).filter(new TierInfo(true)).slot());

    assertEquals(
        new ResourceSlot(8.0, Amount.of(1024L, MB), Amount.of(0L, MB), 0),
        Resources.from(createOffer(resources)).filter(new TierInfo(false)).slot());
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
        .setType(RANGES)
        .setRanges(ranges)
        .build();
  }

  private static Protos.Offer createOffer(Resource resource) {
    return createOffer(ImmutableList.of(resource));
  }

  private static Protos.Offer createOffer(Iterable<Resource> resources) {
    return Protos.Offer.newBuilder()
        .setId(Protos.OfferID.newBuilder().setValue("offer-id"))
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework-id"))
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave-id"))
        .setHostname("hostname")
        .addAllResources(resources).build();
  }
}
