package com.twitter.mesos.scheduler.configuration;

import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Type;
import org.junit.Test;

import com.twitter.common.collections.Pair;
import com.twitter.mesos.scheduler.configuration.Resources.InsufficientResourcesException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ResourcesTest {

  private static final String NAME = "resource_name";

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
        .setName(Resources.PORTS)
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

  @Test
  public void testRangeResourceEmpty() {
    expectRanges(ImmutableSet.<Pair<Long, Long>>of(), ImmutableSet.<Integer>of());
  }

  @Test
  public void testRangeResourceOneEntry() {
    expectRanges(ImmutableSet.of(Pair.of(5L, 5L)), ImmutableSet.of(5));
    expectRanges(ImmutableSet.of(Pair.of(0L, 0L)), ImmutableSet.of(0));
  }

  @Test
  public void testRangeResourceNonContiguous() {
    expectRanges(ImmutableSet.of(Pair.of(1L, 1L), Pair.of(3L, 3L), Pair.of(5L, 5L)),
        ImmutableSet.of(5, 1, 3));
  }

  @Test
  public void testRangeResourceContiguous() {
    expectRanges(ImmutableSet.of(Pair.of(1L, 2L), Pair.of(4L, 5L), Pair.of(7L, 9L)),
        ImmutableSet.of(8, 2, 4, 5, 7, 9, 1));
  }

  private void expectRanges(Set<Pair<Long, Long>> expected, Set<Integer> values) {
    Resource resource = Resources.makeMesosRangeResource(NAME, values);
    assertEquals(Type.RANGES, resource.getType());
    assertEquals(NAME, resource.getName());

    Set<Pair<Long, Long>> actual = ImmutableSet.copyOf(Iterables.transform(
        resource.getRanges().getRangeList(),
        new Function<Range, Pair<Long, Long>>() {
          @Override public Pair<Long, Long> apply(Range range) {
            return Pair.of(range.getBegin(), range.getEnd());
          }
        }));
    assertEquals(expected, actual);
  }
}
