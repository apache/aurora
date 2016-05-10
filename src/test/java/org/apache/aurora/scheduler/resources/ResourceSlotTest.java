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
package org.apache.aurora.scheduler.resources;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.common.collections.Pair;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos;
import org.junit.Test;

import static org.apache.aurora.scheduler.base.TaskTestUtil.DEV_TIER;
import static org.apache.aurora.scheduler.base.TaskTestUtil.REVOCABLE_TIER;
import static org.apache.aurora.scheduler.resources.ResourceSlot.makeMesosRangeResource;
import static org.apache.aurora.scheduler.resources.ResourceSlot.makeMesosResource;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ResourceSlotTest {

  private static final ResourceSlot NEGATIVE_ONE =
      new ResourceSlot(-1.0, Amount.of(-1L, Data.MB), Amount.of(-1L, Data.MB), -1);
  private static final ResourceSlot ONE =
      new ResourceSlot(1.0, Amount.of(1L, Data.MB), Amount.of(1L, Data.MB), 1);
  private static final ResourceSlot TWO =
      new ResourceSlot(2.0, Amount.of(2L, Data.MB), Amount.of(2L, Data.MB), 2);
  private static final ResourceSlot THREE =
      new ResourceSlot(3.0, Amount.of(3L, Data.MB), Amount.of(3L, Data.MB), 3);
  private static final ITaskConfig TASK = ITaskConfig.build(new TaskConfig()
      .setNumCpus(1.0)
      .setRamMb(1024)
      .setDiskMb(2048)
      .setRequestedPorts(ImmutableSet.of("http", "debug")));

  @Test
  public void testSubtract() {
    assertEquals(ONE, TWO.subtract(ONE));
    assertEquals(TWO, THREE.subtract(ONE));
    assertEquals(NEGATIVE_ONE, ONE.subtract(TWO));
    assertEquals(NEGATIVE_ONE, TWO.subtract(THREE));
  }

  @Test
  public void testToResourceListNoRevoca() {
    ResourceSlot resources = ResourceSlot.from(TASK);
    assertEquals(
        ImmutableSet.of(
            makeMesosResource(CPUS, TASK.getNumCpus(), false),
            makeMesosResource(RAM_MB, TASK.getRamMb(), false),
            makeMesosResource(DISK_MB, TASK.getDiskMb(), false)),
        ImmutableSet.copyOf(resources.toResourceList(DEV_TIER)));
  }

  @Test
  public void testToResourceListRevocable() {
    ResourceSlot resources = ResourceSlot.from(TASK);
    assertEquals(
        ImmutableSet.of(
            makeMesosResource(CPUS, TASK.getNumCpus(), true),
            makeMesosResource(RAM_MB, TASK.getRamMb(), false),
            makeMesosResource(DISK_MB, TASK.getDiskMb(), false)),
        ImmutableSet.copyOf(resources.toResourceList(REVOCABLE_TIER)));
  }

  @Test
  public void testToResourceListNoPorts() {
    ResourceSlot resources = ResourceSlot.from(TASK);
    assertEquals(
        ImmutableSet.of(
            makeMesosResource(CPUS, TASK.getNumCpus(), true),
            makeMesosResource(RAM_MB, TASK.getRamMb(), false),
            makeMesosResource(DISK_MB, TASK.getDiskMb(), false)),
        ImmutableSet.copyOf(resources.toResourceList(REVOCABLE_TIER)));
  }

  @Test
  public void testRangeResourceEmpty() {
    expectRanges(ImmutableSet.of(), ImmutableSet.of());
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

  @Test
  public void testEqualsBadType() {
    ResourceSlot resources = ResourceSlot.from(TASK);
    assertNotEquals(resources, "Hello");
    assertNotEquals(resources, null);
  }

  private void expectRanges(Set<Pair<Long, Long>> expected, Set<Integer> values) {
    Protos.Resource resource = makeMesosRangeResource(PORTS, values);
    assertEquals(Protos.Value.Type.RANGES, resource.getType());
    assertEquals(PORTS.getMesosName(), resource.getName());

    Set<Pair<Long, Long>> actual = ImmutableSet.copyOf(Iterables.transform(
        resource.getRanges().getRangeList(),
        range -> Pair.of(range.getBegin(), range.getEnd())));
    assertEquals(expected, actual);
  }
}
