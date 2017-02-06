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

import java.util.List;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.mesos.v1.Protos.Resource;
import org.junit.Test;

import static org.apache.aurora.scheduler.resources.MesosResourceConverter.RANGES;
import static org.apache.aurora.scheduler.resources.MesosResourceConverter.SCALAR;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosRange;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.junit.Assert.assertEquals;

public class MesosResourceConverterTest {
  @Test
  public void testQuantifyScalar() {
    assertEquals(2, SCALAR.quantify(mesosScalar(CPUS, 2.0)).doubleValue(), 0.0);
  }

  @Test
  public void testQuantifyRangeSinglePort() {
    assertEquals(1, RANGES.quantify(mesosRange(PORTS, 5000)).doubleValue(), 0.0);
  }

  @Test
  public void testQuantifyRangeMultiplePorts() {
    assertEquals(3, RANGES.quantify(mesosRange(PORTS, 1, 2, 3)).doubleValue(), 0.0);
  }

  @Test
  public void testQuantifyRangeDefaultValue() {
    assertEquals(0, RANGES.quantify(mesosRange(PORTS)).doubleValue(), 0.0);
  }

  @Test
  public void testAllocateRange() {
    List<Resource> expected = ImmutableList.<Resource>builder()
        .add(mesosRange(PORTS, Optional.absent(), 80))
        .add(mesosRange(PORTS, Optional.absent(), 90, 100))
        .build();

    Iterable<Resource> actual = RANGES.toMesosResource(
        ImmutableSet.of(
            mesosRange(PORTS, Optional.absent(), 79).toBuilder(),
            mesosRange(PORTS, Optional.absent(), 80).toBuilder(),
            mesosRange(PORTS, Optional.absent(), 80, 90, 91, 92, 100).toBuilder()),
        () -> ImmutableSet.of(80, 90, 100),
        false);
    assertEquals(expected, actual);
  }

  @Test
  public void testAllocateRangeRevocable() {
    Resource.Builder builder = mesosRange(PORTS, Optional.absent(), 80).toBuilder()
        .setRevocable(Resource.RevocableInfo.newBuilder());

    List<Resource> expected = ImmutableList.<Resource>builder().add(builder.build()).build();

    Iterable<Resource> actual = RANGES.toMesosResource(
        ImmutableSet.of(builder),
        () -> ImmutableSet.of(80),
        true);
    assertEquals(expected, actual);
  }

  @Test(expected = ResourceManager.InsufficientResourcesException.class)
  public void testAllocateRangeInsufficent() {
    RANGES.toMesosResource(
        ImmutableSet.of(mesosRange(PORTS, Optional.absent(), 80, 81, 90, 91, 92).toBuilder()),
        () -> ImmutableSet.of(80, 90, 100),
        false);
  }

  @Test
  public void testAllocateScalar() {
    List<Resource> expected = ImmutableList.<Resource>builder()
        .add(mesosScalar(CPUS, 31.9999999))
        .build();

    Iterable<Resource> actual = SCALAR.toMesosResource(
        ImmutableSet.of(
            mesosScalar(CPUS, 0.0000001).toBuilder(),
            mesosScalar(CPUS, 31.9999999).toBuilder(),
            mesosScalar(CPUS, 15).toBuilder()),
        () -> 32.0,
        false);
    assertEquals(expected, actual);
  }

  @Test
  public void testAllocateScalarRevocable() {
    Resource.Builder builder = mesosScalar(RAM_MB, 128.0).toBuilder()
        .setRevocable(Resource.RevocableInfo.newBuilder());

    List<Resource> expected = ImmutableList.<Resource>builder().add(builder.build()).build();

    Iterable<Resource> actual = SCALAR.toMesosResource(
        ImmutableSet.of(builder),
        () -> 128.0,
        true);
    assertEquals(expected, actual);
  }

  @Test(expected = ResourceManager.InsufficientResourcesException.class)
  public void testAllocateScalarInsufficent() {
    SCALAR.toMesosResource(
        ImmutableSet.of(mesosScalar(RAM_MB, 32.0).toBuilder()),
        () -> 128.0,
        false);
  }
}
