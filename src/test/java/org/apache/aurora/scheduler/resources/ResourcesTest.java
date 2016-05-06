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

import org.apache.aurora.common.quantity.Amount;
import org.apache.mesos.Protos;
import org.junit.Test;

import static org.apache.aurora.common.quantity.Data.MB;
import static org.apache.aurora.scheduler.base.TaskTestUtil.DEV_TIER;
import static org.apache.aurora.scheduler.base.TaskTestUtil.REVOCABLE_TIER;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosRange;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.offer;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.junit.Assert.assertEquals;

public class ResourcesTest {
  @Test
  public void testGetSlot() {
    Protos.Offer offer = offer(
        mesosScalar(CPUS, 8.0, false),
        mesosScalar(RAM_MB, 1024, false),
        mesosScalar(DISK_MB, 2048, false),
        mesosRange(PORTS, 1, 2, 3));

    ResourceSlot expected = new ResourceSlot(8.0, Amount.of(1024L, MB), Amount.of(2048L, MB), 3);
    assertEquals(expected, Resources.from(offer).slot());
  }

  @Test
  public void testMissingResourcesHandledGracefully() {
    assertEquals(ResourceSlot.NONE, Resources.from(offer()).slot());
  }

  @Test
  public void testFilter() {
    Protos.Offer offer = offer(
        mesosScalar(CPUS, 8.0, true),
        mesosScalar(RAM_MB, 1024, false));

    assertEquals(
        new ResourceSlot(8.0, Amount.of(1024L, MB), Amount.of(0L, MB), 0),
        Resources.from(offer).filter(ResourceManager.REVOCABLE).slot());
  }

  @Test
  public void testFilterByTier() {
    Protos.Offer offer = offer(
        mesosScalar(CPUS, 8.0, true),
        mesosScalar(CPUS, 8.0, false),
        mesosScalar(RAM_MB, 1024, false));

    assertEquals(
        new ResourceSlot(8.0, Amount.of(1024L, MB), Amount.of(0L, MB), 0),
        Resources.from(offer).filter(REVOCABLE_TIER).slot());

    assertEquals(
        new ResourceSlot(8.0, Amount.of(1024L, MB), Amount.of(0L, MB), 0),
        Resources.from(offer).filter(DEV_TIER).slot());
  }
}
