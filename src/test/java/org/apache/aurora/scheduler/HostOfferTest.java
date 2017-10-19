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

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.offer;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HostOfferTest {

  private static final IHostAttributes HOST_ATTRIBUTES_A =
      IHostAttributes.build(new HostAttributes().setMode(NONE).setHost("host-a"));

  @Test
  public void testHasCpuOrMem() {
    List<HostOffer> noCpuOrMem = ImmutableList.of(
        new HostOffer(offer("no-resources"), HOST_ATTRIBUTES_A),
        new HostOffer(
            offer("no-cpu-explicit", mesosScalar(CPUS, 0), mesosScalar(RAM_MB, 1024)),
            HOST_ATTRIBUTES_A),
        new HostOffer(
            offer("no-cpu-implicit", mesosScalar(RAM_MB, 1024)),
            HOST_ATTRIBUTES_A),
        new HostOffer(
            offer("no-mem-explicit", mesosScalar(CPUS, 10), mesosScalar(RAM_MB, 0)),
            HOST_ATTRIBUTES_A),
        new HostOffer(
            offer("no-mem-implicit", mesosScalar(CPUS, 10)),
            HOST_ATTRIBUTES_A));
    for (HostOffer offer : noCpuOrMem) {
      assertFalse(offer.hasCpuAndMem());
    }

    List<HostOffer> hasCpuOrMem = ImmutableList.of(
        new HostOffer(
            offer(
                "mixed-cpu-implicit-1",
                mesosScalar(CPUS, 10, true),
                mesosScalar(RAM_MB, 1024)),
            HOST_ATTRIBUTES_A),
        new HostOffer(
            offer(
                "mixed-cpu-implicit-2",
                mesosScalar(CPUS, 10, false),
                mesosScalar(RAM_MB, 1024)),
            HOST_ATTRIBUTES_A),
        new HostOffer(
            offer(
                "mixed-cpu-explicit-1",
                mesosScalar(CPUS, 0, false),
                mesosScalar(CPUS, 10, true),
                mesosScalar(RAM_MB, 1024)),
            HOST_ATTRIBUTES_A),
        new HostOffer(
            offer(
                "mixed-cpu-explicit-2",
                mesosScalar(CPUS, 10, false),
                mesosScalar(CPUS, 0, true),
                mesosScalar(RAM_MB, 1024)),
            HOST_ATTRIBUTES_A));
    for (HostOffer offer : hasCpuOrMem) {
      assertTrue(offer.hasCpuAndMem());
    }
  }
}
