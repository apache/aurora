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

import java.util.EnumSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.storage.entities.IResource;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Value.Scalar;
import org.junit.Test;

import static org.apache.aurora.gen.Resource.namedPort;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.scheduler.base.TaskTestUtil.JOB;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosRange;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.apache.mesos.Protos.Value.Type.SCALAR;
import static org.junit.Assert.assertEquals;

public class ResourceManagerTest {
  @Test
  public void testGetOfferResources() {
    Protos.Resource resource1 = Protos.Resource.newBuilder()
        .setType(SCALAR)
        .setName(CPUS.getMesosName())
        .setScalar(Scalar.newBuilder().setValue(2.0).build())
        .build();

    Protos.Resource resource2 = Protos.Resource.newBuilder()
        .setType(SCALAR)
        .setName(RAM_MB.getMesosName())
        .setScalar(Scalar.newBuilder().setValue(64).build())
        .build();

    Offer offer = Offer.newBuilder()
        .setId(Protos.OfferID.newBuilder().setValue("offer-id"))
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework-id"))
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave-id"))
        .setHostname("hostname")
        .addAllResources(ImmutableSet.of(resource1, resource2)).build();

    assertEquals(
        resource1,
        Iterables.getOnlyElement(ResourceManager.getOfferResources(offer, CPUS)));
    assertEquals(
        resource2,
        Iterables.getOnlyElement(ResourceManager.getOfferResources(offer, RAM_MB)));
  }

  @Test
  public void testGetTaskResources() {
    assertEquals(
        IResource.build(numCpus(1.0)),
        Iterables.getOnlyElement(ResourceManager.getTaskResources(makeTask("id", JOB), CPUS)));
    assertEquals(
        IResource.build(namedPort("http")),
        Iterables.getOnlyElement(ResourceManager.getTaskResources(makeTask("id", JOB), PORTS)));
  }

  @Test
  public void testGetTaskResourceTypes() {
    ScheduledTask builder = makeTask("id", JOB).newBuilder();
    builder.getAssignedTask().getTask().addToResources(namedPort("health"));

    assertEquals(
        EnumSet.allOf(ResourceType.class),
        ResourceManager.getTaskResourceTypes(IScheduledTask.build(builder)));
  }

  @Test
  public void testMesosResourceQuantity() {
    Set<Protos.Resource> resources = ImmutableSet.of(
        mesosScalar(CPUS, 3.0),
        mesosScalar(CPUS, 4.0),
        mesosScalar(RAM_MB, 64),
        mesosRange(PORTS, 1, 3));

    assertEquals(7.0, ResourceManager.quantityOf(resources, CPUS), 0.0);
    assertEquals(64, ResourceManager.quantityOf(resources, RAM_MB), 0.0);
    assertEquals(0.0, ResourceManager.quantityOf(resources, DISK_MB), 0.0);
    assertEquals(2, ResourceManager.quantityOf(resources, PORTS), 0.0);
  }
}
