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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IResource;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.Offer;
import org.apache.mesos.v1.Protos.Value.Scalar;
import org.junit.Test;

import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.namedPort;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.numGpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.apache.aurora.scheduler.base.TaskTestUtil.JOB;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.aggregate;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.bag;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosRange;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.apache.mesos.v1.Protos.Value.Type.SCALAR;
import static org.junit.Assert.assertEquals;

public class ResourceManagerTest {

  @Test
  public void testGetOfferResources() {
    ResourceType.initializeEmptyCliArgsForTest();
    Protos.Resource resource1 = Protos.Resource.newBuilder()
        .setType(SCALAR)
        .setName(CPUS.getMesosName())
        .setScalar(Scalar.newBuilder().setValue(2.0).build())
        .build();

    Protos.Resource resource2 = Protos.Resource.newBuilder()
        .setType(SCALAR)
        .setName(CPUS.getMesosName())
        .setRevocable(Protos.Resource.RevocableInfo.getDefaultInstance())
        .setScalar(Scalar.newBuilder().setValue(1.0).build())
        .build();

    Protos.Resource resource3 = Protos.Resource.newBuilder()
        .setType(SCALAR)
        .setName(RAM_MB.getMesosName())
        .setScalar(Scalar.newBuilder().setValue(64).build())
        .build();

    Offer offer = Offer.newBuilder()
        .setId(Protos.OfferID.newBuilder().setValue("offer-id"))
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework-id"))
        .setAgentId(Protos.AgentID.newBuilder().setValue("slave-id"))
        .setHostname("hostname")
        .addAllResources(ImmutableSet.of(resource1, resource2, resource3)).build();

    assertEquals(
        ImmutableSet.of(resource1, resource2),
        ImmutableSet.copyOf(ResourceManager.getOfferResources(offer, CPUS)));
    assertEquals(
        resource3,
        Iterables.getOnlyElement(ResourceManager.getOfferResources(offer, RAM_MB)));
    assertEquals(
        ImmutableSet.of(resource1, resource3),
        ImmutableSet.copyOf(ResourceManager.getNonRevocableOfferResources(offer)));
    assertEquals(
        ImmutableSet.of(resource2, resource3),
        ImmutableSet.copyOf(ResourceManager.getRevocableOfferResources(offer)));
    assertEquals(
        ImmutableSet.of(resource1, resource3),
        ImmutableSet.copyOf(ResourceManager.getOfferResources(offer, new TierInfo(false, false))));
    assertEquals(
        ImmutableSet.of(resource2, resource3),
        ImmutableSet.copyOf(ResourceManager.getOfferResources(offer, new TierInfo(false, true))));
  }

  @Test
  public void testGetTaskResources() {
    assertEquals(
        IResource.build(numCpus(1.0)),
        Iterables.getOnlyElement(ResourceManager.getTaskResources(makeTask("id", JOB), CPUS)));
    assertEquals(
        IResource.build(namedPort("http")),
        Iterables.getOnlyElement(ResourceManager.getTaskResources(makeTask("id", JOB), PORTS)));
    assertEquals(
        ImmutableSet.of(IResource.build(numCpus(1.0)), IResource.build(ramMb(1024))),
        ImmutableSet.copyOf(ResourceManager.getTaskResources(
            makeTask("id", JOB).getAssignedTask().getTask(),
            EnumSet.of(CPUS, RAM_MB))));
  }

  @Test
  public void testGetTaskResourceTypes() {
    AssignedTask builder = makeTask("id", JOB).newBuilder().getAssignedTask();
    builder.getTask().addToResources(namedPort("health"));
    builder.getTask().addToResources(numGpus(4));

    assertEquals(
        EnumSet.allOf(ResourceType.class),
        ResourceManager.getTaskResourceTypes(IAssignedTask.build(builder)));
  }

  @Test
  public void testMesosResourceQuantity() {
    Set<Protos.Resource> resources = ImmutableSet.of(
        mesosScalar(CPUS, 3.0),
        mesosScalar(CPUS, 4.0),
        mesosScalar(RAM_MB, 64),
        mesosRange(PORTS, 1, 3));

    assertEquals(7.0, ResourceManager.quantityOfMesosResource(resources, CPUS), 0.0);
    assertEquals(64, ResourceManager.quantityOfMesosResource(resources, RAM_MB), 0.0);
    assertEquals(0.0, ResourceManager.quantityOfMesosResource(resources, DISK_MB), 0.0);
    assertEquals(2, ResourceManager.quantityOfMesosResource(resources, PORTS), 0.0);
  }

  @Test
  public void testResourceQuantity() {
    assertEquals(
        8.0,
        ResourceManager.quantityOf(ImmutableSet.of(
            IResource.build(numCpus(3.0)),
            IResource.build(numCpus(5.0)))),
        0.0);
  }

  @Test
  public void testResourceQuantityByType() {
    assertEquals(
        8.0,
        ResourceManager.quantityOf(
            ImmutableSet.of(
                IResource.build(numCpus(3.0)),
                IResource.build(numCpus(5.0)),
                IResource.build(ramMb(128))),
            CPUS),
        0.0);
  }

  @Test
  public void testBagFromResources() {
    assertEquals(
        bag(2.0, 32, 64),
        ResourceManager.bagFromResources(ImmutableSet.of(
            IResource.build(numCpus(2.0)),
            IResource.build(ramMb(32)),
            IResource.build(diskMb(64)))));
  }

  @Test
  public void testBagFromMesosResources() {
    assertEquals(
        new ResourceBag(ImmutableMap.of(CPUS, 3.0)),
        ResourceManager.bagFromMesosResources(ImmutableSet.of(mesosScalar(CPUS, 3.0))));
  }

  @Test
  public void testBagFromMesosResourcesUnsupportedResources() {
    Protos.Resource unsupported = Protos.Resource.newBuilder()
        .setName("unknown")
        .setType(SCALAR)
        .setScalar(Scalar.newBuilder().setValue(1.0).build()).build();
    assertEquals(
        new ResourceBag(ImmutableMap.of(CPUS, 3.0)),
        ResourceManager.bagFromMesosResources(
            ImmutableSet.of(mesosScalar(CPUS, 3.0), unsupported)));
  }

  @Test
  public void testBagFromAggregate() {
    assertEquals(bag(1.0, 32, 64), ResourceManager.bagFromAggregate(aggregate(1.0, 32, 64)));
  }

  @Test
  public void testAggregateFromBag() {
    assertEquals(aggregate(1.0, 1024, 4096), ResourceManager.aggregateFromBag(ResourceBag.SMALL));
  }
}
