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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AcceptedOfferTest {

  private static final Optional<String> TEST_ROLE = Optional.of("test-role");
  private static final Optional<String> ABSENT_ROLE = Optional.absent();
  private static final ResourceSlot TASK_SLOT = new ResourceSlot(
      4, Amount.of(100L, Data.MB), Amount.of(200L, Data.MB), 0);
  private static final ResourceSlot EXECUTOR_SLOT = new ResourceSlot(
      0.25, Amount.of(25L, Data.MB), Amount.of(75L, Data.MB), 0);
  private static final ResourceSlot TOTAL_SLOT = EXECUTOR_SLOT.add(TASK_SLOT);
  private static final Integer[] TASK_PORTS = {80, 90};
  private static final Set<Integer> TASK_PORTS_SET = ImmutableSet.copyOf(TASK_PORTS);

  @Test
  public void testReservedPredicates() {
    Protos.Resource withRole = makeScalar(ResourceType.CPUS.getName(), TEST_ROLE, false, 1.0);
    assertTrue(AcceptedOffer.RESERVED.apply(withRole));
    assertFalse(AcceptedOffer.NOT_RESERVED.apply(withRole));
    Protos.Resource absentRole = makeScalar(ResourceType.CPUS.getName(), ABSENT_ROLE, false, 1.0);
    assertFalse(AcceptedOffer.RESERVED.apply(absentRole));
    assertTrue(AcceptedOffer.NOT_RESERVED.apply(absentRole));
  }

  @Test
  public void testAllocateEmpty() {
    AcceptedOffer acceptedOffer = AcceptedOffer.create(
        fakeOffer(Collections.emptyList()),
        ResourceSlot.NONE,
        ResourceSlot.NONE,
        ImmutableSet.of(),
        TierInfo.DEFAULT);
    assertEquals(Collections.emptyList(), acceptedOffer.getTaskResources());
    assertEquals(Collections.emptyList(), acceptedOffer.getExecutorResources());
  }

  @Test
  public void testAllocateRange() {
    List<Resource> resources = ImmutableList.<Resource>builder()
        .add(makePortResource(Optional.absent(), 80, 81, 90, 91, 92, 93))
        .add(makePortResource(TEST_ROLE, 100, 101))
        .build();
    AcceptedOffer acceptedOffer = AcceptedOffer.create(
        fakeOffer(resources),
        ResourceSlot.NONE,
        ResourceSlot.NONE,
        ImmutableSet.of(80, 90, 100),
        TierInfo.DEFAULT);

    List<Resource> expected = ImmutableList.<Resource>builder()
        // Because we prefer reserved resources and handle them before non-reserved resources,
        // result should have ports for the reserved resources first.
        .add(makePortResource(TEST_ROLE, 100))
        .add(makePortResource(Optional.absent(), 80, 90))
        .build();
    assertEquals(expected, acceptedOffer.getTaskResources());
    assertEquals(Collections.emptyList(), acceptedOffer.getExecutorResources());
  }

  @Test(expected = Resources.InsufficientResourcesException.class)
  public void testAllocateRangeInsufficent() {
    List<Resource> resources = ImmutableList.of(
        makePortResource(ABSENT_ROLE, 80),
        makePortResource(ABSENT_ROLE, 100, 101));
    AcceptedOffer.create(
        fakeOffer(resources),
        ResourceSlot.NONE,
        ResourceSlot.NONE,
        ImmutableSet.of(80, 90, 100),
        TierInfo.DEFAULT);
  }

  @Test
  public void testAllocateSingleRole() {
    runAllocateSingleRole(ABSENT_ROLE, false);
    runAllocateSingleRole(ABSENT_ROLE, true);
    runAllocateSingleRole(TEST_ROLE, false);
    runAllocateSingleRole(TEST_ROLE, true);
  }

  private void runAllocateSingleRole(Optional<String> role, boolean cpuRevocable) {
    List<Resource> resources = ImmutableList.<Resource>builder()
        .add(makeScalar(
            ResourceType.CPUS.getName(), role, cpuRevocable, TOTAL_SLOT.getNumCpus()))
        .add(makeScalar(
            ResourceType.RAM_MB.getName(), role, false, TOTAL_SLOT.getRam().as(Data.MB)))
        .add(makeScalar(
            ResourceType.DISK_MB.getName(), role, false, TOTAL_SLOT.getDisk().as(Data.MB)))
        .add(makePortResource(role, TASK_PORTS))
        .build();
    Protos.Offer offer = fakeOffer(resources);

    AcceptedOffer offerAllocation = AcceptedOffer.create(
        offer, TASK_SLOT, EXECUTOR_SLOT, TASK_PORTS_SET, new TierInfo(cpuRevocable));

    List<Resource> taskList = ImmutableList.<Resource>builder()
        .add(makeScalar(ResourceType.CPUS.getName(), role, cpuRevocable, TASK_SLOT.getNumCpus()))
        .add(makeScalar(ResourceType.RAM_MB.getName(), role, false, TASK_SLOT.getRam().as(Data.MB)))
        .add(makeScalar(
            ResourceType.DISK_MB.getName(), role, false, TASK_SLOT.getDisk().as(Data.MB)))
        .add(makePortResource(role, TASK_PORTS))
        .build();
    assertEquals(taskList, offerAllocation.getTaskResources());

    List<Resource> executorList = ImmutableList.<Resource>builder()
        .add(makeScalar(
            ResourceType.CPUS.getName(), role, cpuRevocable, EXECUTOR_SLOT.getNumCpus()))
        .add(makeScalar(
            ResourceType.RAM_MB.getName(), role, false, EXECUTOR_SLOT.getRam().as(Data.MB)))
        .add(makeScalar(
            ResourceType.DISK_MB.getName(), role, false, EXECUTOR_SLOT.getDisk().as(Data.MB)))
        .build();
    assertEquals(executorList, offerAllocation.getExecutorResources());
  }

  @Test(expected = Resources.InsufficientResourcesException.class)
  public void testAllocateSingleRoleInsufficient() {
    List<Resource> resources = ImmutableList.<Resource>builder()
        // EXECUTOR_SLOT's CPU is not included here.
        .add(makeScalar(ResourceType.CPUS.getName(), TEST_ROLE, false, TASK_SLOT.getNumCpus()))
        .add(makeScalar(
            ResourceType.RAM_MB.getName(), TEST_ROLE, false, TOTAL_SLOT.getRam().as(Data.MB)))
        .add(makeScalar(
            ResourceType.DISK_MB.getName(), TEST_ROLE, false, TOTAL_SLOT.getDisk().as(Data.MB)))
        .add(makePortResource(TEST_ROLE, TASK_PORTS))
        .build();
    Protos.Offer offer = fakeOffer(resources);

    AcceptedOffer.create(
        offer, TASK_SLOT, EXECUTOR_SLOT, TASK_PORTS_SET, new TierInfo(false));
  }

  @Test
  public void testMultipleRoles() {
    runMultipleRoles(false);
    runMultipleRoles(true);
  }

  private void runMultipleRoles(boolean cpuRevocable) {
    List<Resource> resources = ImmutableList.<Resource>builder()
        // Make cpus come from two roles.
        .add(makeScalar(
            ResourceType.CPUS.getName(),
            TEST_ROLE,
            cpuRevocable,
            EXECUTOR_SLOT.getNumCpus()))
        .add(makeScalar(
            ResourceType.CPUS.getName(),
            ABSENT_ROLE,
            cpuRevocable,
            TASK_SLOT.getNumCpus()))
        // Make ram come from default role
        .add(makeScalar(
            ResourceType.RAM_MB.getName(),
            ABSENT_ROLE,
            false,
            TOTAL_SLOT.getRam().as(Data.MB)))
        // Make disk come from non-default role.
        .add(makeScalar(
            ResourceType.DISK_MB.getName(),
            TEST_ROLE,
            false,
            TOTAL_SLOT.getDisk().as(Data.MB)))
        .add(makePortResource(TEST_ROLE, 80))
        .add(makePortResource(ABSENT_ROLE, 90))
        .build();

    Protos.Offer offer = fakeOffer(resources);

    AcceptedOffer offerAllocation = AcceptedOffer.create(
        offer, TASK_SLOT, EXECUTOR_SLOT, TASK_PORTS_SET, new TierInfo(cpuRevocable));

    List<Resource> taskList = ImmutableList.<Resource>builder()
        // We intentionally sliced the offer resource to not align with TASK_SLOT's num cpus.
        .add(makeScalar(
            ResourceType.CPUS.getName(), TEST_ROLE, cpuRevocable, EXECUTOR_SLOT.getNumCpus()))
        .add(makeScalar(
            ResourceType.CPUS.getName(),
            ABSENT_ROLE,
            cpuRevocable,
            TASK_SLOT.subtract(EXECUTOR_SLOT).getNumCpus()))
        .add(makeScalar(
            ResourceType.RAM_MB.getName(), ABSENT_ROLE, false, TASK_SLOT.getRam().as(Data.MB)))
        .add(makeScalar(
            ResourceType.DISK_MB.getName(), TEST_ROLE, false, TASK_SLOT.getDisk().as(Data.MB)))
        .add(makePortResource(TEST_ROLE, 80))
        .add(makePortResource(ABSENT_ROLE, 90))
        .build();
    assertEquals(taskList, offerAllocation.getTaskResources());

    List<Resource> executorList = ImmutableList.<Resource>builder()
        .add(makeScalar(
            ResourceType.CPUS.getName(), ABSENT_ROLE, cpuRevocable, EXECUTOR_SLOT.getNumCpus()))
        .add(makeScalar(
            ResourceType.RAM_MB.getName(), ABSENT_ROLE, false, EXECUTOR_SLOT.getRam().as(Data.MB)))
        .add(makeScalar(
            ResourceType.DISK_MB.getName(), TEST_ROLE, false, EXECUTOR_SLOT.getDisk().as(Data.MB)))
        .build();
    assertEquals(executorList, offerAllocation.getExecutorResources());
  }

  @Test(expected = Resources.InsufficientResourcesException.class)
  public void testMultipleRolesInsufficient() {
    // Similar to testMultipleRoles, but make some of cpus as revocable
    List<Resource> resources = ImmutableList.<Resource>builder()
        // Make cpus come from two roles.
        .add(makeScalar(
            ResourceType.CPUS.getName(),
            TEST_ROLE,
            true,
            EXECUTOR_SLOT.getNumCpus()))
        .add(makeScalar(
            ResourceType.CPUS.getName(),
            ABSENT_ROLE,
            false,
            TASK_SLOT.getNumCpus()))
        // Make ram come from default role
        .add(makeScalar(
            ResourceType.RAM_MB.getName(),
            ABSENT_ROLE,
            false,
            TOTAL_SLOT.getRam().as(Data.MB)))
        // Make disk come from non-default role.
        .add(makeScalar(
            ResourceType.DISK_MB.getName(),
            TEST_ROLE,
            false,
            TOTAL_SLOT.getDisk().as(Data.MB)))
        .add(makePortResource(TEST_ROLE, 80))
        .add(makePortResource(ABSENT_ROLE, 90))
        .build();
    Protos.Offer offer = fakeOffer(resources);
    // We don't have enough resource to satisfy a non-revocable request.
    AcceptedOffer.create(
        offer, TASK_SLOT, EXECUTOR_SLOT, TASK_PORTS_SET, new TierInfo(false));
  }

  private static Resource makePortResource(Optional<String> role, Integer... values) {
    Resource.Builder prototype = Resource.newBuilder()
        .setType(Protos.Value.Type.RANGES)
        .setName(ResourceType.PORTS.getName());
    if (role.isPresent()) {
      prototype.setRole(role.get());
    }
    return AcceptedOffer.makeMesosRangeResource(prototype.build(), ImmutableSet.copyOf(values));
  }

  private static Resource makeScalar(
      String name, Optional<String> role, boolean revocable, double value) {
    Resource.Builder resource = Resource.newBuilder()
        .setName(name)
        .setType(Protos.Value.Type.SCALAR)
        .setScalar(Protos.Value.Scalar.newBuilder().setValue(value));
    if (role.isPresent()) {
      resource.setRole(role.get());
    }
    if (revocable) {
      resource.setRevocable(Resource.RevocableInfo.getDefaultInstance());
    }
    return resource.build();
  }

  private static Protos.Offer fakeOffer(List<Resource> resources) {
    return Protos.Offer.newBuilder()
        .setId(Protos.OfferID.newBuilder().setValue("offer-id"))
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework-id"))
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave-id"))
        .setHostname("hostname")
        .addAllResources(resources)
        .build();
  }
}
