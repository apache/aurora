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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;
import org.junit.Test;

import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosRange;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.offer;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
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
    Protos.Resource withRole = mesosScalar(CPUS, TEST_ROLE, false, 1.0);
    assertTrue(AcceptedOffer.RESERVED.apply(withRole));
    assertFalse(AcceptedOffer.NOT_RESERVED.apply(withRole));
    Protos.Resource absentRole = mesosScalar(CPUS, ABSENT_ROLE, false, 1.0);
    assertFalse(AcceptedOffer.RESERVED.apply(absentRole));
    assertTrue(AcceptedOffer.NOT_RESERVED.apply(absentRole));
  }

  @Test
  public void testAllocateEmpty() {
    AcceptedOffer acceptedOffer = AcceptedOffer.create(
        offer(),
        ResourceSlot.NONE,
        ResourceSlot.NONE,
        ImmutableSet.of(),
        TaskTestUtil.DEV_TIER);
    assertEquals(Collections.emptyList(), acceptedOffer.getTaskResources());
    assertEquals(Collections.emptyList(), acceptedOffer.getExecutorResources());
  }

  @Test
  public void testAllocateRange() {
    AcceptedOffer acceptedOffer = AcceptedOffer.create(
        offer(
            mesosRange(PORTS, Optional.absent(), 80, 81, 90, 91, 92, 93),
            mesosRange(PORTS, TEST_ROLE, 100, 101)),
        ResourceSlot.NONE,
        ResourceSlot.NONE,
        ImmutableSet.of(80, 90, 100),
        TaskTestUtil.DEV_TIER);

    List<Resource> expected = ImmutableList.<Resource>builder()
        // Because we prefer reserved resources and handle them before non-reserved resources,
        // result should have ports for the reserved resources first.
        .add(mesosRange(PORTS, TEST_ROLE, 100))
        .add(mesosRange(PORTS, Optional.absent(), 80, 90))
        .build();
    assertEquals(expected, acceptedOffer.getTaskResources());
    assertEquals(Collections.emptyList(), acceptedOffer.getExecutorResources());
  }

  @Test(expected = Resources.InsufficientResourcesException.class)
  public void testAllocateRangeInsufficent() {
    AcceptedOffer.create(
        offer(
            mesosRange(PORTS, ABSENT_ROLE, 80),
            mesosRange(PORTS, ABSENT_ROLE, 100, 101)),
        ResourceSlot.NONE,
        ResourceSlot.NONE,
        ImmutableSet.of(80, 90, 100),
        TaskTestUtil.DEV_TIER);
  }

  @Test
  public void testAllocateSingleRole() {
    runAllocateSingleRole(ABSENT_ROLE, false);
    runAllocateSingleRole(ABSENT_ROLE, true);
    runAllocateSingleRole(TEST_ROLE, false);
    runAllocateSingleRole(TEST_ROLE, true);
  }

  private void runAllocateSingleRole(Optional<String> role, boolean cpuRevocable) {
    Protos.Offer offer = offer(
        mesosScalar(CPUS, role, cpuRevocable, TOTAL_SLOT.getNumCpus()),
        mesosScalar(RAM_MB, role, false, TOTAL_SLOT.getRam().as(Data.MB)),
        mesosScalar(DISK_MB, role, false, TOTAL_SLOT.getDisk().as(Data.MB)),
        mesosRange(PORTS, role, TASK_PORTS));

    AcceptedOffer offerAllocation = AcceptedOffer.create(
        offer, TASK_SLOT, EXECUTOR_SLOT, TASK_PORTS_SET, new TierInfo(false, cpuRevocable));

    List<Resource> taskList = ImmutableList.<Resource>builder()
        .add(mesosScalar(CPUS, role, cpuRevocable, TASK_SLOT.getNumCpus()))
        .add(mesosScalar(RAM_MB, role, false, TASK_SLOT.getRam().as(Data.MB)))
        .add(mesosScalar(
            DISK_MB, role, false, TASK_SLOT.getDisk().as(Data.MB)))
        .add(mesosRange(PORTS, role, TASK_PORTS))
        .build();
    assertEquals(taskList, offerAllocation.getTaskResources());

    List<Resource> executorList = ImmutableList.<Resource>builder()
        .add(mesosScalar(
            CPUS, role, cpuRevocable, EXECUTOR_SLOT.getNumCpus()))
        .add(mesosScalar(
            RAM_MB, role, false, EXECUTOR_SLOT.getRam().as(Data.MB)))
        .add(mesosScalar(
            DISK_MB, role, false, EXECUTOR_SLOT.getDisk().as(Data.MB)))
        .build();
    assertEquals(executorList, offerAllocation.getExecutorResources());
  }

  @Test(expected = Resources.InsufficientResourcesException.class)
  public void testAllocateSingleRoleInsufficient() {
    Protos.Offer offer = offer(
        // EXECUTOR_SLOT's CPU is not included here.
        mesosScalar(CPUS, TEST_ROLE, false, TASK_SLOT.getNumCpus()),
        mesosScalar(RAM_MB, TEST_ROLE, false, TOTAL_SLOT.getRam().as(Data.MB)),
        mesosScalar(DISK_MB, TEST_ROLE, false, TOTAL_SLOT.getDisk().as(Data.MB)),
        mesosRange(PORTS, TEST_ROLE, TASK_PORTS));

    AcceptedOffer.create(
        offer, TASK_SLOT, EXECUTOR_SLOT, TASK_PORTS_SET, new TierInfo(false, false));
  }

  @Test
  public void testMultipleRoles() {
    runMultipleRoles(false);
    runMultipleRoles(true);
  }

  private void runMultipleRoles(boolean cpuRevocable) {
    Protos.Offer offer = offer(
        // Make cpus come from two roles.
        mesosScalar(CPUS, TEST_ROLE, cpuRevocable, EXECUTOR_SLOT.getNumCpus()),
        mesosScalar(CPUS, ABSENT_ROLE, cpuRevocable, TASK_SLOT.getNumCpus()),
        // Make ram come from default role
        mesosScalar(RAM_MB, ABSENT_ROLE, false, TOTAL_SLOT.getRam().as(Data.MB)),
        // Make disk come from non-default role.
        mesosScalar(DISK_MB, TEST_ROLE, false, TOTAL_SLOT.getDisk().as(Data.MB)),
        mesosRange(PORTS, TEST_ROLE, 80),
        mesosRange(PORTS, ABSENT_ROLE, 90));

    AcceptedOffer offerAllocation = AcceptedOffer.create(
        offer, TASK_SLOT, EXECUTOR_SLOT, TASK_PORTS_SET, new TierInfo(false, cpuRevocable));

    List<Resource> taskList = ImmutableList.<Resource>builder()
        // We intentionally sliced the offer resource to not align with TASK_SLOT's num cpus.
        .add(mesosScalar(
            CPUS, TEST_ROLE, cpuRevocable, EXECUTOR_SLOT.getNumCpus()))
        .add(mesosScalar(
            CPUS,
            ABSENT_ROLE,
            cpuRevocable,
            TASK_SLOT.subtract(EXECUTOR_SLOT).getNumCpus()))
        .add(mesosScalar(
            RAM_MB, ABSENT_ROLE, false, TASK_SLOT.getRam().as(Data.MB)))
        .add(mesosScalar(
            DISK_MB, TEST_ROLE, false, TASK_SLOT.getDisk().as(Data.MB)))
        .add(mesosRange(PORTS, TEST_ROLE, 80))
        .add(mesosRange(PORTS, ABSENT_ROLE, 90))
        .build();
    assertEquals(taskList, offerAllocation.getTaskResources());

    List<Resource> executorList = ImmutableList.<Resource>builder()
        .add(mesosScalar(
            CPUS, ABSENT_ROLE, cpuRevocable, EXECUTOR_SLOT.getNumCpus()))
        .add(mesosScalar(
            RAM_MB, ABSENT_ROLE, false, EXECUTOR_SLOT.getRam().as(Data.MB)))
        .add(mesosScalar(
            DISK_MB, TEST_ROLE, false, EXECUTOR_SLOT.getDisk().as(Data.MB)))
        .build();
    assertEquals(executorList, offerAllocation.getExecutorResources());
  }

  @Test(expected = Resources.InsufficientResourcesException.class)
  public void testMultipleRolesInsufficient() {
    Protos.Offer offer = offer(
        // Similar to testMultipleRoles, but make some of cpus as revocable
        // Make cpus come from two roles.
        mesosScalar(CPUS, TEST_ROLE, true, EXECUTOR_SLOT.getNumCpus()),
        mesosScalar(CPUS, ABSENT_ROLE, false, TASK_SLOT.getNumCpus()),
        // Make ram come from default role
        mesosScalar(RAM_MB, ABSENT_ROLE, false, TOTAL_SLOT.getRam().as(Data.MB)),
        // Make disk come from non-default role.
        mesosScalar(DISK_MB, TEST_ROLE, false, TOTAL_SLOT.getDisk().as(Data.MB)),
        mesosRange(PORTS, TEST_ROLE, 80),
        mesosRange(PORTS, ABSENT_ROLE, 90));
    // We don't have enough resource to satisfy a non-revocable request.
    AcceptedOffer.create(
        offer, TASK_SLOT, EXECUTOR_SLOT, TASK_PORTS_SET, new TierInfo(false, false));
  }
}
