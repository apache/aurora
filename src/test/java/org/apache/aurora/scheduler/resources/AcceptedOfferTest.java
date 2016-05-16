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
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;
import org.junit.Test;

import static org.apache.aurora.scheduler.base.TaskTestUtil.DEV_TIER;
import static org.apache.aurora.scheduler.base.TaskTestUtil.JOB;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.apache.aurora.scheduler.resources.ResourceManager.bagFromResources;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.bag;
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
  private static final IAssignedTask TASK = makeTask("id", JOB).getAssignedTask();
  private static final ResourceBag EXECUTOR_BAG = bag(0.25, 25, 75);
  private static final ResourceBag TOTAL_BAG =
      EXECUTOR_BAG.add(bagFromResources(TASK.getTask().getResources()));
  private static final Integer[] TASK_PORTS = {TASK.getAssignedPorts().get("http")};

  @Test
  public void testReservedPredicates() {
    Protos.Resource withRole = mesosScalar(CPUS, TEST_ROLE, false, 1.0);
    assertTrue(AcceptedOffer.RESERVED.test(withRole));
    assertFalse(AcceptedOffer.RESERVED.negate().test(withRole));
    Protos.Resource absentRole = mesosScalar(CPUS, ABSENT_ROLE, false, 1.0);
    assertFalse(AcceptedOffer.RESERVED.test(absentRole));
    assertTrue(AcceptedOffer.RESERVED.negate().test(absentRole));
  }

  @Test
  public void testAllocateEmpty() {
    AcceptedOffer acceptedOffer = AcceptedOffer.create(
        offer(),
        IAssignedTask.build(new AssignedTask().setTask(new TaskConfig())),
        ResourceBag.EMPTY,
        DEV_TIER);
    assertEquals(Collections.emptyList(), acceptedOffer.getTaskResources());
    assertEquals(Collections.emptyList(), acceptedOffer.getExecutorResources());
  }

  @Test
  public void testAllocateSingleRole() {
    runAllocateSingleRole(ABSENT_ROLE, false);
    runAllocateSingleRole(ABSENT_ROLE, true);
    runAllocateSingleRole(TEST_ROLE, false);
    runAllocateSingleRole(TEST_ROLE, true);
  }

  private void runAllocateSingleRole(Optional<String> role, boolean revocable) {
    Protos.Offer offer = offer(
        mesosScalar(CPUS, TOTAL_BAG.valueOf(CPUS), revocable),
        mesosScalar(RAM_MB, TOTAL_BAG.valueOf(RAM_MB), false),
        mesosScalar(DISK_MB, TOTAL_BAG.valueOf(DISK_MB), false),
        mesosRange(PORTS, role, TASK_PORTS));

    AcceptedOffer offerAllocation = AcceptedOffer.create(
        offer, TASK, EXECUTOR_BAG, new TierInfo(false, revocable));

    ResourceBag bag = bagFromResources(TASK.getTask().getResources());
    Set<Resource> taskResources = ImmutableSet.<Resource>builder()
        .add(mesosScalar(CPUS, bag.valueOf(CPUS), revocable))
        .add(mesosScalar(RAM_MB, bag.valueOf(RAM_MB), false))
        .add(mesosScalar(DISK_MB, bag.valueOf(DISK_MB), false))
        .add(mesosRange(PORTS, role, TASK_PORTS))
        .build();
    assertEquals(taskResources, ImmutableSet.copyOf(offerAllocation.getTaskResources()));

    Set<Resource> executorResources = ImmutableSet.<Resource>builder()
        .add(mesosScalar(CPUS, EXECUTOR_BAG.valueOf(CPUS), revocable))
        .add(mesosScalar(RAM_MB, EXECUTOR_BAG.valueOf(RAM_MB), false))
        .add(mesosScalar(DISK_MB, EXECUTOR_BAG.valueOf(DISK_MB), false))
        .build();
    assertEquals(executorResources, ImmutableSet.copyOf(offerAllocation.getExecutorResources()));
  }

  @Test
  public void testMultipleRoles() {
    runMultipleRoles(false);
    runMultipleRoles(true);
  }

  private void runMultipleRoles(boolean revocable) {
    ResourceBag bag = bagFromResources(TASK.getTask().getResources());
    Protos.Offer offer = offer(
        // Make cpus come from two roles.
        mesosScalar(CPUS, TEST_ROLE, revocable, EXECUTOR_BAG.valueOf(CPUS)),
        mesosScalar(CPUS, ABSENT_ROLE, revocable, bag.valueOf(CPUS)),
        // Make ram come from default role
        mesosScalar(RAM_MB, ABSENT_ROLE, false, TOTAL_BAG.valueOf(RAM_MB)),
        // Make disk come from non-default role.
        mesosScalar(DISK_MB, TEST_ROLE, false, TOTAL_BAG.valueOf(DISK_MB)),
        mesosRange(PORTS, TEST_ROLE, TASK_PORTS));

    AcceptedOffer offerAllocation = AcceptedOffer.create(
        offer, TASK, EXECUTOR_BAG, new TierInfo(false, revocable));

    Set<Resource> taskSet = ImmutableSet.<Resource>builder()
        .add(mesosScalar(CPUS, TEST_ROLE, revocable, EXECUTOR_BAG.valueOf(CPUS)))
        .add(mesosScalar(
            CPUS,
            ABSENT_ROLE,
            revocable,
            bag.subtract(EXECUTOR_BAG).valueOf(CPUS)))
        .add(mesosScalar(RAM_MB, ABSENT_ROLE, false, bag.valueOf(RAM_MB)))
        .add(mesosScalar(DISK_MB, TEST_ROLE, false, bag.valueOf(DISK_MB)))
        .add(mesosRange(PORTS, TEST_ROLE, TASK_PORTS))
        .build();
    assertEquals(taskSet, ImmutableSet.copyOf(offerAllocation.getTaskResources()));

    Set<Resource> executorSet = ImmutableSet.<Resource>builder()
        .add(mesosScalar(CPUS, ABSENT_ROLE, revocable, EXECUTOR_BAG.valueOf(CPUS)))
        .add(mesosScalar(RAM_MB, ABSENT_ROLE, false, EXECUTOR_BAG.valueOf(RAM_MB)))
        .add(mesosScalar(DISK_MB, TEST_ROLE, false, EXECUTOR_BAG.valueOf(DISK_MB)))
        .build();
    assertEquals(executorSet, ImmutableSet.copyOf(offerAllocation.getExecutorResources()));
  }
}
