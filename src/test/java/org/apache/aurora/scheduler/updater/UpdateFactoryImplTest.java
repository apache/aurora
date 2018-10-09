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
package org.apache.aurora.scheduler.updater;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateStrategy;
import org.apache.aurora.gen.QueueJobUpdateStrategy;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.updater.UpdateFactory.Update;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test can't exercise much functionality of the output from the factory without duplicating
 * test behavior in the job updater integration test. So instead, we test only some basic behavior.
 */
public class UpdateFactoryImplTest {

  private static final IJobUpdateInstructions INSTRUCTIONS = IJobUpdateInstructions.build(
      new JobUpdateInstructions()
          .setDesiredState(instanceConfig(new Range(0, 2)))
          .setInitialState(ImmutableSet.of(instanceConfig(new Range(1, 2))))
          .setSettings(new JobUpdateSettings()
              .setMaxFailedInstances(1)
              .setMaxPerInstanceFailures(1)
              .setMinWaitInInstanceRunningMs(100)
              .setUpdateStrategy(
                  JobUpdateStrategy.queueStrategy(new QueueJobUpdateStrategy().setGroupSize(2)))
              .setUpdateOnlyTheseInstances(ImmutableSet.of())));

  private UpdateFactory factory;

  @Before
  public void setUp() {
    factory = new UpdateFactory.UpdateFactoryImpl(new FakeClock());
  }

  @Test
  public void testRollingForward() throws Exception  {
    Update update = factory.newUpdate(INSTRUCTIONS, true);
    assertEquals(ImmutableSet.of(0, 1, 2), update.getUpdater().getInstances());
  }

  @Test
  public void testRollingBack() throws Exception {
    Update update = factory.newUpdate(INSTRUCTIONS, false);
    assertEquals(ImmutableSet.of(0, 1, 2), update.getUpdater().getInstances());
  }

  @Test
  public void testRollForwardSpecificInstances() throws Exception {
    JobUpdateInstructions config = INSTRUCTIONS.newBuilder();
    config.setInitialState(ImmutableSet.of());
    config.setDesiredState(instanceConfig(new Range(1, 1)));
    config.getSettings().setUpdateOnlyTheseInstances(ImmutableSet.of(new Range(0, 1)));

    Update update = factory.newUpdate(IJobUpdateInstructions.build(config), true);
    assertEquals(ImmutableSet.of(1), update.getUpdater().getInstances());
  }

  @Test
  public void testRollBackSpecificInstances() throws Exception {
    JobUpdateInstructions config = INSTRUCTIONS.newBuilder();
    config.setInitialState(ImmutableSet.of());
    config.setDesiredState(instanceConfig(new Range(1, 1)));
    config.getSettings().setUpdateOnlyTheseInstances(ImmutableSet.of(new Range(0, 1)));

    Update update = factory.newUpdate(IJobUpdateInstructions.build(config), false);
    assertEquals(ImmutableSet.of(1), update.getUpdater().getInstances());
  }

  @Test
  public void testUpdateRemovesInstance() throws Exception {
    JobUpdateInstructions config = INSTRUCTIONS.newBuilder();
    config.getDesiredState().setInstances(ImmutableSet.of(new Range(0, 1)));

    Update update = factory.newUpdate(IJobUpdateInstructions.build(config), true);
    assertEquals(ImmutableSet.of(0, 1, 2), update.getUpdater().getInstances());
  }

  @Test
  public void testUpdateOrdering() throws Exception {
    Set<Integer> currentInstances = ImmutableSet.of(0, 1, 2, 3);
    Set<Integer> desiredInstances = ImmutableSet.of(0, 1, 2, 3);

    UpdateFactory.UpdateOrdering ordering = new UpdateFactory.UpdateOrdering(
        currentInstances,
        desiredInstances);

    // Rolling forward
    assertTrue(ordering.isOrdered(Lists.newArrayList(0, 1, 2, 3)));

    // Rolling backward
    assertTrue(ordering.reverse().isOrdered(Lists.newArrayList(3, 2, 1, 0)));
  }

  @Test
  public void testUpdateCreateOrdering() throws Exception {
    Set<Integer> currentInstances = ImmutableSet.of(0, 1, 2, 3);
    Set<Integer> desiredInstances = ImmutableSet.of(0, 1, 2, 3, 4, 5);

    UpdateFactory.UpdateOrdering ordering = new UpdateFactory.UpdateOrdering(
        currentInstances,
        desiredInstances);

    // Rolling forward
    assertTrue(ordering.isOrdered(Lists.newArrayList(4, 5, 0, 1, 2, 3)));

    // Rolling backward
    assertTrue(ordering.reverse().isOrdered(Lists.newArrayList(3, 2, 1, 0, 5, 4)));
  }

  @Test
  public void testUpdateKillOrdering() throws Exception {
    Set<Integer> currentInstances = ImmutableSet.of(0, 1, 2, 3);
    Set<Integer> desiredInstances = ImmutableSet.of(0, 1);

    UpdateFactory.UpdateOrdering ordering = new UpdateFactory.UpdateOrdering(
        currentInstances,
        desiredInstances);

    // Rolling forward
    assertTrue(ordering.isOrdered(Lists.newArrayList(0, 1, 2, 3)));

    // Rolling backward
    assertTrue(ordering.reverse().isOrdered(Lists.newArrayList(3, 2, 1, 0)));
  }

  @Test
  public void testCreateUpdateKillOrdering() throws Exception {
    Set<Integer> currentInstances = ImmutableSet.of(0, 2, 4, 5, 6);
    Set<Integer> desiredInstances = ImmutableSet.of(0, 1, 2, 3);

    UpdateFactory.UpdateOrdering ordering = new UpdateFactory.UpdateOrdering(
        currentInstances,
        desiredInstances);

    // Rolling forward
    assertTrue(ordering.isOrdered(Lists.newArrayList(1, 3, 0, 2, 5, 6)));

    // Rolling backward
    assertTrue(ordering.reverse().isOrdered(Lists.newArrayList(6, 5, 2, 0, 3, 1)));
  }

  private static InstanceTaskConfig instanceConfig(Range instances) {
    return new InstanceTaskConfig()
        .setTask(new TaskConfig())
        .setInstances(ImmutableSet.of(instances));
  }
}
