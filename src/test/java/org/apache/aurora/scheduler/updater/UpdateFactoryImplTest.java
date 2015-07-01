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

import com.google.common.collect.ImmutableSet;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.updater.UpdateFactory.Update;
import static org.junit.Assert.assertEquals;

/**
 * This test can't exercise much functionality of the output from the factory without duplicating
 * test behavior in the job updater integration test.  So instead, we test only some basic behavior.
 */
public class UpdateFactoryImplTest {

  private static final IJobUpdateInstructions INSTRUCTIONS = IJobUpdateInstructions.build(
      new JobUpdateInstructions()
          .setDesiredState(new InstanceTaskConfig()
              .setTask(new TaskConfig())
              .setInstances(ImmutableSet.of(new Range(0, 2))))
          .setInitialState(ImmutableSet.of(new InstanceTaskConfig()
              .setTask(new TaskConfig())
              .setInstances(ImmutableSet.of(new Range(1, 2)))))
          .setSettings(new JobUpdateSettings()
              .setMaxFailedInstances(1)
              .setMaxPerInstanceFailures(1)
              .setMaxWaitToInstanceRunningMs(100)
              .setMinWaitInInstanceRunningMs(100)
              .setUpdateGroupSize(2)
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
    config.getSettings().setUpdateOnlyTheseInstances(ImmutableSet.of(new Range(1, 2)));

    Update update = factory.newUpdate(IJobUpdateInstructions.build(config), true);
    assertEquals(ImmutableSet.of(1, 2), update.getUpdater().getInstances());
  }

  @Test
  public void testRollBackSpecificInstances() throws Exception {
    JobUpdateInstructions config = INSTRUCTIONS.newBuilder();
    config.getSettings().setUpdateOnlyTheseInstances(ImmutableSet.of(new Range(1, 2)));

    Update update = factory.newUpdate(IJobUpdateInstructions.build(config), false);
    assertEquals(ImmutableSet.of(1, 2), update.getUpdater().getInstances());
  }

  @Test
  public void testUpdateRemovesInstance() throws Exception {
    JobUpdateInstructions config = INSTRUCTIONS.newBuilder();
    config.getDesiredState().setInstances(ImmutableSet.of(new Range(0, 1)));

    Update update = factory.newUpdate(IJobUpdateInstructions.build(config), true);
    assertEquals(ImmutableSet.of(0, 1, 2), update.getUpdater().getInstances());
  }
}
