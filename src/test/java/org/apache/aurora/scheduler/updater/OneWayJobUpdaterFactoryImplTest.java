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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobUpdateConfiguration;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateConfiguration;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.updater.OneWayJobUpdaterFactory.UpdateConfigurationException;
import static org.junit.Assert.assertEquals;

/**
 * This test can't exercise much functionality of the output from the factory without duplicating
 * test behavior in the job updater integration test.  So instead, we test only some basic behavior.
 */
public class OneWayJobUpdaterFactoryImplTest {

  private static final IJobUpdateConfiguration CONFIG = IJobUpdateConfiguration.build(
      new JobUpdateConfiguration()
          .setNewTaskConfig(new TaskConfig())
          .setInstanceCount(3)
          .setOldTaskConfigs(ImmutableSet.of(new InstanceTaskConfig()
              .setInstances(ImmutableSet.of(new Range(1, 2)))
              .setTask(new TaskConfig())))
          .setSettings(new JobUpdateSettings()
              .setMaxFailedInstances(1)
              .setMaxPerInstanceFailures(1)
              .setMaxWaitToInstanceRunningMs(100)
              .setMinWaitInInstanceRunningMs(100)
              .setUpdateGroupSize(2)
              .setUpdateOnlyTheseInstances(ImmutableSet.<Range>of())));

  private OneWayJobUpdaterFactory factory;

  @Before
  public void setUp() {
    factory = new OneWayJobUpdaterFactory.OneWayJobUpdaterFactoryImpl(new FakeClock());
  }

  @Test
  public void testRollingForward() throws Exception  {
    OneWayJobUpdater<Integer, Optional<IScheduledTask>> update = factory.newUpdate(CONFIG, true);
    assertEquals(ImmutableSet.of(0, 1, 2), update.getInstances());
  }

  @Test
  public void testRollingBack() throws Exception {
    OneWayJobUpdater<Integer, Optional<IScheduledTask>> update = factory.newUpdate(CONFIG, false);
    assertEquals(ImmutableSet.of(0, 1, 2), update.getInstances());
  }

  @Test
  public void testRollForwardSpecificInstances() throws Exception {
    JobUpdateConfiguration config = CONFIG.newBuilder();
    config.getSettings().setUpdateOnlyTheseInstances(ImmutableSet.of(new Range(1, 2)));

    OneWayJobUpdater<Integer, Optional<IScheduledTask>> update =
        factory.newUpdate(IJobUpdateConfiguration.build(config), true);
    assertEquals(ImmutableSet.of(1, 2), update.getInstances());
  }

  @Test
  public void testRollBackSpecificInstances() throws Exception {
    JobUpdateConfiguration config = CONFIG.newBuilder();
    config.getSettings().setUpdateOnlyTheseInstances(ImmutableSet.of(new Range(1, 2)));

    OneWayJobUpdater<Integer, Optional<IScheduledTask>> update =
        factory.newUpdate(IJobUpdateConfiguration.build(config), false);
    assertEquals(ImmutableSet.of(1, 2), update.getInstances());
  }

  @Test(expected = UpdateConfigurationException.class)
  public void testInvalidConfiguration() throws Exception {
    JobUpdateConfiguration config = CONFIG.newBuilder();
    config.getSettings().setUpdateOnlyTheseInstances(ImmutableSet.of(new Range(10, 10)));

    factory.newUpdate(IJobUpdateConfiguration.build(config), true);
  }

  @Test
  public void testUpdateRemovesInstance() throws Exception {
    JobUpdateConfiguration config = CONFIG.newBuilder();
    config.setInstanceCount(2);

    OneWayJobUpdater<Integer, Optional<IScheduledTask>> update =
        factory.newUpdate(IJobUpdateConfiguration.build(config), true);
    assertEquals(ImmutableSet.of(0, 1, 2), update.getInstances());
  }
}
