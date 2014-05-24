/**
 *
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

import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.scheduler.Driver.DriverImpl;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.junit.Before;
import org.junit.Test;

import static org.apache.mesos.Protos.Status.DRIVER_ABORTED;
import static org.apache.mesos.Protos.Status.DRIVER_RUNNING;
import static org.easymock.EasyMock.expect;

public class DriverTest extends EasyMockTest {

  private static final String TASK_1 = "1";
  private static final String TASK_2 = "2";

  private SchedulerDriver schedulerDriver;
  private DriverImpl driver;

  private static Protos.TaskID createTaskId(String taskId) {
    return Protos.TaskID.newBuilder().setValue(taskId).build();
  }

  @Before
  public void setUp() {
    schedulerDriver = createMock(SchedulerDriver.class);
    driver = new DriverImpl();
  }

  @Test
  public void testNoopStop() {
    control.replay();

    driver.stop();
  }

  @Test
  public void testMultipleStops() {
    expect(schedulerDriver.stop(true)).andReturn(DRIVER_ABORTED);
    control.replay();

    driver.initialize(schedulerDriver);
    driver.stop();
    driver.stop();
  }

  @Test
  public void testStop() {
    expect(schedulerDriver.stop(true)).andReturn(DRIVER_ABORTED);
    control.replay();

    driver.initialize(schedulerDriver);
    driver.stop();
  }

  @Test
  public void testNormalLifecycle() {
    expect(schedulerDriver.killTask(createTaskId(TASK_1))).andReturn(DRIVER_RUNNING);
    expect(schedulerDriver.killTask(createTaskId(TASK_2))).andReturn(DRIVER_RUNNING);
    expect(schedulerDriver.stop(true)).andReturn(DRIVER_ABORTED);
    control.replay();

    driver.initialize(schedulerDriver);
    driver.killTask(TASK_1);
    driver.killTask(TASK_2);
    driver.stop();
  }

  @Test(expected = IllegalStateException.class)
  public void testMustRunBeforeKill() {
    control.replay();

    driver.killTask(TASK_1);
  }

  @Test(expected = IllegalStateException.class)
  public void testOnlyOneSetAllowed() {
    control.replay();

    driver.initialize(schedulerDriver);
    driver.initialize(schedulerDriver);
  }
}
