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
package org.apache.aurora.scheduler.mesos;

import java.util.Optional;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.junit.Before;
import org.junit.Test;

import static org.apache.mesos.Protos.Status.DRIVER_ABORTED;
import static org.apache.mesos.Protos.Status.DRIVER_RUNNING;
import static org.apache.mesos.Protos.TaskID;
import static org.apache.mesos.v1.Protos.FrameworkID;
import static org.apache.mesos.v1.Protos.FrameworkInfo;
import static org.easymock.EasyMock.expect;

public class SchedulerDriverServiceTest extends EasyMockTest {

  private static final Optional<String> FRAMEWORK_ID = Optional.of("test framework");
  private static final Optional<String> NEW_FRAMEWORK_ID = Optional.empty();

  private static final FrameworkInfo BASE_INFO = FrameworkInfo.newBuilder()
          .setUser("framework user")
          .setName("test framework")
          .build();
  private static final DriverSettings SETTINGS = new DriverSettings(
      "fakemaster",
      Optional.empty());

  private static final String TASK_1 = "1";
  private static final String TASK_2 = "2";

  private Scheduler scheduler;
  private StorageTestUtil storage;
  private DriverFactory driverFactory;
  private Driver driverService;
  private SchedulerDriver schedulerDriver;
  private FrameworkInfoFactory infoFactory;

  private static TaskID createTaskId(String taskId) {
    return TaskID.newBuilder().setValue(taskId).build();
  }

  @Before
  public void setUp() {
    scheduler = createMock(Scheduler.class);
    storage = new StorageTestUtil(this);
    driverFactory = createMock(DriverFactory.class);
    schedulerDriver = createMock(SchedulerDriver.class);
    infoFactory = createMock(FrameworkInfoFactory.class);
    driverService = new SchedulerDriverService(
        scheduler,
        storage.storage,
        SETTINGS,
        driverFactory,
        infoFactory);
  }

  @Test
  public void testNoopStop() {
    control.replay();

    driverService.stopAsync().awaitTerminated();
  }

  @Test
  public void testMultipleStops() {
    expectCreateDriver(NEW_FRAMEWORK_ID);
    expect(schedulerDriver.stop(true)).andReturn(DRIVER_ABORTED);
    control.replay();

    driverService.startAsync().awaitRunning();
    driverService.stopAsync().awaitTerminated();
    driverService.stopAsync().awaitTerminated();
  }

  @Test
  public void testStartNewFramework() {
    expectCreateDriver(NEW_FRAMEWORK_ID);
    control.replay();

    driverService.startAsync().awaitRunning();
  }

  @Test
  public void testStop() {
    expectCreateDriver(FRAMEWORK_ID);
    expect(schedulerDriver.stop(true)).andReturn(DRIVER_ABORTED);
    control.replay();

    driverService.startAsync().awaitRunning();
    driverService.stopAsync().awaitTerminated();
  }

  @Test
  public void testNormalLifecycle() {
    expectCreateDriver(NEW_FRAMEWORK_ID);
    expect(schedulerDriver.killTask(createTaskId(TASK_1))).andReturn(DRIVER_RUNNING);
    expect(schedulerDriver.killTask(createTaskId(TASK_2))).andReturn(DRIVER_RUNNING);
    expect(schedulerDriver.stop(true)).andReturn(DRIVER_ABORTED);
    control.replay();

    driverService.startAsync().awaitRunning();
    driverService.killTask(TASK_1);
    driverService.killTask(TASK_2);
    driverService.stopAsync().awaitTerminated();
  }

  @Test(expected = IllegalStateException.class)
  public void testMustRunBeforeKill() {
    control.replay();

    driverService.killTask(TASK_1);
  }

  @Test(expected = IllegalStateException.class)
  public void testMultipleStarts() {
    expectCreateDriver(NEW_FRAMEWORK_ID);

    control.replay();

    driverService.startAsync().awaitRunning();
    driverService.startAsync().awaitRunning();
  }

  private void expectCreateDriver(Optional<String> frameworkId) {
    storage.expectOperations();
    expect(storage.schedulerStore.fetchFrameworkId()).andReturn(frameworkId);
    expect(infoFactory.getFrameworkInfo()).andReturn(BASE_INFO);

    FrameworkInfo.Builder builder = BASE_INFO.toBuilder();
    if (frameworkId.isPresent()) {
      builder.setId(FrameworkID.newBuilder().setValue(frameworkId.get()));
    }

    expect(driverFactory.create(
        scheduler,
        SETTINGS.getCredentials(),
        builder.build(),
        SETTINGS.getMasterUri()))
        .andReturn(schedulerDriver);
    expect(schedulerDriver.start()).andReturn(DRIVER_RUNNING);
  }
}
