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
package org.apache.aurora.scheduler.async;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Optional;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.comm.AdjustRetainedTasks;
import org.apache.aurora.scheduler.Driver;
import org.apache.aurora.scheduler.async.GcExecutorLauncher.GcExecutorSettings;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.apache.aurora.gen.ScheduleStatus.SANDBOX_DELETED;
import static org.apache.aurora.scheduler.async.GcExecutorLauncher.SYSTEM_TASK_PREFIX;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GcExecutorLauncherTest extends EasyMockTest {

  private static final String HOST = "slave-host";

  private static final Offer OFFER = Offer.newBuilder()
      .setSlaveId(SlaveID.newBuilder().setValue("slave-id"))
      .setHostname(HOST)
      .setFrameworkId(FrameworkID.newBuilder().setValue("framework-id").build())
      .setId(OfferID.newBuilder().setValue("offer-id"))
      .addAllResources(GcExecutorLauncher.TOTAL_GC_EXECUTOR_RESOURCES.toResourceList())
      .build();

  private static final String JOB_A = "jobA";

  private static final GcExecutorSettings SETTINGS =
      new GcExecutorSettings(Amount.of(1L, Time.HOURS), Optional.of("nonempty"));

  private final AtomicInteger taskIdCounter = new AtomicInteger();

  private FakeClock clock;
  private StorageTestUtil storageUtil;
  private Driver driver;
  private GcExecutorLauncher gcExecutorLauncher;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    clock = new FakeClock();
    storageUtil.expectOperations();
    driver = createMock(Driver.class);
    gcExecutorLauncher = new GcExecutorLauncher(
        SETTINGS,
        storageUtil.storage,
        clock,
        MoreExecutors.sameThreadExecutor(),
        driver,
        Suppliers.ofInstance("gc"));
  }

  @Test
  public void testPruning() throws Exception {
    IScheduledTask a = makeTask(JOB_A, FAILED);
    IScheduledTask b = makeTask(JOB_A, FAILED);
    IScheduledTask c = makeTask(JOB_A, FAILED);

    // First call - no tasks to be collected.
    expectGetTasksByHost(HOST, a, b, c);
    expectAdjustRetainedTasks(a, b, c);

    // Third call - two tasks collected.
    expectGetTasksByHost(HOST, a);
    expectAdjustRetainedTasks(a);

    control.replay();

    // First call - no items in the cache, no tasks collected.
    assertTrue(gcExecutorLauncher.willUse(OFFER));

    // Second call - host item alive, no tasks collected.
    clock.advance(Amount.of((long) SETTINGS.getDelayMs() - 1, Time.MILLISECONDS));
    assertFalse(gcExecutorLauncher.willUse(OFFER));

    // Third call - two tasks collected.
    clock.advance(Amount.of(15L, Time.MINUTES));
    assertTrue(gcExecutorLauncher.willUse(OFFER));
  }

  @Test
  public void testNoAcceptingSmallOffers() {
    control.replay();

    Iterable<Resource> resources =
        Resources.subtract(
            GcExecutorLauncher.TOTAL_GC_EXECUTOR_RESOURCES,
            GcExecutorLauncher.EPSILON).toResourceList();
    Offer smallOffer = OFFER.toBuilder()
        .clearResources()
        .addAllResources(resources)
        .build();
    assertFalse(gcExecutorLauncher.willUse(smallOffer));
  }

  private static TaskStatus makeStatus(String taskId) {
    return TaskStatus.newBuilder()
        .setSlaveId(OFFER.getSlaveId())
        .setState(TaskState.TASK_RUNNING)
        .setTaskId(TaskID.newBuilder().setValue(taskId))
        .build();
  }

  @Test
  public void testStatusUpdate() {
    control.replay();

    assertTrue(gcExecutorLauncher.statusUpdate(makeStatus(SYSTEM_TASK_PREFIX)));
    assertTrue(gcExecutorLauncher.statusUpdate(makeStatus(SYSTEM_TASK_PREFIX + "1")));
    assertFalse(gcExecutorLauncher.statusUpdate(makeStatus("1" + SYSTEM_TASK_PREFIX)));
    assertFalse(gcExecutorLauncher.statusUpdate(makeStatus("asdf")));
  }

  @Test
  public void testGcExecutorDisabled() {
    control.replay();

    gcExecutorLauncher = new GcExecutorLauncher(
        new GcExecutorSettings(Amount.of(1L, Time.HOURS), Optional.<String>absent()),
        storageUtil.storage,
        clock,
        MoreExecutors.sameThreadExecutor(),
        driver,
        Suppliers.ofInstance("gc"));
    assertFalse(gcExecutorLauncher.willUse(OFFER));
  }

  @Test
  public void testFiltersSandboxDeleted() {
    IScheduledTask a = makeTask(JOB_A, FAILED);
    IScheduledTask b = makeTask(JOB_A, SANDBOX_DELETED);

    expectGetTasksByHost(HOST, a, b);
    expectAdjustRetainedTasks(a);

    control.replay();

    // First call - no items in the cache, no tasks collected.
    assertTrue(gcExecutorLauncher.willUse(OFFER));
  }

  private void expectAdjustRetainedTasks(IScheduledTask... tasks) {
    Map<String, ScheduleStatus> statuses =
        Maps.transformValues(Tasks.mapById(ImmutableSet.copyOf(tasks)), Tasks.GET_STATUS);
    AdjustRetainedTasks message = new AdjustRetainedTasks().setRetainedTasks(statuses);
    TaskInfo task = gcExecutorLauncher.makeGcTask(HOST, OFFER.getSlaveId(), message);
    driver.launchTask(OFFER.getId(), task);
  }

  private IScheduledTask makeTask(String jobName, ScheduleStatus status) {
    return IScheduledTask.build(new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask()
            .setTaskId("task-" + taskIdCounter.incrementAndGet())
            .setSlaveHost(HOST)
            .setTask(new TaskConfig()
                .setJobName(jobName)
                .setOwner(new Identity().setRole("role").setUser("user"))
                .setExecutorConfig(new ExecutorConfig("aurora", "config")))));
  }

  private void expectGetTasksByHost(String host, IScheduledTask... tasks) {
    storageUtil.expectTaskFetch(Query.slaveScoped(host), tasks);
  }
}
