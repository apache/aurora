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
package org.apache.aurora.scheduler.async;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Optional;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.comm.AdjustRetainedTasks;
import org.apache.aurora.scheduler.async.GcExecutorLauncher.GcExecutorSettings;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.mesos.Driver;
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
import static org.apache.aurora.scheduler.async.GcExecutorLauncher.INSUFFICIENT_OFFERS_STAT_NAME;
import static org.apache.aurora.scheduler.async.GcExecutorLauncher.LOST_TASKS_STAT_NAME;
import static org.apache.aurora.scheduler.async.GcExecutorLauncher.SYSTEM_TASK_PREFIX;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
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
  private static final String TASK_UUID = "gc";

  private static final GcExecutorSettings SETTINGS =
      new GcExecutorSettings(Amount.of(1L, Time.HOURS), Optional.of("nonempty"));

  private final AtomicInteger taskIdCounter = new AtomicInteger();

  private FakeClock clock;
  private StorageTestUtil storageUtil;
  private Driver driver;
  private StatsProvider statsProvider;
  private GcExecutorLauncher gcExecutorLauncher;
  private AtomicLong lostTasks;
  private AtomicLong insufficientOffers;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    clock = new FakeClock();
    storageUtil.expectOperations();
    driver = createMock(Driver.class);
    statsProvider = createMock(StatsProvider.class);
    lostTasks = new AtomicLong();
    insufficientOffers = new AtomicLong();
  }

  private void replayAndConstruct() {
    expect(statsProvider.makeCounter(LOST_TASKS_STAT_NAME)).andReturn(lostTasks);
    expect(statsProvider.makeCounter(INSUFFICIENT_OFFERS_STAT_NAME)).andReturn(insufficientOffers);
    control.replay();
    gcExecutorLauncher = new GcExecutorLauncher(
        SETTINGS,
        storageUtil.storage,
        clock,
        MoreExecutors.sameThreadExecutor(),
        driver,
        statsProvider,
        Suppliers.ofInstance(TASK_UUID));
  }

  @Test
  public void testPruning() throws Exception {
    IScheduledTask a = makeTask(JOB_A, FAILED);
    IScheduledTask b = makeTask(JOB_A, FAILED);
    IScheduledTask c = makeTask(JOB_A, FAILED);

    // Third call - no tasks to be collected.
    expectGetTasksByHost(HOST, a, b, c);
    expectAdjustRetainedTasks(a, b, c);

    // Fourth call - two tasks collected.
    expectGetTasksByHost(HOST, a);
    expectAdjustRetainedTasks(a);

    // Fifth call - the last task collected.
    expectGetTasksByHost(HOST);
    expectAdjustRetainedTasks();

    replayAndConstruct();

    // First call - no items in the cache, no tasks collected.
    assertFalse(gcExecutorLauncher.willUse(OFFER));

    // Second call - host item alive, no tasks collected.
    clock.advance(Amount.of((long) SETTINGS.getDelayMs() - 1, Time.MILLISECONDS));
    assertFalse(gcExecutorLauncher.willUse(OFFER));

    // Third call - host item expires (initial delay), no tasks collected
    clock.advance(Amount.of(1L, Time.HOURS));
    assertTrue(gcExecutorLauncher.willUse(OFFER));

    // Fourth call - host item expires (regular delay), two tasks collected
    clock.advance(Amount.of(1L, Time.HOURS));
    assertTrue(gcExecutorLauncher.willUse(OFFER));

    // Fifth call - host item expires (regular delay), one task collected
    clock.advance(Amount.of(1L, Time.HOURS));
    assertTrue(gcExecutorLauncher.willUse(OFFER));

    assertEquals(0, insufficientOffers.get());
  }

  @Test
  public void testNoAcceptingSmallOffers() {
    replayAndConstruct();

    Iterable<Resource> resources =
        Resources.subtract(
            GcExecutorLauncher.TOTAL_GC_EXECUTOR_RESOURCES,
            GcExecutorLauncher.EPSILON).toResourceList();
    Offer smallOffer = OFFER.toBuilder()
        .clearResources()
        .addAllResources(resources)
        .build();
    assertEquals(0, insufficientOffers.get());
    assertFalse(gcExecutorLauncher.willUse(smallOffer));
    assertEquals(1, insufficientOffers.get());
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
    replayAndConstruct();

    assertTrue(gcExecutorLauncher.statusUpdate(makeStatus(SYSTEM_TASK_PREFIX)));
    assertTrue(gcExecutorLauncher.statusUpdate(makeStatus(SYSTEM_TASK_PREFIX + "1")));
    assertFalse(gcExecutorLauncher.statusUpdate(makeStatus("1" + SYSTEM_TASK_PREFIX)));
    assertFalse(gcExecutorLauncher.statusUpdate(makeStatus("asdf")));
    assertEquals(0, lostTasks.get());
    assertTrue(gcExecutorLauncher.statusUpdate(
        makeStatus(SYSTEM_TASK_PREFIX).toBuilder().setState(TaskState.TASK_LOST).build()));
    assertEquals(1, lostTasks.get());
  }

  @Test
  public void testGcExecutorDisabled() {
    expect(statsProvider.makeCounter(LOST_TASKS_STAT_NAME)).andReturn(lostTasks);
    expect(statsProvider.makeCounter(INSUFFICIENT_OFFERS_STAT_NAME)).andReturn(insufficientOffers);
    control.replay();

    gcExecutorLauncher = new GcExecutorLauncher(
        new GcExecutorSettings(Amount.of(1L, Time.HOURS), Optional.<String>absent()),
        storageUtil.storage,
        clock,
        MoreExecutors.sameThreadExecutor(),
        driver,
        statsProvider,
        Suppliers.ofInstance("gc"));
    assertFalse(gcExecutorLauncher.willUse(OFFER));
    assertEquals(0, insufficientOffers.get());
  }

  private void expectAdjustRetainedTasks(IScheduledTask... tasks) {
    Map<String, ScheduleStatus> statuses =
        Maps.transformValues(Tasks.mapById(ImmutableSet.copyOf(tasks)), Tasks.GET_STATUS);
    AdjustRetainedTasks message = new AdjustRetainedTasks().setRetainedTasks(statuses);
    TaskInfo task = GcExecutorLauncher.makeGcTask(
        HOST, OFFER.getSlaveId(), SETTINGS.getGcExecutorPath().get(), TASK_UUID, message);
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
