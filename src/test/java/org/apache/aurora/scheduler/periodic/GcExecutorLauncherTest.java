/*
 * Copyright 2013 Twitter, Inc.
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
package org.apache.aurora.scheduler.periodic;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.comm.AdjustRetainedTasks;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.periodic.GcExecutorLauncher.GcExecutorSettings;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;

import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;

import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.FAILED;

import static org.easymock.EasyMock.expect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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

  private static final Amount<Long, Time> MAX_GC_INTERVAL = Amount.of(1L, Time.HOURS);
  private static final Optional<String> GC_EXCECUTOR_PATH = Optional.of("nonempty");

  private final AtomicInteger taskIdCounter = new AtomicInteger();

  private FakeClock clock;
  private StorageTestUtil storageUtil;
  private GcExecutorLauncher gcExecutorLauncher;
  private GcExecutorSettings settings;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    clock = new FakeClock();
    storageUtil.expectOperations();
    settings = createMock(GcExecutorSettings.class);
    expect(settings.getMaxGcInterval()).andReturn(MAX_GC_INTERVAL.as(Time.MILLISECONDS)).anyTimes();
  }

  private void replayAndCreate() {
    control.replay();
    gcExecutorLauncher = new GcExecutorLauncher(settings, storageUtil.storage, clock);
  }

  @Test
  public void testPruning() throws Exception {
    IScheduledTask thermosPrunedTask = makeTask(JOB_A, true, FAILED);
    IScheduledTask thermosTask = makeTask(JOB_A, true, FAILED);
    IScheduledTask nonThermosTask = makeTask(JOB_A, false, FAILED);

    // First call - no tasks to be collected.
    expectGetTasksByHost(HOST, thermosPrunedTask, thermosTask, nonThermosTask);
    expect(settings.getDelayMs()).andReturn(Amount.of(30, Time.MINUTES).as(Time.MILLISECONDS));

    // Third call - two tasks collected.
    expectGetTasksByHost(HOST, thermosPrunedTask);
    expect(settings.getDelayMs()).andReturn(Amount.of(30, Time.MINUTES).as(Time.MILLISECONDS));

    expect(settings.getGcExecutorPath()).andReturn(GC_EXCECUTOR_PATH).times(5);

    replayAndCreate();

    // First call - no items in the cache, no tasks collected.
    Optional<TaskInfo> taskInfo = gcExecutorLauncher.createTask(OFFER);
    assertTrue(taskInfo.isPresent());
    assertRetainedTasks(taskInfo.get(), thermosPrunedTask, thermosTask, nonThermosTask);
    ExecutorInfo executor1 = taskInfo.get().getExecutor();

    // Second call - host item alive, no tasks collected.
    clock.advance(Amount.of(15L, Time.MINUTES));
    taskInfo = gcExecutorLauncher.createTask(OFFER);
    assertFalse(taskInfo.isPresent());

    // Third call - two tasks collected.
    clock.advance(Amount.of(15L, Time.MINUTES));
    taskInfo = gcExecutorLauncher.createTask(OFFER);
    assertTrue(taskInfo.isPresent());
    assertRetainedTasks(taskInfo.get(), thermosPrunedTask);

    // Same executor should be re-used for both tasks
    assertEquals(executor1, taskInfo.get().getExecutor());
  }

  @Test
  public void testNoAcceptingSmallOffers() {
    expect(settings.getGcExecutorPath()).andReturn(GC_EXCECUTOR_PATH);
    replayAndCreate();

    Iterable<Resource> resources =
        Resources.subtract(
            GcExecutorLauncher.TOTAL_GC_EXECUTOR_RESOURCES,
            GcExecutorLauncher.EPSILON).toResourceList();
    Offer smallOffer = OFFER.toBuilder()
        .clearResources()
        .addAllResources(resources)
        .build();
    assertFalse(gcExecutorLauncher.createTask(smallOffer).isPresent());
  }

  private static void assertRetainedTasks(TaskInfo taskInfo, IScheduledTask... tasks)
      throws ThriftBinaryCodec.CodingException {
    AdjustRetainedTasks message = ThriftBinaryCodec.decode(
        AdjustRetainedTasks.class, taskInfo.getData().toByteArray());
    Map<String, IScheduledTask> byId = Tasks.mapById(ImmutableSet.copyOf(tasks));
    assertNotNull(message);
    assertEquals(Maps.transformValues(byId, Tasks.GET_STATUS), message.getRetainedTasks());
  }

  private IScheduledTask makeTask(String jobName, boolean isThermos, ScheduleStatus status) {
    return IScheduledTask.build(new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask()
            .setTaskId("task-" + taskIdCounter.incrementAndGet())
            .setSlaveHost(HOST)
            .setTask(new TaskConfig()
                .setJobName(jobName)
                .setOwner(new Identity().setRole("role").setUser("user"))
                .setExecutorConfig(isThermos ? new ExecutorConfig("aurora", "config") : null))));
  }

  private void expectGetTasksByHost(String host, IScheduledTask... tasks) {
    storageUtil.expectTaskFetch(Query.slaveScoped(host), tasks);
  }
}
