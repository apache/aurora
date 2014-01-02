/**
 * Copyright 2013 Apache Software Foundation
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

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.events.PubsubEvent.StorageStarted;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.INIT;
import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class TaskVarsTest extends EasyMockTest {

  private static final String ROLE_A = "role_a";
  private static final String JOB_A = "job_a";
  private static final String JOB_B = "job_b";
  private static final String TASK_ID = "task_id";

  private StorageTestUtil storageUtil;
  private StatsProvider trackedStats;
  private TaskVars vars;
  private Map<ScheduleStatus, AtomicLong> globalCounters;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    trackedStats = createMock(StatsProvider.class);
  }

  private void initialize() {
    vars = new TaskVars(storageUtil.storage, trackedStats);
    vars.storageStarted(new StorageStarted());
  }

  private void changeState(IScheduledTask task, ScheduleStatus status) {
    vars.taskChangedState(new TaskStateChange(
        IScheduledTask.build(task.newBuilder().setStatus(status)),
        task.getStatus()));
  }

  private void expectLoadStorage(IScheduledTask... result) {
    storageUtil.expectOperations();
    storageUtil.expectTaskFetch(Query.unscoped(), result);
    globalCounters = Maps.newHashMap();
    for (ScheduleStatus status : ScheduleStatus.values()) {
      AtomicLong counter = new AtomicLong(0);
      globalCounters.put(status, counter);
      expect(trackedStats.makeCounter(TaskVars.getVarName(status))).andReturn(counter);
    }
  }

  private IScheduledTask makeTask(String job, ScheduleStatus status, String host) {
    return IScheduledTask.build(new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask()
            .setTaskId(TASK_ID)
            .setSlaveHost(host)
            .setTask(new TaskConfig()
                .setJobName(job)
                .setOwner(new Identity(ROLE_A, ROLE_A + "-user")))));
  }

  private IScheduledTask makeTask(String job, ScheduleStatus status) {
    return makeTask(job, status, "hostA");
  }

  private void assertAllZero() {
    for (AtomicLong counter : globalCounters.values()) {
      assertEquals(0L, counter.get());
    }
  }

  @Test
  public void testStartsAtZero() {
    expectLoadStorage();

    control.replay();
    initialize();

    assertAllZero();
  }

  @Test
  public void testNoEarlyExport() {
    control.replay();

    // No variables should be exported prior to storage starting.
    vars = new TaskVars(storageUtil.storage, trackedStats);
  }

  @Test
  public void testTaskLifeCycle() {
    expectLoadStorage();

    control.replay();
    initialize();

    IScheduledTask taskA = makeTask(JOB_A, INIT);
    changeState(taskA, PENDING);
    assertEquals(1, globalCounters.get(PENDING).get());
    changeState(IScheduledTask.build(taskA.newBuilder().setStatus(PENDING)), ASSIGNED);
    assertEquals(0, globalCounters.get(PENDING).get());
    assertEquals(1, globalCounters.get(ASSIGNED).get());
    changeState(IScheduledTask.build(taskA.newBuilder().setStatus(ASSIGNED)), RUNNING);
    assertEquals(0, globalCounters.get(ASSIGNED).get());
    assertEquals(1, globalCounters.get(RUNNING).get());
    changeState(IScheduledTask.build(taskA.newBuilder().setStatus(RUNNING)), FINISHED);
    assertEquals(0, globalCounters.get(RUNNING).get());
    assertEquals(1, globalCounters.get(FINISHED).get());
    vars.tasksDeleted(new TasksDeleted(ImmutableSet.of(
        IScheduledTask.build(taskA.newBuilder().setStatus(FINISHED)))));
    assertAllZero();
  }

  @Test
  public void testLoadsFromStorage() {
    expectLoadStorage(
        makeTask(JOB_A, PENDING),
        makeTask(JOB_A, RUNNING),
        makeTask(JOB_A, FINISHED),
        makeTask(JOB_B, PENDING),
        makeTask(JOB_B, FAILED));
    control.replay();
    initialize();

    assertEquals(2, globalCounters.get(PENDING).get());
    assertEquals(1, globalCounters.get(RUNNING).get());
    assertEquals(1, globalCounters.get(FINISHED).get());
    assertEquals(1, globalCounters.get(FAILED).get());
  }

  private IExpectationSetters<?> expectGetHostRack(String host, String rackToReturn) {
    HostAttributes attributes = new HostAttributes()
        .setHost(host)
        .setAttributes(ImmutableSet.of(
            new Attribute().setName("rack").setValues(ImmutableSet.of(rackToReturn))));
    return expect(storageUtil.attributeStore.getHostAttributes(host))
        .andReturn(Optional.of(attributes));
  }

  @Test
  public void testLostCounters() {
    expectLoadStorage();
    expectGetHostRack("host1", "rackA").atLeastOnce();
    expectGetHostRack("host2", "rackB").atLeastOnce();
    expectGetHostRack("host3", "rackB").atLeastOnce();

    AtomicLong rackA = new AtomicLong();
    expect(trackedStats.makeCounter(TaskVars.rackStatName("rackA"))).andReturn(rackA);
    AtomicLong rackB = new AtomicLong();
    expect(trackedStats.makeCounter(TaskVars.rackStatName("rackB"))).andReturn(rackB);

    control.replay();
    initialize();

    IScheduledTask a = makeTask("jobA", RUNNING, "host1");
    IScheduledTask b = makeTask("jobB", RUNNING, "host2");
    IScheduledTask c = makeTask("jobC", RUNNING, "host3");
    IScheduledTask d = makeTask("jobD", RUNNING, "host1");

    changeState(a, LOST);
    changeState(b, LOST);
    changeState(c, LOST);
    changeState(d, LOST);

    assertEquals(2, rackA.get());
    assertEquals(2, rackB.get());
  }

  @Test
  public void testRackMissing() {
    expectLoadStorage();
    expect(storageUtil.attributeStore.getHostAttributes("a"))
        .andReturn(Optional.<HostAttributes>absent());

    control.replay();
    initialize();

    IScheduledTask a = makeTask(JOB_A, RUNNING, "a");
    changeState(a, LOST);
    // Since no attributes are stored for the host, a variable is not exported/updated.
  }

  @Test
  public void testDeleteBeforeStorageStarted() {
    expectLoadStorage();

    control.replay();

    vars = new TaskVars(storageUtil.storage, trackedStats);
    vars.tasksDeleted(new TasksDeleted(ImmutableSet.of(makeTask("jobA", RUNNING, "host1"))));
    assertEquals(0, globalCounters.get(RUNNING).get());

    vars.storageStarted(new StorageStarted());
    assertEquals(0, globalCounters.get(RUNNING).get());
  }
}
