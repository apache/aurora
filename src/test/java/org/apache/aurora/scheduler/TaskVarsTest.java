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
package org.apache.aurora.scheduler;

import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.twitter.common.stats.Stat;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.SchedulerActive;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
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
import static org.apache.aurora.scheduler.TaskVars.rackStatName;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TaskVarsTest extends EasyMockTest {

  private static final String ROLE_A = "role_a";
  private static final String JOB_A = "job_a";
  private static final String JOB_B = "job_b";
  private static final String TASK_ID = "task_id";

  private StorageTestUtil storageUtil;
  private StatsProvider trackedStats;
  private TaskVars vars;
  private Map<String, Supplier<Long>> globalCounters;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    trackedStats = createMock(StatsProvider.class);
    vars = new TaskVars(storageUtil.storage, trackedStats);

    storageUtil.expectOperations();
    globalCounters = Maps.newHashMap();
  }

  private void expectStatExport(final String name) {
    expect(trackedStats.makeGauge(EasyMock.eq(name), EasyMock.<Supplier<Long>>anyObject()))
        .andAnswer(new IAnswer<Stat<Long>>() {
          @SuppressWarnings("unchecked")
          @Override
          public Stat<Long> answer() {
            assertFalse(globalCounters.containsKey(name));
            globalCounters.put(name, (Supplier<Long>) EasyMock.getCurrentArguments()[1]);
            return null;
          }
        });
  }

  private void expectStatusCountersInitialized() {
    for (ScheduleStatus status : ScheduleStatus.values()) {
      expectStatExport(TaskVars.getVarName(status));
    }
  }

  private void changeState(IScheduledTask task, ScheduleStatus status) {
    vars.taskChangedState(TaskStateChange.transition(
        IScheduledTask.build(task.newBuilder().setStatus(status)),
        task.getStatus()));
  }

  private void schedulerActivated(IScheduledTask... initialTasks) {
    for (IScheduledTask task : initialTasks) {
      vars.taskChangedState(TaskStateChange.initialized(task));
    }
    vars.schedulerActive(new SchedulerActive());
  }

  private IScheduledTask makeTask(String job, ScheduleStatus status, String host) {
    ScheduledTask task = new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask()
            .setTaskId(TASK_ID)
            .setTask(new TaskConfig()
                .setJobName(job)
                .setOwner(new Identity(ROLE_A, ROLE_A + "-user"))));
    if (Tasks.SLAVE_ASSIGNED_STATES.contains(status) || Tasks.isTerminated(status)) {
      task.getAssignedTask().setSlaveHost(host);
    }

    return IScheduledTask.build(task);
  }

  private IScheduledTask makeTask(String job, ScheduleStatus status) {
    return makeTask(job, status, "hostA");
  }

  private void assertAllZero() {
    for (Supplier<Long> counter : globalCounters.values()) {
      assertEquals(0L, counter.get().longValue());
    }
  }

  @Test
  public void testStartsAtZero() {
    expectStatusCountersInitialized();

    control.replay();
    schedulerActivated();

    assertAllZero();
  }

  @Test
  public void testNoEarlyExport() {
    control.replay();

    // No variables should be exported since schedulerActive is never called.
    vars = new TaskVars(storageUtil.storage, trackedStats);
    IScheduledTask taskA = makeTask(JOB_A, INIT);
    changeState(taskA, PENDING);
    changeState(IScheduledTask.build(taskA.newBuilder().setStatus(PENDING)), ASSIGNED);
  }

  private int getValue(String name) {
    return globalCounters.get(name).get().intValue();
  }

  private int getValue(ScheduleStatus status) {
    return getValue(TaskVars.getVarName(status));
  }

  @Test
  public void testTaskLifeCycle() {
    expectStatusCountersInitialized();

    IScheduledTask taskA = makeTask(JOB_A, INIT);
    expectGetHostRack("hostA", "rackA").atLeastOnce();
    expectStatExport(rackStatName("rackA"));

    control.replay();
    schedulerActivated();

    changeState(makeTask(JOB_A, INIT), PENDING);
    assertEquals(1, getValue(PENDING));
    changeState(IScheduledTask.build(taskA.newBuilder().setStatus(PENDING)), ASSIGNED);
    assertEquals(0, getValue(PENDING));
    assertEquals(1, getValue(ASSIGNED));
    taskA = makeTask(JOB_A, ASSIGNED, "hostA");
    changeState(IScheduledTask.build(taskA.newBuilder().setStatus(ASSIGNED)), RUNNING);
    assertEquals(0, getValue(ASSIGNED));
    assertEquals(1, getValue(RUNNING));
    changeState(IScheduledTask.build(taskA.newBuilder().setStatus(RUNNING)), FINISHED);
    assertEquals(0, getValue(RUNNING));
    assertEquals(1, getValue(FINISHED));
    assertEquals(0, getValue(rackStatName("rackA")));
    vars.tasksDeleted(new TasksDeleted(ImmutableSet.of(
        IScheduledTask.build(taskA.newBuilder().setStatus(FINISHED)))));
    assertAllZero();
  }

  @Test
  public void testLoadsFromStorage() {
    expectStatusCountersInitialized();
    expectGetHostRack("hostA", "rackA").atLeastOnce();
    expectGetHostRack("hostB", "rackB").atLeastOnce();
    expectStatExport(rackStatName("rackA"));
    expectStatExport(rackStatName("rackB"));

    control.replay();
    schedulerActivated(
        makeTask(JOB_A, PENDING),
        makeTask(JOB_A, RUNNING, "hostA"),
        makeTask(JOB_A, FINISHED, "hostA"),
        makeTask(JOB_B, PENDING),
        makeTask(JOB_B, FAILED, "hostB"));

    assertEquals(2, getValue(PENDING));
    assertEquals(1, getValue(RUNNING));
    assertEquals(1, getValue(FINISHED));
    assertEquals(1, getValue(FAILED));
    assertEquals(0, getValue(rackStatName("rackA")));
    assertEquals(0, getValue(rackStatName("rackB")));
  }

  private IExpectationSetters<?> expectGetHostRack(String host, String rackToReturn) {
    IHostAttributes attributes = IHostAttributes.build(new HostAttributes()
        .setHost(host)
        .setAttributes(ImmutableSet.of(
            new Attribute().setName("rack").setValues(ImmutableSet.of(rackToReturn)))));
    return expect(storageUtil.attributeStore.getHostAttributes(host))
        .andReturn(Optional.of(attributes));
  }

  @Test
  public void testLostCounters() {
    expectStatusCountersInitialized();
    expectGetHostRack("host1", "rackA").atLeastOnce();
    expectGetHostRack("host2", "rackB").atLeastOnce();
    expectGetHostRack("host3", "rackB").atLeastOnce();

    expectStatExport(rackStatName("rackA"));
    expectStatExport(rackStatName("rackB"));

    control.replay();
    schedulerActivated();

    IScheduledTask a = makeTask("jobA", RUNNING, "host1");
    IScheduledTask b = makeTask("jobB", RUNNING, "host2");
    IScheduledTask c = makeTask("jobC", RUNNING, "host3");
    IScheduledTask d = makeTask("jobD", RUNNING, "host1");

    changeState(a, LOST);
    changeState(b, LOST);
    changeState(c, LOST);
    changeState(d, LOST);

    assertEquals(2, getValue(rackStatName("rackA")));
    assertEquals(2, getValue(rackStatName("rackB")));
  }

  @Test
  public void testRackMissing() {
    expectStatusCountersInitialized();
    expect(storageUtil.attributeStore.getHostAttributes("a"))
        .andReturn(Optional.<IHostAttributes>absent());

    control.replay();
    schedulerActivated();

    IScheduledTask a = makeTask(JOB_A, RUNNING, "a");
    changeState(a, LOST);
    // Since no attributes are stored for the host, a variable is not exported/updated.
  }
}
