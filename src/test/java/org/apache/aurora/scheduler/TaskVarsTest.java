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

import java.util.EnumSet;
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
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.filter.SchedulingFilter.VetoGroup;
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
import static org.apache.aurora.scheduler.TaskVars.VETO_GROUPS_TO_COUNTERS;
import static org.apache.aurora.scheduler.TaskVars.jobStatName;
import static org.apache.aurora.scheduler.TaskVars.rackStatName;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class TaskVarsTest extends EasyMockTest {

  private static final String ROLE_A = "role_a";
  private static final String JOB_A = "job_a";
  private static final String JOB_B = "job_b";
  private static final String TASK_ID = "task_id";
  private static final String ENV = "test";

  private static final String STATIC_COUNTER = VETO_GROUPS_TO_COUNTERS.get(VetoGroup.STATIC);
  private static final String DYNAMIC_COUNTER = VETO_GROUPS_TO_COUNTERS.get(VetoGroup.DYNAMIC);
  private static final String MIXED_COUNTER = VETO_GROUPS_TO_COUNTERS.get(VetoGroup.MIXED);

  private StorageTestUtil storageUtil;
  private StatsProvider trackedProvider;
  private StatsProvider untrackedProvider;
  private TaskVars vars;
  private Map<String, Supplier<Long>> globalCounters;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    trackedProvider = createMock(StatsProvider.class);
    untrackedProvider = createMock(StatsProvider.class);
    expect(trackedProvider.untracked()).andReturn(untrackedProvider);
    storageUtil.expectOperations();
    globalCounters = Maps.newHashMap();
  }

  private void replayAndBuild() {
    control.replay();
    vars = new TaskVars(storageUtil.storage, trackedProvider);
  }

  private void expectStatExport(String name) {
    expectStatExport(name, trackedProvider);
  }

  private void expectStatExport(final String name, StatsProvider provider) {
    expect(provider.makeGauge(EasyMock.eq(name), EasyMock.<Supplier<Long>>anyObject()))
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

  private void applyVeto(IScheduledTask task, Veto... vetoes) {
    vars.taskVetoed(new PubsubEvent.Vetoed(
        TaskGroupKey.from(task.getAssignedTask().getTask()),
        ImmutableSet.copyOf(vetoes)));
  }

  private void schedulerActivated(IScheduledTask... initialTasks) {
    for (IScheduledTask task : initialTasks) {
      vars.taskChangedState(TaskStateChange.initialized(task));
    }
    vars.startAsync().awaitRunning();
  }

  private IScheduledTask makeTask(String job, ScheduleStatus status, String host) {
    ScheduledTask task = new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask()
            .setTaskId(TASK_ID)
            .setTask(new TaskConfig()
                .setJob(new JobKey(ROLE_A, ENV, job))
                .setJobName(job)
                .setEnvironment(ENV)
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

    replayAndBuild();
    schedulerActivated();

    assertAllZero();
  }

  @Test
  public void testNoEarlyExport() {
    replayAndBuild();

    // No variables should be exported since schedulerActive is never called.
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

    replayAndBuild();
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
  public void testStaticVetoGroup() {
    expectStatusCountersInitialized();
    expectStatExport(STATIC_COUNTER);

    replayAndBuild();
    schedulerActivated();

    applyVeto(
        makeTask(JOB_A, PENDING),
        Veto.insufficientResources("ram", 500),
        Veto.insufficientResources("cpu", 500));

    assertEquals(1, getValue(STATIC_COUNTER));
  }

  @Test
  public void testDynamicVetoGroup() {
    expectStatusCountersInitialized();
    expectStatExport(DYNAMIC_COUNTER);

    replayAndBuild();
    schedulerActivated();

    applyVeto(makeTask(JOB_A, PENDING), Veto.unsatisfiedLimit("constraint"));
    assertEquals(1, getValue(DYNAMIC_COUNTER));
  }

  @Test
  public void testMixedVetoGroup() {
    expectStatusCountersInitialized();
    expectStatExport(MIXED_COUNTER);

    replayAndBuild();
    schedulerActivated();

    applyVeto(makeTask(JOB_A, PENDING),
        Veto.unsatisfiedLimit("constraint"),
        Veto.insufficientResources("ram", 500));

    assertEquals(1, getValue(MIXED_COUNTER));
  }

  @Test
  public void testLoadsFromStorage() {
    expectStatusCountersInitialized();
    expectGetHostRack("hostA", "rackA").atLeastOnce();
    expectGetHostRack("hostB", "rackB").atLeastOnce();
    expectStatExport(rackStatName("rackA"));
    expectStatExport(rackStatName("rackB"));

    IScheduledTask failedTask = makeTask(JOB_B, FAILED, "hostB");
    expectStatExport(jobStatName(failedTask, FAILED), untrackedProvider);

    replayAndBuild();
    schedulerActivated(
        makeTask(JOB_A, PENDING),
        makeTask(JOB_A, RUNNING, "hostA"),
        makeTask(JOB_A, FINISHED, "hostA"),
        makeTask(JOB_B, PENDING),
        failedTask);

    assertEquals(2, getValue(PENDING));
    assertEquals(1, getValue(RUNNING));
    assertEquals(1, getValue(FINISHED));
    assertEquals(1, getValue(FAILED));
    assertEquals(0, getValue(rackStatName("rackA")));
    assertEquals(0, getValue(rackStatName("rackB")));
    assertEquals(1, getValue(jobStatName(failedTask, FAILED)));
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

    IScheduledTask a = makeTask("jobA", RUNNING, "host1");
    IScheduledTask b = makeTask("jobB", RUNNING, "host2");
    IScheduledTask c = makeTask("jobD", RUNNING, "host3");
    IScheduledTask d = makeTask("jobD", RUNNING, "host1");

    expectStatExport(jobStatName(a, LOST), untrackedProvider);
    expectStatExport(jobStatName(b, LOST), untrackedProvider);
    expectStatExport(jobStatName(c, LOST), untrackedProvider);

    replayAndBuild();
    schedulerActivated();

    changeState(a, LOST);
    changeState(b, LOST);
    changeState(c, LOST);
    changeState(d, LOST);

    assertEquals(2, getValue(rackStatName("rackA")));
    assertEquals(2, getValue(rackStatName("rackB")));

    assertEquals(1, getValue(jobStatName(a, LOST)));
    assertEquals(1, getValue(jobStatName(b, LOST)));
    assertEquals(2, getValue(jobStatName(c, LOST)));
  }

  @Test
  public void testRackMissing() {
    expectStatusCountersInitialized();
    expect(storageUtil.attributeStore.getHostAttributes("a"))
        .andReturn(Optional.absent());

    IScheduledTask a = makeTask(JOB_A, RUNNING, "a");
    expectStatExport(jobStatName(a, LOST), untrackedProvider);

    replayAndBuild();
    schedulerActivated();

    changeState(a, LOST);
    // Since no attributes are stored for the host, a variable is not exported/updated.
  }

  @Test
  public void testAllVetoGroupsCovered() {
    replayAndBuild();
    for (VetoGroup group : EnumSet.complementOf(EnumSet.of(VetoGroup.EMPTY))) {
      assertNotNull("Unknown VetoGroup value: " + group, VETO_GROUPS_TO_COUNTERS.get(group));
    }
  }
}
