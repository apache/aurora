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
package com.twitter.aurora.scheduler.periodic;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.Attribute;
import com.twitter.aurora.gen.Constraint;
import com.twitter.aurora.gen.HostAttributes;
import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskEvent;
import com.twitter.aurora.scheduler.ResourceSlot;
import com.twitter.aurora.scheduler.filter.SchedulingFilter;
import com.twitter.aurora.scheduler.filter.SchedulingFilterImpl;
import com.twitter.aurora.scheduler.state.MaintenanceController;
import com.twitter.aurora.scheduler.state.SchedulerCore;
import com.twitter.aurora.scheduler.storage.entities.IAssignedTask;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;
import com.twitter.aurora.scheduler.storage.testing.StorageTestUtil;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import static org.easymock.EasyMock.expect;

import static com.twitter.aurora.gen.MaintenanceMode.NONE;
import static com.twitter.aurora.gen.ScheduleStatus.PENDING;
import static com.twitter.aurora.gen.ScheduleStatus.RUNNING;
import static com.twitter.aurora.scheduler.filter.SchedulingFilter.Veto;

public class PreempterTest extends EasyMockTest {

  private static final String USER_A = "user_a";
  private static final String USER_B = "user_b";
  private static final String USER_C = "user_c";
  private static final String JOB_A = "job_a";
  private static final String JOB_B = "job_b";
  private static final String JOB_C = "job_c";
  private static final String JOB_D = "job_d";
  private static final String TASK_ID_A = "task_a";
  private static final String TASK_ID_B = "task_b";
  private static final String TASK_ID_C = "task_c";
  private static final String TASK_ID_D = "task_d";
  private static final String HOST_A = "host_a";
  private static final String HOST_B = "host_b";
  private static final String RACK_A = "rackA";
  private static final String RACK_B = "rackB";
  private static final String RACK_ATTRIBUTE = "rack";
  private static final String HOST_ATTRIBUTE = "host";

  private static final Amount<Long, Time> PREEMPTION_DELAY = Amount.of(30L, Time.SECONDS);

  private StorageTestUtil storageUtil;
  private SchedulerCore scheduler;
  private SchedulingFilter schedulingFilter;
  private FakeClock clock;
  private Preempter preempter;
  private MaintenanceController maintenance;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    scheduler = createMock(SchedulerCore.class);
    maintenance = createMock(MaintenanceController.class);
    clock = new FakeClock();
  }

  private void runPreempter() {
    preempter = new Preempter(
        storageUtil.storage,
        scheduler,
        schedulingFilter,
        PREEMPTION_DELAY,
        clock);

    preempter.run();
  }

  // TODO(zmanji): Put together a SchedulerPreempterIntegrationTest as well.

  @Test
  public void testNoPendingTasks() {
    schedulingFilter = createMock(SchedulingFilter.class);
    expectGetPendingTasks();

    control.replay();
    runPreempter();
  }

  private void expectGetPendingTasks(ScheduledTask... returnedTasks) {
    storageUtil.expectTaskFetch(
        Preempter.PENDING_QUERY,
        IScheduledTask.setFromBuilders(Arrays.asList(returnedTasks)));
  }

  private void expectGetActiveTasks(ScheduledTask... returnedTasks) {
    storageUtil.expectTaskFetch(
        Preempter.ACTIVE_NOT_PENDING_QUERY,
        IScheduledTask.setFromBuilders(Arrays.asList(returnedTasks)));
  }

  @Test
  public void testRecentlyPending() {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A);
    runOnHost(lowPriority, HOST_A);

    expectGetPendingTasks(lowPriority, makeTask(USER_A, JOB_A, TASK_ID_B, 100));

    control.replay();
    runPreempter();
  }

  @Test
  public void testPreempted() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A);
    runOnHost(lowPriority, HOST_A);

    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 100);
    clock.advance(PREEMPTION_DELAY);

    expectGetPendingTasks(highPriority);
    expectGetActiveTasks(lowPriority);

    expectFiltering();
    expectPreempted(lowPriority, highPriority);

    control.replay();
    runPreempter();
  }

  @Test
  public void testLowestPriorityPreempted() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A, 10);
    runOnHost(lowPriority, HOST_A);

    ScheduledTask lowerPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 1);
    runOnHost(lowerPriority, HOST_A);

    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_C, 100);
    clock.advance(PREEMPTION_DELAY);

    expectGetPendingTasks(highPriority);
    expectGetActiveTasks(lowerPriority, lowerPriority);

    expectFiltering();
    expectPreempted(lowerPriority, highPriority);

    control.replay();
    runPreempter();
  }

  @Test
  public void testOnePreemptableTask() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_A, 100);
    runOnHost(highPriority, HOST_A);

    ScheduledTask lowerPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 99);
    runOnHost(lowerPriority, HOST_A);

    ScheduledTask lowestPriority = makeTask(USER_A, JOB_A, TASK_ID_C, 1);
    runOnHost(lowestPriority, HOST_A);

    ScheduledTask pendingPriority = makeTask(USER_A, JOB_A, TASK_ID_D, 98);
    clock.advance(PREEMPTION_DELAY);

    expectGetPendingTasks(pendingPriority);
    expectGetActiveTasks(highPriority, lowerPriority, lowestPriority);

    expectFiltering();
    expectPreempted(lowestPriority, pendingPriority);

    control.replay();
    runPreempter();
  }

  @Test
  public void testHigherPriorityRunning() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 100);
    runOnHost(highPriority, HOST_A);

    ScheduledTask task = makeTask(USER_A, JOB_A, TASK_ID_A);
    clock.advance(PREEMPTION_DELAY);

    expectGetPendingTasks(task);
    expectGetActiveTasks(highPriority);

    control.replay();
    runPreempter();
  }

  @Test
  public void testOversubscribed() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A);
    runOnHost(lowPriority, HOST_A);

    // Despite having two high priority tasks, we only perform one eviction.
    ScheduledTask highPriority1 = makeTask(USER_A, JOB_A, TASK_ID_B, 100);
    ScheduledTask highPriority2 = makeTask(USER_A, JOB_A, TASK_ID_C, 100);
    clock.advance(PREEMPTION_DELAY);

    expectGetPendingTasks(highPriority1, highPriority2);
    expectGetActiveTasks(lowPriority);

    expectFiltering();
    expectPreempted(lowPriority, highPriority1);

    control.replay();
    runPreempter();
  }

  @Test
  public void testProductionPreemptingNonproduction() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    // Use a very low priority for the production task to show that priority is irrelevant.
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", -1000);
    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_B + "_a1", 100);
    runOnHost(a1, HOST_A);

    clock.advance(PREEMPTION_DELAY);

    expectGetPendingTasks(p1);
    expectGetActiveTasks(a1);

    expectFiltering();
    expectPreempted(a1, p1);

    control.replay();
    runPreempter();
  }

  @Test
  public void testProductionPreemptingNonproductionAcrossUsers() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    // Use a very low priority for the production task to show that priority is irrelevant.
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", -1000);
    ScheduledTask a1 = makeTask(USER_B, JOB_A, TASK_ID_B + "_a1", 100);
    runOnHost(a1, HOST_A);

    clock.advance(PREEMPTION_DELAY);

    expectGetPendingTasks(p1);
    expectGetActiveTasks(a1);

    expectFiltering();
    expectPreempted(a1, p1);

    control.replay();
    runPreempter();
  }

  @Test
  public void testProductionUsersDoNotPreemptEachOther() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", 1000);
    ScheduledTask a1 = makeProductionTask(USER_B, JOB_A, TASK_ID_B + "_a1", 0);
    runOnHost(a1, HOST_A);

    clock.advance(PREEMPTION_DELAY);

    expectGetPendingTasks(p1);
    expectGetActiveTasks(a1);

    control.replay();
    runPreempter();
  }

  @Test
  public void testInterleavedPriorities() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    ScheduledTask p1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_p1", 1);
    ScheduledTask a3 = makeTask(USER_A, JOB_A, TASK_ID_B + "_a3", 3);
    ScheduledTask p2 = makeTask(USER_A, JOB_B, TASK_ID_A + "_p2", 2);
    ScheduledTask a2 = makeTask(USER_A, JOB_B, TASK_ID_B + "_a2", 2);
    ScheduledTask p3 = makeTask(USER_B, JOB_A, TASK_ID_A + "_p3", 3);
    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_B + "_a1", 1);
    runOnHost(a3, HOST_A);
    runOnHost(a2, HOST_A);
    runOnHost(a1, HOST_B);

    clock.advance(PREEMPTION_DELAY);

    expectGetPendingTasks(p1, p2, p3);
    expectGetActiveTasks(a1, a2, a3);

    expectFiltering().anyTimes();
    expectPreempted(a1, p2);

    control.replay();
    runPreempter();
  }

  // This test ensures a production task can preempt 2 tasks on the same host.
  @Test
  public void testProductionPreemptingManyNonProduction() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(storageUtil.storage, maintenance);
    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    a1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    ScheduledTask b1 = makeTask(USER_B, JOB_B, TASK_ID_B + "_b1");
    b1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    setUpHost(HOST_A, RACK_A);

    runOnHost(a1, HOST_A);
    runOnHost(b1, HOST_A);

    ScheduledTask p1 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p1");
    p1.getAssignedTask().getTask().setNumCpus(2).setRamMb(1024);

    clock.advance(PREEMPTION_DELAY);

    expectGetPendingTasks(p1);
    expectGetActiveTasks(a1, b1);

    expectPreempted(a1, p1);
    expectPreempted(b1, p1);

    control.replay();
    runPreempter();
  }

  // This test ensures we select the minimal number of tasks to preempt
  @Test
  public void testMinimalSetPreempted() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(storageUtil.storage, maintenance);
    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    a1.getAssignedTask().getTask().setNumCpus(4).setRamMb(4096);

    ScheduledTask b1 = makeTask(USER_B, JOB_B, TASK_ID_B + "_b1");
    b1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    ScheduledTask b2 = makeTask(USER_B, JOB_B, TASK_ID_B + "_b2");
    b2.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    setUpHost(HOST_A, RACK_A);

    runOnHost(a1, HOST_A);
    runOnHost(b1, HOST_A);
    runOnHost(b2, HOST_A);

    ScheduledTask p1 = makeProductionTask(USER_C, JOB_C, TASK_ID_C + "_p1");
    p1.getAssignedTask().getTask().setNumCpus(2).setRamMb(1024);

    clock.advance(PREEMPTION_DELAY);

    expectGetPendingTasks(p1);
    expectGetActiveTasks(b1, b2, a1);

    expectPreempted(a1, p1);

    control.replay();
    runPreempter();
  }

  // This test ensures a production task *never* preempts a production task from another job.
  @Test
  public void testProductionJobNeverPreemptsProductionJob() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(storageUtil.storage, maintenance);
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1");
    p1.getAssignedTask().getTask().setNumCpus(2).setRamMb(1024);

    setUpHost(HOST_A, RACK_A);

    runOnHost(p1, HOST_A);

    ScheduledTask p2 = makeProductionTask(USER_B, JOB_B, TASK_ID_B + "_p2");
    p2.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    clock.advance(PREEMPTION_DELAY);

    expectGetActiveTasks(p1);
    expectGetPendingTasks(p2);

    control.replay();
    runPreempter();
  }

  // This test ensures two production tasks can preempt multiple non production tasks on different
  // hosts.
  @Test
  public void testMultipleProductionPreemptingManyNonProduction() throws Exception {
    schedulingFilter = new SchedulingFilterImpl(storageUtil.storage, maintenance);
    ScheduledTask a1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a1");
    a1.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    ScheduledTask a2 = makeTask(USER_A, JOB_A, TASK_ID_A + "_a2");
    a2.getAssignedTask().getTask().setNumCpus(1).setRamMb(512);

    ScheduledTask b1 = makeTask(USER_B, JOB_C, TASK_ID_A + "_b1");
    b1.getAssignedTask().getTask().setNumCpus(4).setRamMb(1024);

    setUpHost(HOST_A, RACK_A);
    setUpHost(HOST_B, RACK_B);

    runOnHost(a1, HOST_A);
    runOnHost(a2, HOST_A);
    runOnHost(b1, HOST_B);

    ScheduledTask p1 = makeProductionTask(USER_C, JOB_D, TASK_ID_B + "_p1");
    p1.getAssignedTask().getTask().setNumCpus(2).setRamMb(512);

    ScheduledTask p2 = makeProductionTask(USER_C, JOB_D, TASK_ID_B + "_p2");
    p2.getAssignedTask().getTask().setNumCpus(2).setRamMb(512);

    clock.advance(PREEMPTION_DELAY);

    expectGetActiveTasks(a1, a2, b1);
    expectGetPendingTasks(p1, p2);

    expectPreempted(b1, p1);
    expectPreempted(a2, p2);
    expectPreempted(a1, p2);

    control.replay();
    runPreempter();
  }

  private IExpectationSetters<Set<Veto>> expectFiltering() {
    return expect(schedulingFilter.filter(
        EasyMock.<ResourceSlot>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<ITaskConfig>anyObject(),
        EasyMock.<String>anyObject())).andAnswer(
        new IAnswer<Set<Veto>>() {
          @Override public Set<Veto> answer() {
            return ImmutableSet.of();
          }
        }
    );
  }

  private void expectPreempted(ScheduledTask preempted, ScheduledTask preempting) throws Exception {
    scheduler.preemptTask(
        IAssignedTask.build(preempted.getAssignedTask()),
        IAssignedTask.build(preempting.getAssignedTask()));
  }

  private ScheduledTask makeProductionTask(String role, String job, String taskId) {
    return makeTask(role, job, taskId, 0, "prod", true);
  }

  private ScheduledTask makeProductionTask(String role, String job, String taskId, int priority) {
    return makeTask(role, job, taskId, priority, "prod", true);
  }

  private ScheduledTask makeTask(String role, String job, String taskId, int priority,
                                 String env, boolean production) {
    AssignedTask assignedTask = new AssignedTask()
        .setTaskId(taskId)
        .setTask(new TaskConfig()
            .setOwner(new Identity(role, role))
            .setPriority(priority)
            .setProduction(production)
            .setJobName(job)
            .setEnvironment(env)
            .setConstraints(new HashSet<Constraint>()));
    ScheduledTask scheduledTask = new ScheduledTask()
        .setStatus(PENDING)
        .setAssignedTask(assignedTask);
    addEvent(scheduledTask, PENDING);
    return scheduledTask;
  }

  private ScheduledTask makeTask(String role, String job, String taskId) {
    return makeTask(role, job, taskId, 0, "dev", false);
  }

  private ScheduledTask makeTask(String role, String job, String taskId, int priority) {
    return makeTask(role, job, taskId, priority, "dev", false);
  }

  private void addEvent(ScheduledTask task, ScheduleStatus status) {
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), status));
  }

  private void runOnHost(ScheduledTask task, String host) {
    task.setStatus(RUNNING);
    addEvent(task, RUNNING);
    task.getAssignedTask().setSlaveHost(host);
  }

  private Attribute host(String host) {
    return new Attribute(HOST_ATTRIBUTE, ImmutableSet.of(host));
  }

  private Attribute rack(String rack) {
    return new Attribute(RACK_ATTRIBUTE, ImmutableSet.of(rack));
  }

  // Sets up a normal host, no dedicated hosts and no maintenance.
  private void setUpHost(String host, String rack) {
    HostAttributes hostAttrs = new HostAttributes().setHost(host).setSlaveId(host + "_id")
        .setMode(NONE).setAttributes(ImmutableSet.of(rack(rack), host(host)));

    expect(this.storageUtil.attributeStore.getHostAttributes(host))
        .andReturn(Optional.of(hostAttrs)).anyTimes();
    expect(this.maintenance.getMode(host)).andReturn(NONE).anyTimes();
  }
}
