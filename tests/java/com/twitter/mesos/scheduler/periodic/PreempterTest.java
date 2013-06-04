package com.twitter.mesos.scheduler.periodic;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.SchedulerCore;
import com.twitter.mesos.scheduler.configuration.Resources;
import com.twitter.mesos.scheduler.filter.SchedulingFilter;
import com.twitter.mesos.scheduler.storage.testing.StorageTestUtil;

import static org.easymock.EasyMock.expect;

import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.scheduler.filter.SchedulingFilter.Veto;

public class PreempterTest extends EasyMockTest {

  private static final String USER_A = "user_a";
  private static final String USER_B = "user_b";
  private static final String JOB_A = "job_a";
  private static final String JOB_B = "job_b";
  private static final String TASK_ID_A = "task_a";
  private static final String TASK_ID_B = "task_b";
  private static final String TASK_ID_C = "task_c";
  private static final String TASK_ID_D = "task_d";
  private static final String HOST_A = "host_a";
  private static final String HOST_B = "host_b";

  private static final Amount<Long, Time> PREEMPTION_DELAY = Amount.of(30L, Time.SECONDS);

  private StorageTestUtil storageUtil;
  private SchedulerCore scheduler;
  private SchedulingFilter schedulingFilter;
  private FakeClock clock;
  private Preempter preempter;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    scheduler = createMock(SchedulerCore.class);
    schedulingFilter = createMock(SchedulingFilter.class);
    clock = new FakeClock();
    preempter = new Preempter(
        storageUtil.storage,
        scheduler,
        schedulingFilter,
        PREEMPTION_DELAY,
        clock);
  }

  // TODO(wfarner): Put together a SchedulerPreempterIntegrationTest as well.
  // May want to just have a PreempterBaseTest, PreempterTest, PreempterSchedulerIntegrationTest.

  @Test
  public void testNoPendingTasks() {
    expectGetPendingTasks();

    control.replay();
    preempter.run();
  }

  private void expectGetPendingTasks(ScheduledTask... returnedTasks) {
    storageUtil.expectTaskFetch(Preempter.PENDING_QUERY, returnedTasks);
  }

  private void expectGetActiveTasks(ScheduledTask... returnedTasks) {
    storageUtil.expectTaskFetch(Preempter.ACTIVE_NOT_PENDING_QUERY, returnedTasks);
  }

  @Test
  public void testRecentlyPending() {
    ScheduledTask lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A);
    runOnHost(lowPriority, HOST_A);

    expectGetPendingTasks(lowPriority, makeTask(USER_A, JOB_A, TASK_ID_B, 100));

    control.replay();
    preempter.run();
  }

  @Test
  public void testPreempted() throws Exception {
    ScheduledTask lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A);
    runOnHost(lowPriority, HOST_A);

    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 100);
    clock.advance(PREEMPTION_DELAY);

    expectGetPendingTasks(highPriority);
    expectGetActiveTasks(lowPriority);

    expectFiltering();
    expectPreempted(lowPriority, highPriority);

    control.replay();
    preempter.run();
  }

  @Test
  public void testLowestPriorityPreempted() throws Exception {
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
    preempter.run();
  }

  @Test
  public void testOnePreemptableTask() throws Exception {
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
    preempter.run();
  }

  @Test
  public void testHigherPriorityRunning() throws Exception {
    ScheduledTask highPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 100);
    runOnHost(highPriority, HOST_A);

    ScheduledTask task = makeTask(USER_A, JOB_A, TASK_ID_A);
    clock.advance(PREEMPTION_DELAY);

    expectGetPendingTasks(task);
    expectGetActiveTasks(highPriority);

    control.replay();
    preempter.run();
  }

  @Test
  public void testOversubscribed() throws Exception {
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
    preempter.run();
  }

  @Test
  public void testProductionPreemptingNonproduction() throws Exception {
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
    preempter.run();
  }

  @Test
  public void testProductionPreemptingNonproductionAcrossUsers() throws Exception {
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
    preempter.run();
  }

  @Test
  public void testProductionUsersDoNotPreemptEachOther() throws Exception {
    ScheduledTask p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", 1000);
    ScheduledTask a1 = makeProductionTask(USER_B, JOB_A, TASK_ID_B + "_a1", 0);
    runOnHost(a1, HOST_A);

    clock.advance(PREEMPTION_DELAY);

    expectGetPendingTasks(p1);
    expectGetActiveTasks(a1);

    control.replay();
    preempter.run();
  }

  @Test
  public void testInterleavedPriorities() throws Exception {
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
    preempter.run();
  }

  private IExpectationSetters<Set<Veto>> expectFiltering() {
    return expect(schedulingFilter.filter(
        EasyMock.<Resources>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<TwitterTaskInfo>anyObject(),
        EasyMock.<String>anyObject())).andAnswer(
        new IAnswer<Set<Veto>>() {
          @Override public Set<Veto> answer() {
            return ImmutableSet.of();
          }
        }
    );
  }

  private void expectPreempted(ScheduledTask preempted, ScheduledTask preempting) throws Exception {
    scheduler.preemptTask(preempted.getAssignedTask(), preempting.getAssignedTask());
  }

  private ScheduledTask makeTask(String role, String job, String taskId, int priority) {
    return makeTask(role, job, taskId, priority, false);
  }

  private ScheduledTask makeProductionTask(String role, String job, String taskId, int priority) {
    return makeTask(role, job, taskId, priority, true);
  }

  private ScheduledTask makeTask(String role, String job, String taskId, int priority,
      boolean production) {
    AssignedTask assignedTask = new AssignedTask()
        .setTaskId(taskId)
        .setTask(new TwitterTaskInfo()
            .setOwner(new Identity(role, role))
            .setPriority(priority)
            .setProduction(production)
            .setJobName(job));
    ScheduledTask scheduledTask = new ScheduledTask()
        .setStatus(PENDING)
        .setAssignedTask(assignedTask);
    addEvent(scheduledTask, PENDING);
    return scheduledTask;
  }

  private ScheduledTask makeTask(String role, String job, String taskId) {
    return makeTask(role, job, taskId, 0);
  }

  private void addEvent(ScheduledTask task, ScheduleStatus status) {
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), status));
  }

  private void runOnHost(ScheduledTask task, String host) {
    task.setStatus(RUNNING);
    addEvent(task, RUNNING);
    task.getAssignedTask().setSlaveHost(host);
  }
}
