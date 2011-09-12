package com.twitter.mesos.scheduler;

import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;

import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static org.easymock.EasyMock.expect;

/**
 * @author William Farner
 */
public class PreempterTest extends EasyMockTest {

  private static final String USER_A = "user_a";
  private static final String USER_B = "user_b";
  private static final String JOB_A = "job_a";
  private static final String JOB_B = "job_b";
  private static final String TASK_ID_A = "task_a";
  private static final String TASK_ID_B = "task_b";
  private static final String TASK_ID_C = "task_c";
  private static final String HOST_A = "host_a";
  private static final String HOST_B = "host_b";

  private static final Amount<Long, Time> preemptionCandidacyDelay = Amount.of(30L, Time.SECONDS);

  private SchedulerCore scheduler;
  private SchedulingFilter schedulingFilter;
  private Predicate<TwitterTaskInfo> dynamicHostFilter;
  private FakeClock clock;
  private Preempter preempter;

  private FakeStorage storage;

  @Before
  public void setUp() {
    scheduler = createMock(SchedulerCore.class);
    schedulingFilter = createMock(SchedulingFilter.class);
    dynamicHostFilter = createMock(new Clazz<Predicate<TwitterTaskInfo>>() {});
    clock = new FakeClock();
    preempter = new Preempter(scheduler, schedulingFilter, preemptionCandidacyDelay, clock);
    storage = new FakeStorage();
  }

  // TODO(wfarner): Put together a SchedulerPreempterIntegrationTest as well.
  // May want to just have a PreempterBaseTest, PreempterTest, PreempterSchedulerIntegrationTest.

  @Test
  public void testNoPendingTasks() {
    expectGetTasks();

    control.replay();
    preempter.run();
  }

  @Test
  public void testRecentlyPending() {
    TaskState lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A);
    runOnHost(lowPriority, HOST_A);

    makeTask(USER_A, JOB_A, TASK_ID_B, 100);

    expectGetTasks();

    control.replay();
    preempter.run();
  }

  @Test
  public void testPreempted() throws Exception {
    TaskState lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A);
    runOnHost(lowPriority, HOST_A);

    TaskState highPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 100);
    clock.advance(preemptionCandidacyDelay);

    expectGetTasks().times(2);

    expectFiltering();
    expectPreempted(lowPriority, highPriority);

    control.replay();
    preempter.run();
  }

  @Test
  public void testLowestPriorityPreempted() throws Exception {
    TaskState lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A, 10);
    runOnHost(lowPriority, HOST_A);

    TaskState lowerPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 1);
    runOnHost(lowerPriority, HOST_A);

    TaskState highPriority = makeTask(USER_A, JOB_A, TASK_ID_C, 100);
    clock.advance(preemptionCandidacyDelay);

    expectGetTasks().times(2);

    expectFiltering();
    expectPreempted(lowerPriority, highPriority);

    control.replay();
    preempter.run();
  }

  @Test
  public void testRespectsDynamicFilter() throws Exception {
    TaskState lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A);
    runOnHost(lowPriority, HOST_A);

    TaskState highPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 100);
    clock.advance(preemptionCandidacyDelay);

    expectGetTasks().times(2);

    expectStaticFiltering();
    expect(schedulingFilter.dynamicHostFilter(scheduler, HOST_A)).andReturn(dynamicHostFilter);
    expect(dynamicHostFilter.apply(highPriority.task.getAssignedTask().getTask())).andReturn(false);

    control.replay();
    preempter.run();
  }

  @Test
  public void testHigherPriorityRunning() throws Exception {
    TaskState highPriority = makeTask(USER_A, JOB_A, TASK_ID_B, 100);
    runOnHost(highPriority, HOST_A);

    makeTask(USER_A, JOB_A, TASK_ID_A);
    clock.advance(preemptionCandidacyDelay);

    expectGetTasks().times(2);
    expectFiltering();

    control.replay();
    preempter.run();
  }

  @Test
  public void testOversubscribed() throws Exception {
    TaskState lowPriority = makeTask(USER_A, JOB_A, TASK_ID_A);
    runOnHost(lowPriority, HOST_A);

    // Despite having two high priority tasks, we only perform one eviction.
    TaskState highPriority1 = makeTask(USER_A, JOB_A, TASK_ID_B, 100);
    TaskState highPriority2 = makeTask(USER_A, JOB_A, TASK_ID_C, 100);
    clock.advance(preemptionCandidacyDelay);

    expectGetTasks().times(2);

    expectFiltering();
    expectPreempted(lowPriority, highPriority1);

    control.replay();
    preempter.run();
  }

  @Test
  public void testProductionPreemptingNonproduction() throws Exception {
    // Use a very low priority for the production task to show that priority is irrelevant.
    TaskState p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", -1000);
    TaskState a1 = makeTask(USER_A, JOB_A, TASK_ID_B + "_a1", 100);
    runOnHost(a1, HOST_A);

    clock.advance(preemptionCandidacyDelay);
    expectGetTasks().times(2);

    expectFiltering();
    expectPreempted(a1, p1);

    control.replay();
    preempter.run();
  }

  @Test
  public void testProductionPreemptingNonproductionAcrossUsers() throws Exception {
    // Use a very low priority for the production task to show that priority is irrelevant.
    TaskState p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", -1000);
    TaskState a1 = makeTask(USER_B, JOB_A, TASK_ID_B + "_a1", 100);
    runOnHost(a1, HOST_A);

    clock.advance(preemptionCandidacyDelay);
    expectGetTasks().times(2);

    expectFiltering();
    expectPreempted(a1, p1);

    control.replay();
    preempter.run();
  }

  @Test
  public void testProductionUsersDoNotPreemptEachOther() throws Exception {
    TaskState p1 = makeProductionTask(USER_A, JOB_A, TASK_ID_A + "_p1", 1000);
    TaskState a1 = makeProductionTask(USER_B, JOB_A, TASK_ID_B + "_a1", 0);
    runOnHost(a1, HOST_A);

    clock.advance(preemptionCandidacyDelay);
    expectGetTasks().times(2);

    expectFiltering();

    control.replay();
    preempter.run();
  }

  @Test
  public void testInterleavedPriorities() throws Exception {
    TaskState p1 = makeTask(USER_A, JOB_A, TASK_ID_A + "_p1", 1);
    TaskState a3 = makeTask(USER_A, JOB_A, TASK_ID_B + "_a3", 3);
    TaskState p2 = makeTask(USER_A, JOB_B, TASK_ID_A + "_p2", 2);
    TaskState a2 = makeTask(USER_A, JOB_B, TASK_ID_B + "_a2", 2);
    TaskState p3 = makeTask(USER_B, JOB_A, TASK_ID_A + "_p3", 3);
    TaskState a1 = makeTask(USER_A, JOB_A, TASK_ID_B + "_a1", 1);
    runOnHost(a3, HOST_A);
    runOnHost(a2, HOST_A);
    runOnHost(a1, HOST_B);

    clock.advance(preemptionCandidacyDelay);

    expectGetTasks().times(2);

    expectFiltering();
    expectPreempted(a1, p2);

    control.replay();
    preempter.run();
  }

  private void expectFiltering() {
    expectStaticFiltering();
    expect(schedulingFilter.dynamicHostFilter(
        EasyMock.<SchedulerCore>anyObject(), EasyMock.<String>anyObject()))
        .andReturn(Predicates.<TwitterTaskInfo>alwaysTrue())
        .anyTimes();
  }

  private void expectStaticFiltering() {
    expect(schedulingFilter.staticFilter(EasyMock.<Resources>anyObject(), EasyMock
        .<String>anyObject()))
        .andReturn(Predicates.<TwitterTaskInfo>alwaysTrue())
        .anyTimes();
  }

  private IExpectationSetters<Set<TaskState>> expectGetTasks() {
    return expect(scheduler.getTasks(EasyMock.<Query>anyObject())).andAnswer(
        new IAnswer<Set<TaskState>>() {
          @Override public Set<TaskState> answer() {
            return storage.fetch((Query) EasyMock.getCurrentArguments()[0]);
          }
        }
    );
  }

  private void expectPreempted(TaskState preempted, TaskState preempting) throws Exception {
    scheduler.preemptTask(preempted.task.getAssignedTask(), preempting.task.getAssignedTask());
  }

  private TaskState makeTask(String role, String job, String taskId, int priority) {
    return makeTask(role, job, taskId, priority, false);
  }

  private TaskState makeProductionTask(String role, String job, String taskId, int priority) {
    return makeTask(role, job, taskId, priority, true);
  }

  private TaskState makeTask(String role, String job, String taskId, int priority,
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
    TaskState state = new TaskState(scheduledTask, new VolatileTaskState());
    addEvent(state, PENDING);
    storage.addTask(state);
    return state;
  }

  private TaskState makeTask(String role, String job, String taskId) {
    return makeTask(role, job, taskId, 0);
  }

  private void addEvent(TaskState state, ScheduleStatus status) {
    state.task.addToTaskEvents(new TaskEvent(clock.nowMillis(), status, null));
  }

  private void runOnHost(TaskState state, String host) {
    state.task.setStatus(RUNNING);
    addEvent(state, RUNNING);
    state.task.getAssignedTask().setSlaveHost(host);
  }

  private static class FakeStorage {
    private final Set<TaskState> tasks = Sets.newHashSet();

    void addTask(TaskState state) {
      tasks.add(state);
    }

    Set<TaskState> fetch(final Query query) {
      return ImmutableSet.copyOf(Iterables.filter(tasks,
          Predicates.and(Predicates.compose(query.postFilter(), TaskState.STATE_TO_SCHEDULED),
              new Predicate<TaskState>() {
                @Override public boolean apply(TaskState state) {
                  AssignedTask task = state.task.getAssignedTask();
                  TaskQuery q = query.base();
                  return (!q.isSetOwner() || q.getOwner().equals(task.getTask().getOwner()))
                      && (!q.isSetJobName() || q.getJobName().equals(task.getTask().getJobName()))
                      && (!q.isSetJobKey() || q.getJobKey().equals(Tasks.jobKey(task)))
                      && (!q.isSetTaskIds() || q.getTaskIds().contains(task.getTaskId()))
                      && (!q.isSetStatuses() || q.getStatuses().contains(state.task.getStatus()))
                      && (!q.isSetSlaveHost() || q.getSlaveHost().equals(task.getSlaveHost()))
                      && (!q.isSetShardIds()
                          || q.getShardIds().contains(task.getTask().getShardId()));
                }
              })));
    }
  }
}
