package com.twitter.mesos.scheduler;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.stats.StatsProvider;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.events.PubsubEvent.TasksDeleted;
import com.twitter.mesos.scheduler.storage.testing.StorageTestUtil;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.INIT;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;

public class StateManagerVarsTest extends EasyMockTest {

  private static final String ROLE_A = "role_a";
  private static final String JOB_A = "job_a";
  private static final String JOB_B = "job_b";
  private static final String TASK_ID = "task_id";

  private StorageTestUtil storageUtil;
  private StatsProvider trackedStats;
  private Map<String, AtomicLong> totalCounters;
  private Map<String, AtomicLong> perJobCounters;
  private StateManagerVars vars;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);

    totalCounters = Maps.newHashMap();
    trackedStats = createMock(StatsProvider.class);
    collectStatsInto(trackedStats, totalCounters);

    perJobCounters = Maps.newHashMap();
    StatsProvider untrackedStats = createMock(StatsProvider.class);
    expect(trackedStats.untracked()).andReturn(untrackedStats).anyTimes();
    collectStatsInto(untrackedStats, perJobCounters);
  }

  private void initialize() {
    vars = new StateManagerVars(storageUtil.storage, trackedStats);
  }

  private static void collectStatsInto(
      StatsProvider mockProvider,
      final Map<String, AtomicLong> counters) {

    expect(mockProvider.makeCounter(EasyMock.<String>anyObject())).andAnswer(
        new IAnswer<AtomicLong>() {
          @Override public AtomicLong answer() {
            String name = (String) EasyMock.getCurrentArguments()[0];
            AtomicLong counter = new AtomicLong();
            counters.put(name, counter);
            return counter;
          }
        }
    ).anyTimes();
  }

  private void changeState(ScheduledTask task, ScheduleStatus status) {
    ScheduleStatus oldState = task.getStatus();
    task.setStatus(status);
    vars.taskChangedState(new TaskStateChange(task, oldState));
  }

  private void taskDeleted(ScheduledTask task) {
    vars.tasksDeleted(new TasksDeleted(ImmutableSet.of(task)));
  }

  private ScheduledTask makeTask(String job, ScheduleStatus status) {
    return new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask()
            .setTaskId(TASK_ID)
            .setTask(new TwitterTaskInfo()
                .setJobName(job)
                .setOwner(new Identity(ROLE_A, ROLE_A + "-user"))));
  }

  private void assertAllZero() {
    for (ScheduleStatus status : ScheduleStatus.values()) {
      assertCount(0, status);
    }
  }

  @Test
  public void testStartsAtZero() {
    control.replay();
    initialize();
    assertAllZero();
  }

  @Test
  public void testTaskLifeCycle() {
    control.replay();
    initialize();

    ScheduledTask taskA = makeTask(JOB_A, INIT);
    changeState(taskA, PENDING);
    assertCounts(0, JOB_A, INIT);
    assertCounts(1, JOB_A, PENDING);
    changeState(taskA, ASSIGNED);
    assertCounts(0, JOB_A, PENDING);
    assertCounts(1, JOB_A, ASSIGNED);
    changeState(taskA, RUNNING);
    assertCounts(0, JOB_A, ASSIGNED);
    assertCounts(1, JOB_A, RUNNING);
    changeState(taskA, FINISHED);
    assertCounts(0, JOB_A, RUNNING);
    assertCounts(1, JOB_A, FINISHED);
    taskDeleted(taskA);
    assertAllZero();
  }

  @Test
  public void testLoadsFromStorage() {
    storageUtil.expectTransactions();
    storageUtil.expectTaskFetch(Query.GET_ALL,
        makeTask(JOB_A, PENDING),
        makeTask(JOB_A, RUNNING),
        makeTask(JOB_A, FINISHED),
        makeTask(JOB_B, PENDING),
        makeTask(JOB_B, FAILED));

    control.replay();
    initialize();

    vars.storageStarted(new StorageStarted());
    assertCount(2, PENDING);
    assertCount(1, RUNNING);
    assertCount(1, FINISHED);
    assertCount(1, FAILED);
    assertJobCount(1, JOB_A, PENDING);
    assertJobCount(1, JOB_A, RUNNING);
    assertJobCount(1, JOB_A, FINISHED);
    assertJobCount(1, JOB_B, PENDING);
    assertJobCount(1, JOB_B, FAILED);
  }

  private void assertCount(long expected, ScheduleStatus status) {
    assertEquals(expected, totalCounters.get(StateManagerVars.getVarName(status)).get());
  }

  private void assertJobCount(long expected, String job, ScheduleStatus status) {
    assertEquals(
        expected,
        perJobCounters.get(StateManagerVars.getVarName(ROLE_A, job, status)).get());
  }

  private void assertCounts(long expected, String job, ScheduleStatus status) {
    assertCount(expected, status);
    if (status != INIT) {
      assertJobCount(expected, job, status);
    }
  }
}
