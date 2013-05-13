package com.twitter.mesos.scheduler;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.stats.StatsProvider;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.HostAttributes;
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
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;

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

  private void changeState(ScheduledTask task, ScheduleStatus status) {
    ScheduleStatus oldState = task.getStatus();
    task.setStatus(status);
    vars.taskChangedState(new TaskStateChange(task, oldState));
  }

  private void expectLoadStorage(ScheduledTask... result) {
    storageUtil.expectTransactions();
    storageUtil.expectTaskFetch(Query.GET_ALL, result);
    globalCounters = Maps.newHashMap();
    for (ScheduleStatus status : ScheduleStatus.values()) {
      AtomicLong counter = new AtomicLong(0);
      globalCounters.put(status, counter);
      expect(trackedStats.makeCounter(TaskVars.getVarName(status))).andReturn(counter);
    }
  }

  private ScheduledTask makeTask(String job, ScheduleStatus status, String host) {
    return new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask()
            .setTaskId(TASK_ID)
            .setSlaveHost(host)
            .setTask(new TwitterTaskInfo()
                .setJobName(job)
                .setOwner(new Identity(ROLE_A, ROLE_A + "-user"))));
  }

  private ScheduledTask makeTask(String job, ScheduleStatus status) {
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

    ScheduledTask taskA = makeTask(JOB_A, INIT);
    changeState(taskA, PENDING);
    assertEquals(1, globalCounters.get(PENDING).get());
    changeState(taskA, ASSIGNED);
    assertEquals(0, globalCounters.get(PENDING).get());
    assertEquals(1, globalCounters.get(ASSIGNED).get());
    changeState(taskA, RUNNING);
    assertEquals(0, globalCounters.get(ASSIGNED).get());
    assertEquals(1, globalCounters.get(RUNNING).get());
    changeState(taskA, FINISHED);
    assertEquals(0, globalCounters.get(RUNNING).get());
    assertEquals(1, globalCounters.get(FINISHED).get());
    vars.tasksDeleted(new TasksDeleted(ImmutableSet.of(taskA)));
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

    ScheduledTask a = makeTask("jobA", RUNNING, "host1");
    ScheduledTask b = makeTask("jobB", RUNNING, "host2");
    ScheduledTask c = makeTask("jobC", RUNNING, "host3");
    ScheduledTask d = makeTask("jobD", RUNNING, "host1");

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

    ScheduledTask a = makeTask(JOB_A, RUNNING, "a");
    changeState(a, LOST);
    // Since no attributes are stored for the host, a variable is not exported/updated.
  }
}
