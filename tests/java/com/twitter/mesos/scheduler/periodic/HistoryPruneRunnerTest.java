package com.twitter.mesos.scheduler.periodic;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.SlaveID;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.comm.AdjustRetainedTasks;
import com.twitter.mesos.gen.comm.ExecutorMessage;
import com.twitter.mesos.scheduler.Driver;
import com.twitter.mesos.scheduler.MesosSchedulerImpl.SlaveHosts;
import com.twitter.mesos.scheduler.MesosTaskFactory;
import com.twitter.mesos.scheduler.StateManager;

import static org.easymock.EasyMock.expect;

import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;

/**
 * @author William Farner
 */
public class HistoryPruneRunnerTest extends EasyMockTest {

  private static final ExecutorID EXECUTOR = MesosTaskFactory.DEFAULT_EXECUTOR_ID;

  private static final String HOST_A = "host_a";
  private static final SlaveID HOST_A_ID =  slaveId(HOST_A);
  private static final String HOST_B = "host_b";
  private static final SlaveID HOST_B_ID = slaveId(HOST_B);
  private static final String UNKNOWN_HOST = "unknown_host";
  private static final Map<String, SlaveID> SLAVES =
      ImmutableMap.of(HOST_A, HOST_A_ID, HOST_B, HOST_B_ID);

  private final AtomicInteger taskIdCounter = new AtomicInteger();

  private Driver driver;
  private HistoryPruner pruner;
  private StateManager stateManager;
  private SlaveHosts slaveHosts;

  private Runnable runner;

  private static SlaveID slaveId(String host) {
    return SlaveID.newBuilder().setValue(host).build();
  }

  @Before
  public void setUp() {
    driver = createMock(Driver.class);
    pruner = createMock(HistoryPruner.class);
    stateManager = createMock(StateManager.class);
    slaveHosts = createMock(SlaveHosts.class);
    runner = new HistoryPruneRunner(driver, stateManager, pruner, slaveHosts);
  }

  @Test
  public void testNoTasks() {
    expectGetInactiveTasks();

    control.replay();
    runner.run();
  }

  private void expectGetInactiveTasks(ScheduledTask... tasks) {
    expect(stateManager.fetchTasks(HistoryPruneRunner.INACTIVE_QUERY))
        .andReturn(ImmutableSet.<ScheduledTask>builder().add(tasks).build());
  }

  private void expectGetTasksByHost(String host, ScheduledTask... tasks) {
    expect(stateManager.fetchTasks(HistoryPruneRunner.hostQuery(host)))
        .andReturn(ImmutableSet.<ScheduledTask>builder().add(tasks).build());
  }

  @Test
  public void testNoPruning() {
    ScheduledTask finished = makeTask(FINISHED);
    ScheduledTask lost = makeTask(LOST);

    expectGetInactiveTasks(finished, lost);
    expectPruneCandidates(finished, lost).andReturn(ImmutableSet.<ScheduledTask>of());

    control.replay();
    runner.run();
  }

  @Test
  public void testPruning() {
    ScheduledTask running = makeTask(RUNNING, HOST_A);
    ScheduledTask finished = makeTask(FINISHED, HOST_A);
    ScheduledTask failed = makeTask(FAILED, HOST_B);
    ScheduledTask lost = makeTask(LOST, HOST_A);

    expectGetInactiveTasks(failed, finished, lost);
    expectPruneCandidates(finished, failed, lost).andReturn(ImmutableSet.of(lost, failed));
    expectGetSlaves();

    expectGetTasksByHost(HOST_A, running, finished);
    expectRetainTasks(HOST_A, running, finished);
    expectGetTasksByHost(HOST_B, failed);
    expectRetainTasks(HOST_B);

    stateManager.deleteTasks(ImmutableSet.of(Tasks.id(lost), Tasks.id(failed)));

    control.replay();
    runner.run();
  }

  @Test
  public void testPruneUnknownHosts() {
    ScheduledTask running = makeTask(RUNNING, HOST_A);
    ScheduledTask finished = makeTask(FINISHED, HOST_A);
    ScheduledTask failed = makeTask(FAILED, UNKNOWN_HOST);

    expectGetInactiveTasks(failed, finished);
    expectPruneCandidates(finished, failed).andReturn(ImmutableSet.of(finished, failed));
    expectGetSlaves();
    expectGetTasksByHost(HOST_A, running, finished);
    expectRetainTasks(HOST_A, running);

    stateManager.deleteTasks(Tasks.ids(finished, failed));

    control.replay();
    runner.run();
  }

  private ScheduledTask makeTask(ScheduleStatus status) {
    return new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(makeAssignedTask());
  }

  private ScheduledTask makeTask(ScheduleStatus status, String host) {
    ScheduledTask task = makeTask(status);
    task.getAssignedTask().setSlaveHost(host);
    return task;
  }

  private AssignedTask makeAssignedTask() {
    return new AssignedTask()
        .setTaskId("task-" + taskIdCounter.incrementAndGet())
        .setTask(new TwitterTaskInfo()
            .setOwner(new Identity().setRole("role").setUser("user"))
            .setJobName("jobName")
            .setShardId(0));
  }

  private IExpectationSetters<Set<ScheduledTask>> expectPruneCandidates(ScheduledTask... tasks) {
    return expect(pruner.apply(ImmutableSet.<ScheduledTask>builder().add(tasks).build()));
  }

  private void expectGetSlaves() {
    expect(slaveHosts.getSlaves()).andReturn(SLAVES);
  }

  private void expectRetainTasks(String host, ScheduledTask... tasks) {
    Map<String, ScheduledTask> byId = Tasks.mapById(ImmutableSet.copyOf(tasks));
    Map<String, ScheduleStatus> idToStatus = Maps.transformValues(byId, Tasks.GET_STATUS);

    AdjustRetainedTasks message = new AdjustRetainedTasks().setRetainedTasks(idToStatus);
    driver.sendMessage(ExecutorMessage.adjustRetainedTasks(message), slaveId(host), EXECUTOR);
  }
}
