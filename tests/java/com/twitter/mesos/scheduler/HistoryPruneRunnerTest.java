package com.twitter.mesos.scheduler;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.SlaveID;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.comm.AdjustRetainedTasks;
import com.twitter.mesos.gen.comm.ExecutorMessage;
import com.twitter.mesos.scheduler.MesosSchedulerImpl.SlaveHosts;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

/**
 * @author William Farner
 */
public class HistoryPruneRunnerTest extends BaseStateManagerTest {

  private static final ExecutorID EXECUTOR = ExecutorID.newBuilder().setValue("executor").build();

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
  private SlaveHosts slaveHosts;

  private Runnable runner;

  private static SlaveID slaveId(String host) {
    return SlaveID.newBuilder().setValue(host).build();
  }

  @Before
  public void setUp() {
    driver = createMock(Driver.class);
    pruner = createMock(HistoryPruner.class);
    slaveHosts = createMock(SlaveHosts.class);
    runner = new HistoryPruneRunner(driver, stateManager, pruner, EXECUTOR, slaveHosts);
  }

  @Test
  public void testNoTasks() {
    control.replay();
    runner.run();
  }

  @Test
  public void testNoPruning() {
    ScheduledTask pending = makeTask(PENDING);
    ScheduledTask running = makeTask(RUNNING);
    ScheduledTask finished = makeTask(FINISHED);
    ScheduledTask lost = makeTask(LOST);
    insertTasks(pending, running, finished, lost);

    expectPruneCandidates(finished, lost).andReturn(ImmutableSet.<ScheduledTask>of());

    control.replay();
    runner.run();
  }

  @Test
  public void testPruning() {
    ScheduledTask pending = makeTask(PENDING);
    ScheduledTask running = makeTask(RUNNING, HOST_A);
    ScheduledTask finished = makeTask(FINISHED, HOST_A);
    ScheduledTask failed = makeTask(FAILED, HOST_B);
    ScheduledTask lost = makeTask(LOST, HOST_A);
    insertTasks(pending, running, finished, failed, lost);

    expectPruneCandidates(finished, failed, lost).andReturn(ImmutableSet.of(lost, failed));
    expectGetSlaves();
    expectRetainTasks(HOST_A, running, finished);
    expectRetainTasks(HOST_B);

    control.replay();
    runner.run();

    assertDeleted(lost, failed);
  }

  @Test
  public void testPruneUnknownHosts() {
    ScheduledTask running = makeTask(RUNNING, HOST_A);
    ScheduledTask finished = makeTask(FINISHED, HOST_A);
    ScheduledTask failed = makeTask(FAILED, UNKNOWN_HOST);
    insertTasks(running, finished, failed);

    expectPruneCandidates(finished, failed).andReturn(ImmutableSet.of(finished, failed));
    expectGetSlaves();
    expectRetainTasks(HOST_A, running);

    control.replay();
    runner.run();

    assertDeleted(failed);
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

  private void insertTasks(final ScheduledTask... tasks) {
    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider storeProvider) {
        storeProvider.getTaskStore()
            .saveTasks(ImmutableSet.<ScheduledTask>builder().add(tasks).build());
      }
    });
  }

  private void assertDeleted(final ScheduledTask... tasks) {
    final Query query = new Query(new TaskQuery().setTaskIds(ids(tasks)));
    assertEquals(ImmutableSet.<String>of(),
        storage.doInTransaction(new Work.Quiet<Set<String>>() {
          @Override
          public Set<String> apply(StoreProvider storeProvider) {
            return storeProvider.getTaskStore().fetchTaskIds(query);
          }
        }));
  }

  private IExpectationSetters<Set<ScheduledTask>> expectPruneCandidates(ScheduledTask... tasks) {
    return expect(pruner.apply(ImmutableSet.<ScheduledTask>builder().add(tasks).build()));
  }

  private void expectGetSlaves() {
    expect(slaveHosts.getSlaves()).andReturn(SLAVES);
  }

  private Set<String> ids(ScheduledTask... tasks) {
    return ImmutableSet.copyOf(Iterables.transform(
        ImmutableSet.<ScheduledTask>builder().add(tasks).build(), Tasks.SCHEDULED_TO_ID));
  }

  private void expectRetainTasks(String host, ScheduledTask... tasks) {
    Set<String> taskIds = ids(tasks);

    driver.sendMessage(
        ExecutorMessage.adjustRetainedTasks(new AdjustRetainedTasks(taskIds)),
        slaveId(host),
        EXECUTOR);
  }
}
