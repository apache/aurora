package com.twitter.mesos.scheduler.periodic;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.comm.AdjustRetainedTasks;
import com.twitter.mesos.scheduler.PulseMonitor;
import com.twitter.mesos.scheduler.storage.testing.StorageTestUtil;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static com.twitter.mesos.gen.ScheduleStatus.FAILED;

public class GcExecutorLauncherTest extends EasyMockTest {

  private static final String HOST = "slave-host";

  private static final Offer OFFER = Offer.newBuilder()
        .setSlaveId(SlaveID.newBuilder().setValue("slave-id"))
        .setHostname(HOST)
        .setFrameworkId(FrameworkID.newBuilder().setValue("framework-id").build())
        .setId(OfferID.newBuilder().setValue("offer-id"))
        .build();

  private static final String JOB_A = "jobA";

  private final AtomicInteger taskIdCounter = new AtomicInteger();

  private StorageTestUtil storageUtil;
  private PulseMonitor<String> hostMonitor;
  private GcExecutorLauncher gcExecutorLauncher;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectTransactions();
    hostMonitor = createMock(new Clazz<PulseMonitor<String>>() { });
    gcExecutorLauncher = new GcExecutorLauncher(
        hostMonitor,
        Optional.of("nonempty"),
        storageUtil.storage);
  }

  @Test
  public void testPruning() throws ThriftBinaryCodec.CodingException {
    ScheduledTask thermosPrunedTask = makeTask(JOB_A, true, FAILED);
    ScheduledTask thermosTask = makeTask(JOB_A, true, FAILED);
    ScheduledTask nonThermosTask = makeTask(JOB_A, false, FAILED);

    // Service first createTask - no hosts ready for GC.
    expect(hostMonitor.isAlive(HOST)).andReturn(true);

    // Service second createTask - prune no tasks.
    expect(hostMonitor.isAlive(HOST)).andReturn(false);
    expectGetTasksByHost(HOST, thermosPrunedTask, thermosTask, nonThermosTask);
    hostMonitor.pulse(HOST);

    // Service third createTask - prune one tasks.
    expect(hostMonitor.isAlive(HOST)).andReturn(false);
    expectGetTasksByHost(HOST, thermosPrunedTask);
    hostMonitor.pulse(HOST);

    control.replay();

    // First call - hostMonitor returns true, no GC.
    Optional<TaskInfo> taskInfo = gcExecutorLauncher.createTask(OFFER);
    assertFalse(taskInfo.isPresent());

    // Second call - no tasks pruned.
    taskInfo = gcExecutorLauncher.createTask(OFFER);
    assertTrue(taskInfo.isPresent());
    assertRetainedTasks(taskInfo.get(), thermosPrunedTask, thermosTask, nonThermosTask);

    // Third call - two tasks pruned.
    taskInfo = gcExecutorLauncher.createTask(OFFER);
    assertTrue(taskInfo.isPresent());
    assertRetainedTasks(taskInfo.get(), thermosPrunedTask);
  }

  private static void assertRetainedTasks(TaskInfo taskInfo, ScheduledTask... tasks)
      throws ThriftBinaryCodec.CodingException {
    AdjustRetainedTasks message = ThriftBinaryCodec.decode(
        AdjustRetainedTasks.class, taskInfo.getData().toByteArray());
    Map<String, ScheduledTask> byId = Tasks.mapById(ImmutableSet.copyOf(tasks));
    assertEquals(Maps.transformValues(byId, Tasks.GET_STATUS), message.getRetainedTasks());
  }

  private ScheduledTask makeTask(String jobName, boolean isThermos, ScheduleStatus status) {
    return new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask()
            .setTaskId("task-" + taskIdCounter.incrementAndGet())
            .setSlaveHost(HOST)
            .setTask(new TwitterTaskInfo()
                .setJobName(jobName)
                .setOwner(new Identity().setRole("role").setUser("user"))
                .setThermosConfig(isThermos ? new byte[] {1, 2, 3} : new byte[] {})));
  }

  private void expectGetTasksByHost(String host, ScheduledTask... tasks) {
    storageUtil.expectTaskFetch(new TaskQuery().setSlaveHost(host), tasks);
  }
}
