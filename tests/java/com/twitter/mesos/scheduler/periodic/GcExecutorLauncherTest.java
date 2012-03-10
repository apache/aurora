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
import org.apache.mesos.Protos.TaskDescription;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.comm.AdjustRetainedTasks;
import com.twitter.mesos.scheduler.PulseMonitor;
import com.twitter.mesos.scheduler.StateManager;

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

  private StateManager stateManager;
  private HistoryPruner pruner;
  private PulseMonitor<String> hostMonitor;
  private GcExecutorLauncher gcExecutorLauncher;

  @Before
  public void setUp() {
    stateManager = createMock(StateManager.class);
    pruner = createMock(HistoryPruner.class);
    hostMonitor = createMock(new Clazz<PulseMonitor<String>>() { });
    gcExecutorLauncher =
        new GcExecutorLauncher(hostMonitor, Optional.of("nonempty"), stateManager, pruner);
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
    expectGetInactiveTasks(thermosPrunedTask, thermosTask, nonThermosTask);
    expect(pruner.apply(ImmutableSet.of(thermosPrunedTask, thermosTask)))
        .andReturn(ImmutableSet.<ScheduledTask>of());
    expectGetTasksByHost(HOST, thermosPrunedTask, thermosTask, nonThermosTask);
    hostMonitor.pulse(HOST);

    // Service third createTask - prune one tasks.
    expect(hostMonitor.isAlive(HOST)).andReturn(false);
    expectGetInactiveTasks(thermosPrunedTask, thermosTask, nonThermosTask);
    expect(pruner.apply(ImmutableSet.of(thermosPrunedTask, thermosTask)))
        .andReturn(ImmutableSet.of(thermosPrunedTask));
    expectGetTasksByHost(HOST, thermosPrunedTask, thermosTask, nonThermosTask);
    stateManager.deleteTasks(ImmutableSet.<String>of(Tasks.id(thermosPrunedTask)));
    hostMonitor.pulse(HOST);

    control.replay();

    // First call - hostMonitor returns true, no GC.
    Optional<TaskDescription> taskDescription = gcExecutorLauncher.createTask(OFFER);
    assertFalse(taskDescription.isPresent());

    // Second call - no tasks pruned.
    taskDescription = gcExecutorLauncher.createTask(OFFER);
    assertTrue(taskDescription.isPresent());
    assertRetainedTasks(taskDescription.get(), thermosTask, thermosPrunedTask);

    thermosPrunedTask.setStatus(FAILED);
    thermosTask.setStatus(FAILED);
    nonThermosTask.setStatus(FAILED);

    // Third call - one task pruned.
    taskDescription = gcExecutorLauncher.createTask(OFFER);
    assertTrue(taskDescription.isPresent());
    assertRetainedTasks(taskDescription.get(), thermosTask);
  }

  private static void assertRetainedTasks(TaskDescription taskDescription, ScheduledTask... tasks)
      throws ThriftBinaryCodec.CodingException {
    AdjustRetainedTasks message = ThriftBinaryCodec.decode(
        AdjustRetainedTasks.class, taskDescription.getData().toByteArray());
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

  private void expectGetInactiveTasks(ScheduledTask... tasks) {
    expect(stateManager.fetchTasks(HistoryPruneRunner.INACTIVE_QUERY))
        .andReturn(ImmutableSet.<ScheduledTask>builder().add(tasks).build());
  }

  private void expectGetTasksByHost(String host, ScheduledTask... tasks) {
    expect(stateManager.fetchTasks(HistoryPruneRunner.hostQuery(host)))
        .andReturn(ImmutableSet.<ScheduledTask>builder().add(tasks).build());
  }
}
