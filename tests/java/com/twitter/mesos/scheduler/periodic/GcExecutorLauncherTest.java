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

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.comm.AdjustRetainedTasks;
import com.twitter.mesos.scheduler.BaseStateManagerTest;
import com.twitter.mesos.scheduler.PulseMonitor;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.periodic.HistoryPruner.HistoryPrunerImpl;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;

public class GcExecutorLauncherTest extends BaseStateManagerTest {

  private static final String HOST = "slave-host";

  private static final Amount<Long, Time> ONE_DAY = Amount.of(1L, Time.DAYS);
  private static final Amount<Long, Time> ONE_HOUR = Amount.of(1L, Time.HOURS);

  private static final Offer OFFER = Offer.newBuilder()
        .setSlaveId(SlaveID.newBuilder().setValue("slave-id"))
        .setHostname(HOST)
        .setFrameworkId(FrameworkID.newBuilder().setValue("framework-id").build())
        .setId(OfferID.newBuilder().setValue("offer-id"))
        .build();

  private static final String JOB_A = "jobA";

  private static final int PER_JOB_HISTORY = 1;

  private final AtomicInteger taskIdCounter = new AtomicInteger();

  private FakeClock clock;
  private HistoryPruner pruner;
  private PulseMonitor<String> hostMonitor;
  private GcExecutorLauncher gcExecutorLauncher;

  private static void assertRetainedTasks(TaskDescription taskDescription, ScheduledTask... tasks)
      throws ThriftBinaryCodec.CodingException {
    AdjustRetainedTasks message = ThriftBinaryCodec.decode(
        AdjustRetainedTasks.class, taskDescription.getData().toByteArray());
    Map<String, ScheduledTask> byId =
        Maps.uniqueIndex(ImmutableSet.copyOf(tasks), Tasks.SCHEDULED_TO_ID);
    assertEquals(message.getRetainedTasks(), Maps.transformValues(byId, Tasks.GET_STATUS));
  }

  @Before
  public void setUp() {
    clock = new FakeClock();
    pruner = new HistoryPrunerImpl(clock, ONE_DAY, PER_JOB_HISTORY);
    hostMonitor = createMock(new Clazz<PulseMonitor<String>>() { });
    gcExecutorLauncher =
        new GcExecutorLauncher(hostMonitor, Optional.of("nonempty"), stateManager, pruner);
  }

  @Test
  public void testNoPruning() throws ThriftBinaryCodec.CodingException {
    hostMonitor.pulse("slave-host");
    hostMonitor.pulse("slave-host");
    expect(hostMonitor.isAlive("slave-host")).andReturn(true);
    expect(hostMonitor.isAlive("slave-host")).andReturn(false).anyTimes();

    control.replay();

    ScheduledTask thermosTask = makeTask(JOB_A, true);
    clock.advance(ONE_HOUR);
    ScheduledTask thermosTask2 = makeTask(JOB_A, true);
    clock.advance(ONE_HOUR);
    ScheduledTask nonThermosTask = makeTask(JOB_A, false);
    clock.advance(ONE_HOUR);

    insertTasks(thermosTask, thermosTask2, nonThermosTask);

    changeState(thermosTask, RUNNING);
    changeState(thermosTask2, RUNNING);
    changeState(nonThermosTask, RUNNING);

    // Initially pulse true.
    Optional<TaskDescription> taskDescription = gcExecutorLauncher.createTask(OFFER);
    assertFalse(taskDescription.isPresent());

    // Pulse false.
    taskDescription = gcExecutorLauncher.createTask(OFFER);
    assertTrue(taskDescription.isPresent());
    assertRetainedTasks(taskDescription.get(), thermosTask, thermosTask2);

    changeState(thermosTask, FAILED);
    changeState(thermosTask2, FAILED);
    changeState(nonThermosTask, FAILED);

    taskDescription = gcExecutorLauncher.createTask(OFFER);
    assertTrue(taskDescription.isPresent());
    assertRetainedTasks(taskDescription.get(), thermosTask2);
  }

  private ScheduledTask makeTask(String jobName, boolean isThermos) {
    return new ScheduledTask()
        .setStatus(ScheduleStatus.ASSIGNED)
        .setAssignedTask(new AssignedTask()
            .setTaskId("task-" + taskIdCounter.incrementAndGet())
            .setSlaveHost(HOST)
            .setTask(new TwitterTaskInfo()
                .setJobName(jobName)
                .setOwner(new Identity().setRole("role").setUser("user"))
                .setThermosConfig(isThermos ? new byte[] {1, 2, 3} : new byte[] {})));
  }

  private void insertTasks(final ScheduledTask... tasks) {
    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider storeProvider) {
        storeProvider.getTaskStore()
                     .saveTasks(ImmutableSet.<ScheduledTask>builder().add(tasks).build());
      }
    });
  }

  private ScheduledTask changeState(ScheduledTask task, ScheduleStatus status) {
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), status));
    task.setStatus(status);
    stateManager.changeState(Query.byId(Tasks.id(task)), status);
    return task;
  }
}
