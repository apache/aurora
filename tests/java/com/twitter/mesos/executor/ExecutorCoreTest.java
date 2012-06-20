package com.twitter.mesos.executor;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.executor.ProcessKiller.KillCommand;
import com.twitter.mesos.executor.ProcessKiller.KillException;
import com.twitter.mesos.executor.ProcessScanner.ProcessInfo;
import com.twitter.mesos.executor.Task.AuditedStatus;
import com.twitter.mesos.executor.Task.TaskRunException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TwitterTaskInfo;

import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ExecutorCoreTest extends EasyMockTest {
  private static final String USER_A = "user-a";
  private static final String ROLE_A = "role-a";
  private static final Identity OWNER_A = new Identity(ROLE_A, USER_A);
  private static final String JOB_A = "job-a";

  private static final File DEVNULL = new File("/dev/null");

  private static final Range<Integer> RESERVED_PORT_RANGE = Ranges.closed(30000, 32000);

  private Function<AssignedTask, Task> taskFactory;
  private ExecutorService taskExecutor;
  private Driver driver;
  private Task task1;
  private Task task2;
  private ExceptionalClosure<KillCommand, KillException> processKiller;
  private FileDeleter fileDeleter;
  private ExecutorCore executor;

  @Before
  public void setUp() {
    taskFactory = createMock(new Clazz<Function<AssignedTask,Task>>() {});
    taskExecutor = createMock(ExecutorService.class);
    driver = createMock(Driver.class);
    task1 = createMock(Task.class);
    task2 = createMock(Task.class);
    processKiller = createMock(new Clazz<ExceptionalClosure<KillCommand, KillException>>() {});
    fileDeleter = createMock(FileDeleter.class);

    executor = new ExecutorCore(
        DEVNULL,
        taskFactory,
        taskExecutor,
        driver,
        processKiller,
        Amount.of(2, Time.MINUTES),
        RESERVED_PORT_RANGE,
        fileDeleter);
  }

  private void expectStaging(AssignedTask assigned, Task task) throws Exception {
    expect(taskFactory.apply(assigned)).andReturn(task);
    stateChange(assigned.getTaskId(), STARTING);
    expect(task.getMutex()).andReturn(task);
    task.stage();
  }

  private void expectStageAndRun(AssignedTask assigned, Task task) throws Exception {
    expectStaging(assigned, task);
    stateChange(assigned.getTaskId(), RUNNING);
    task.run();
  }

  @Test
  public void testRunTask() throws Exception {
    AssignedTask task = makeTask(OWNER_A, JOB_A);
    expectStageAndRun(task, task1);

    expect(task1.blockUntilTerminated()).andReturn(new AuditedStatus(FINISHED));
    Capture<Runnable> taskCapture = new Capture<Runnable>();
    taskExecutor.execute(capture(taskCapture));
    stateChange(task.getTaskId(), FINISHED);

    control.replay();

    executor.executeTask(task);
    taskCapture.getValue().run();
  }

  @Test
  public void testTaskFails() throws Exception {
    String msg = "Bad stuff!";
    AssignedTask task = makeTask(OWNER_A, JOB_A);

    expectStageAndRun(task, task1);
    expect(task1.blockUntilTerminated()).andReturn(new AuditedStatus(FAILED, msg));
    Capture<Runnable> taskCapture = new Capture<Runnable>();
    taskExecutor.execute(capture(taskCapture));
    stateChange(task.getTaskId(), FAILED, Optional.of(msg));

    control.replay();

    executor.executeTask(task);
    taskCapture.getValue().run();
  }

  @Test
  public void testStagingFails() throws Exception {
    AssignedTask task = makeTask(OWNER_A, JOB_A);

    expectStaging(task, task1);
    expectLastCall().andThrow(new TaskRunException("Staging failed."));
    stateChange(task.getTaskId(), FAILED, "Staging failed.");
    task1.terminate(new AuditedStatus(FAILED));

    control.replay();

    executor.executeTask(task);
  }

  @Test
  public void testRunFails() throws Exception {
    AssignedTask task = makeTask(OWNER_A, JOB_A);

    expectStaging(task, task1);
    task1.run();
    expectLastCall().andThrow(new TaskRunException("Failed to start."));
    stateChange(task.getTaskId(), FAILED, "Failed to start.");
    task1.terminate(new AuditedStatus(FAILED));

    control.replay();

    executor.executeTask(task);
  }

  @Test
  public void testDeleteActiveTask() throws Exception {
    AssignedTask task = makeTask(OWNER_A, JOB_A);

    expectStageAndRun(task, task1);
    expectGetStatus(task1, RUNNING);
    expect(task1.blockUntilTerminated()).andReturn(new AuditedStatus(FINISHED));
    Capture<Runnable> taskCapture = new Capture<Runnable>();
    taskExecutor.execute(capture(taskCapture));
    stateChange(task.getTaskId(), FINISHED);

    control.replay();

    executor.executeTask(task);

    executor.deleteCompletedTasks(ImmutableSet.of(task.getTaskId()));
    taskCapture.getValue().run();
  }

  private AssignedTask setupRunningTask(Task task) throws Exception {
    AssignedTask running = makeTask(OWNER_A, JOB_A);
    expectStageAndRun(running, task);
    taskExecutor.execute(EasyMock.<Runnable>anyObject());
    return running;
  }

  private void expectGetStatus(Task task, AuditedStatus returnedStatsus) {
    expect(task.getAuditedStatus()).andReturn(returnedStatsus).anyTimes();
  }

  private void expectGetStatus(Task task, ScheduleStatus returnedStatus) {
    expectGetStatus(task, new AuditedStatus(returnedStatus));
  }

  private void expectDelete(Task task) throws Exception {
    expect(task.getSandboxDir()).andReturn(DEVNULL);
    fileDeleter.execute(DEVNULL);
  }

  @Test
  public void testRetainRunningTask() throws Exception {
    AssignedTask running = setupRunningTask(task1);
    expectGetStatus(task1, RUNNING);

    AssignedTask killed = setupRunningTask(task2);
    expectGetStatus(task2, KILLED);
    expectDelete(task2);

    control.replay();

    executor.executeTask(killed);
    executor.executeTask(running);

    executor.adjustRetainedTasks(ImmutableMap.of(running.getTaskId(), ScheduleStatus.RUNNING));
    assertNotNull(executor.getTask(running.getTaskId()));
    assertNull(executor.getTask(killed.getTaskId()));
  }

  @Test
  public void testRetainRunningRemoteStarting() throws Exception {
    AssignedTask running = setupRunningTask(task1);
    expectGetStatus(task1, RUNNING);

    control.replay();

    executor.executeTask(running);

    executor.adjustRetainedTasks(ImmutableMap.of(running.getTaskId(), STARTING));
    assertNotNull(executor.getTask(running.getTaskId()));
  }

  @Test
  public void testRetainLocallyRunningMismatch() throws Exception {
    AssignedTask running = setupRunningTask(task1);
    expectGetStatus(task1, RUNNING);
    task1.terminate(new AuditedStatus(KILLED));

    control.replay();

    executor.executeTask(running);

    executor.adjustRetainedTasks(ImmutableMap.of(running.getTaskId(), FINISHED));
    assertNotNull(executor.getTask(running.getTaskId()));
  }

  @Test
  public void testRetainLocallyDeadMismatch() throws Exception {
    AssignedTask running = setupRunningTask(task1);
    expectGetStatus(task1, FAILED);
    stateChange(running.getTaskId(), FAILED, Optional.of(ExecutorCore.REPLAY_STATUS_MSG));

    control.replay();

    executor.executeTask(running);

    executor.adjustRetainedTasks(ImmutableMap.of(running.getTaskId(), RUNNING));
    assertNotNull(executor.getTask(running.getTaskId()));
  }

  @Test
  public void testRetainKilledTask() throws Exception {
    AssignedTask killed = setupRunningTask(task2);
    expectGetStatus(task2, KILLED);

    AssignedTask running = setupRunningTask(task1);
    expectGetStatus(task1, RUNNING);
    task1.terminate(new AuditedStatus(KILLED));
    expectDelete(task1);

    control.replay();

    executor.executeTask(killed);
    executor.executeTask(running);

    executor.adjustRetainedTasks(ImmutableMap.of(killed.getTaskId(), KILLED));
    assertNotNull(executor.getTask(killed.getTaskId()));
    assertNull(executor.getTask(running.getTaskId()));
  }

  @Test
  public void testRetainNoTasks() throws Exception {
    AssignedTask killed = setupRunningTask(task1);
    expectGetStatus(task1, KILLED);
    expectDelete(task1);

    AssignedTask running = setupRunningTask(task2);
    expectGetStatus(task2, RUNNING);
    task2.terminate(new AuditedStatus(KILLED));
    expectDelete(task2);

    control.replay();

    executor.executeTask(killed);
    executor.executeTask(running);

    executor.adjustRetainedTasks(ImmutableMap.<String, ScheduleStatus>of());
    assertNull(executor.getTask(killed.getTaskId()));
    assertNull(executor.getTask(running.getTaskId()));
  }

  @Test
  public void testRetainAllTasks() throws Exception {
    AssignedTask killed = setupRunningTask(task1);
    expectGetStatus(task1, KILLED);

    AssignedTask running = setupRunningTask(task2);
    expectGetStatus(task2, RUNNING);

    control.replay();

    executor.executeTask(killed);
    executor.executeTask(running);

    executor.adjustRetainedTasks(
        ImmutableMap.of(running.getTaskId(), RUNNING, killed.getTaskId(), KILLED));
    assertNotNull(executor.getTask(killed.getTaskId()));
    assertNotNull(executor.getTask(running.getTaskId()));
  }

  @Test
  public void testScanNoProcesses() throws Exception {
    control.replay();

    executor.checkProcesses(ImmutableSet.<ProcessInfo>of());
  }

  @Test
  public void testScanNoBadProcesses() throws Exception {
    AssignedTask running = setupRunningTask(task1);
    expect(task1.isRunning()).andReturn(true);
    expect(task1.getAssignedTask())
        .andReturn(new AssignedTask().setAssignedPorts(ImmutableMap.<String, Integer>of()));

    control.replay();

    executor.executeTask(running);

    executor.checkProcesses(ImmutableSet.of(
        new ProcessInfo(1, running.getTaskId(), ImmutableSet.<Integer>of())));
  }

  @Test
  public void testScanUnrecognizedProcess() throws Exception {
    processKiller.execute(new KillCommand(1));
    processKiller.execute(new KillCommand(2));

    control.replay();

    executor.checkProcesses(ImmutableSet.of(
        new ProcessInfo(1, "foo", ImmutableSet.<Integer>of()),
        new ProcessInfo(2, "bar", ImmutableSet.of(5, 6))));
  }

  @Test
  public void testScanNotRunningTask() throws Exception {
    AssignedTask killed = setupRunningTask(task1);
    expectGetStatus(task1, RUNNING);
    task1.terminate(new AuditedStatus(KILLED));
    expect(task1.isRunning()).andReturn(false);

    processKiller.execute(new KillCommand(2));

    control.replay();

    executor.executeTask(killed);
    executor.stopLiveTask(killed.getTaskId(), "Halting for test.");

    executor.checkProcesses(ImmutableSet.of(
        new ProcessInfo(2, killed.getTaskId(), ImmutableSet.of(5, 6))));
  }

  @Test
  public void testScanAllocatedReservedPort() throws Exception {
    int allocatedPort = RESERVED_PORT_RANGE.lowerEndpoint() + 5;

    AssignedTask running = setupRunningTask(task1);
    expect(task1.isRunning()).andReturn(true);
    expect(task1.getAssignedTask()).andReturn(
        new AssignedTask().setAssignedPorts(ImmutableMap.of("http", allocatedPort)));

    control.replay();

    executor.executeTask(running);

    executor.checkProcesses(ImmutableSet.of(
        new ProcessInfo(1, running.getTaskId(), ImmutableSet.of(allocatedPort))));
  }

  @Test
  public void testScanUnallocatedReservedPort() throws Exception {
    int unallocatedPort = RESERVED_PORT_RANGE.lowerEndpoint() + 5;

    AssignedTask running = setupRunningTask(task1);
    expect(task1.isRunning()).andReturn(true);
    expect(task1.getAssignedTask()).andReturn(
        new AssignedTask().setAssignedPorts(ImmutableMap.<String, Integer>of()));

    // Eventually we will probably kill here, but for now we only log a warning.

    control.replay();

    executor.executeTask(running);

    executor.checkProcesses(ImmutableSet.of(
        new ProcessInfo(1, running.getTaskId(), ImmutableSet.of(unallocatedPort))));
  }

  @Test
  public void testScanUnallocatedUnreservedPort() throws Exception {
    int unallocatedUnreservedPort = RESERVED_PORT_RANGE.upperEndpoint() + 5;

    AssignedTask running = setupRunningTask(task1);
    expect(task1.isRunning()).andReturn(true);
    expect(task1.getAssignedTask()).andReturn(
        new AssignedTask().setAssignedPorts(ImmutableMap.<String, Integer>of()));

    control.replay();

    executor.executeTask(running);

    executor.checkProcesses(ImmutableSet.of(
        new ProcessInfo(1, running.getTaskId(), ImmutableSet.of(unallocatedUnreservedPort))));
  }

  private static final AtomicLong taskIdCounter = new AtomicLong();

  private static AssignedTask makeTask(Identity owner, String jobName) {
    TwitterTaskInfo task = new TwitterTaskInfo()
        .setOwner(owner)
        .setJobName(jobName);

    return new AssignedTask()
        .setTaskId("task-" + taskIdCounter.incrementAndGet())
        .setTask(task);
  }

  private void stateChange(String taskId, ScheduleStatus status) {
    stateChange(taskId, status, Optional.<String>absent());
  }

  private void stateChange(String taskId, ScheduleStatus status, String message) {
    stateChange(taskId, status, Optional.of(message));
  }

  private void stateChange(String taskId, ScheduleStatus status, Optional<String> message) {
    expect(driver.sendStatusUpdate(taskId, status, message)).andReturn(0);
  }
}
