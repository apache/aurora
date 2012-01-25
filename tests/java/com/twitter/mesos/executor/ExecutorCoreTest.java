package com.twitter.mesos.executor;

import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;

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

/**
 * @author William Farner
 */
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
  private Task runningTask;
  private ExceptionalClosure<KillCommand, KillException> processKiller;
  private FileDeleter fileDeleter;
  private ExecutorCore executor;

  @Before
  public void setUp() {
    taskFactory = createMock(new Clazz<Function<AssignedTask,Task>>() {});
    taskExecutor = createMock(ExecutorService.class);
    driver = createMock(Driver.class);
    runningTask = createMock(Task.class);
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

  @Test
  public void testRunTask() throws Exception {
    AssignedTask task = makeTask(OWNER_A, JOB_A);

    expect(taskFactory.apply(task)).andReturn(runningTask);
    stateChange(task.getTaskId(), STARTING);
    runningTask.stage();
    stateChange(task.getTaskId(), RUNNING);
    runningTask.run();
    expect(runningTask.blockUntilTerminated()).andReturn(FINISHED);
    Capture<Runnable> taskCapture = new Capture<Runnable>();
    taskExecutor.execute(capture(taskCapture));
    stateChange(task.getTaskId(), FINISHED);

    control.replay();

    executor.executeTask(task);
    taskCapture.getValue().run();
  }

  @Test
  public void testTaskFails() throws Exception {
    AssignedTask task = makeTask(OWNER_A, JOB_A);

    expect(taskFactory.apply(task)).andReturn(runningTask);
    stateChange(task.getTaskId(), STARTING);
    runningTask.stage();
    stateChange(task.getTaskId(), RUNNING);
    runningTask.run();
    expect(runningTask.blockUntilTerminated()).andReturn(FAILED);
    Capture<Runnable> taskCapture = new Capture<Runnable>();
    taskExecutor.execute(capture(taskCapture));
    stateChange(task.getTaskId(), FAILED);

    control.replay();

    executor.executeTask(task);
    taskCapture.getValue().run();
  }

  @Test
  public void testStagingFails() throws Exception {
    AssignedTask task = makeTask(OWNER_A, JOB_A);

    expect(taskFactory.apply(task)).andReturn(runningTask);
    stateChange(task.getTaskId(), STARTING);
    runningTask.stage();
    expectLastCall().andThrow(new TaskRunException("Staging failed."));
    stateChange(task.getTaskId(), FAILED, "Staging failed.");
    runningTask.terminate(FAILED);

    control.replay();

    executor.executeTask(task);
  }

  @Test
  public void testRunFails() throws Exception {
    AssignedTask task = makeTask(OWNER_A, JOB_A);

    expect(taskFactory.apply(task)).andReturn(runningTask);
    stateChange(task.getTaskId(), STARTING);
    runningTask.stage();
    stateChange(task.getTaskId(), RUNNING);
    runningTask.run();
    expectLastCall().andThrow(new TaskRunException("Failed to start."));
    stateChange(task.getTaskId(), FAILED, "Failed to start.");
    runningTask.terminate(FAILED);

    control.replay();

    executor.executeTask(task);
  }

  @Test
  public void testDeleteActiveTask() throws Exception {
    AssignedTask task = makeTask(OWNER_A, JOB_A);

    expect(taskFactory.apply(task)).andReturn(runningTask);
    stateChange(task.getTaskId(), STARTING);
    runningTask.stage();
    stateChange(task.getTaskId(), RUNNING);
    runningTask.run();
    expect(runningTask.isRunning()).andReturn(true);
    expect(runningTask.blockUntilTerminated()).andReturn(FINISHED);
    Capture<Runnable> taskCapture = new Capture<Runnable>();
    taskExecutor.execute(capture(taskCapture));
    stateChange(task.getTaskId(), FINISHED);

    control.replay();

    executor.executeTask(task);

    executor.deleteCompletedTasks(ImmutableSet.of(task.getTaskId()));
    taskCapture.getValue().run();
  }

  private AssignedTask setupRunningTask() throws Exception {
    AssignedTask running = makeTask(OWNER_A, JOB_A);
    expect(taskFactory.apply(running)).andReturn(runningTask);
    stateChange(running.getTaskId(), STARTING);
    runningTask.stage();
    stateChange(running.getTaskId(), RUNNING);
    runningTask.run();
    taskExecutor.execute(EasyMock.<Runnable>anyObject());
    return running;
  }

  private AssignedTask setupKilledTask() throws Exception {
    AssignedTask killed = setupRunningTask();
    executor.stopLiveTask(killed.getTaskId());
    return killed;
  }

  @Test
  public void testRetainRunningTask() throws Exception {
    AssignedTask killed = setupKilledTask();
    expect(runningTask.isRunning()).andReturn(false);
    expect(runningTask.getSandboxDir()).andReturn(DEVNULL);
    fileDeleter.execute(DEVNULL);
    AssignedTask running = setupRunningTask();

    control.replay();

    executor.executeTask(killed);
    executor.executeTask(running);

    executor.adjustRetainedTasks(ImmutableSet.of(running.getTaskId()));
    assertNotNull(executor.getTask(running.getTaskId()));
    assertNull(executor.getTask(killed.getTaskId()));
  }

  @Test
  public void testRetainKilledTask() throws Exception {
    AssignedTask killed = setupKilledTask();
    AssignedTask running = setupRunningTask();
    expect(runningTask.isRunning()).andReturn(true);
    expect(runningTask.getSandboxDir()).andReturn(DEVNULL);
    runningTask.terminate(KILLED);
    fileDeleter.execute(DEVNULL);

    control.replay();

    executor.executeTask(killed);
    executor.executeTask(running);

    executor.adjustRetainedTasks(ImmutableSet.of(killed.getTaskId()));
    assertNotNull(executor.getTask(killed.getTaskId()));
    assertNull(executor.getTask(running.getTaskId()));
  }

  @Test
  public void testRetainNoTasks() throws Exception {
    AssignedTask killed = setupKilledTask();
    expect(runningTask.isRunning()).andReturn(false);
    expect(runningTask.getSandboxDir()).andReturn(DEVNULL);
    fileDeleter.execute(DEVNULL);
    AssignedTask running = setupRunningTask();
    expect(runningTask.isRunning()).andReturn(true);
    expect(runningTask.getSandboxDir()).andReturn(DEVNULL);
    runningTask.terminate(KILLED);
    fileDeleter.execute(DEVNULL);

    control.replay();

    executor.executeTask(killed);
    executor.executeTask(running);

    executor.adjustRetainedTasks(ImmutableSet.<String>of());
    assertNull(executor.getTask(killed.getTaskId()));
    assertNull(executor.getTask(running.getTaskId()));
  }

  @Test
  public void testRetainAllTasks() throws Exception {
    AssignedTask finished = setupKilledTask();
    AssignedTask running = setupRunningTask();

    control.replay();

    executor.executeTask(finished);
    executor.executeTask(running);

    executor.adjustRetainedTasks(ImmutableSet.of(running.getTaskId(), finished.getTaskId()));
    assertNotNull(executor.getTask(finished.getTaskId()));
    assertNotNull(executor.getTask(running.getTaskId()));
  }

  @Test
  public void testScanNoProcesses() throws Exception {
    control.replay();

    executor.checkProcesses(ImmutableSet.<ProcessInfo>of());
  }

  @Test
  public void testScanNoBadProcesses() throws Exception {
    AssignedTask running = setupRunningTask();
    expect(runningTask.isRunning()).andReturn(true);
    expect(runningTask.getAssignedTask())
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
    AssignedTask finished = setupKilledTask();
    expect(runningTask.isRunning()).andReturn(false);

    processKiller.execute(new KillCommand(2));

    control.replay();

    executor.executeTask(finished);

    executor.checkProcesses(ImmutableSet.of(
        new ProcessInfo(2, finished.getTaskId(), ImmutableSet.of(5, 6))));
  }

  @Test
  public void testScanAllocatedReservedPort() throws Exception {
    int allocatedPort = RESERVED_PORT_RANGE.lowerEndpoint() + 5;

    AssignedTask running = setupRunningTask();
    expect(runningTask.isRunning()).andReturn(true);
    expect(runningTask.getAssignedTask()).andReturn(
        new AssignedTask().setAssignedPorts(ImmutableMap.of("http", allocatedPort)));

    control.replay();

    executor.executeTask(running);

    executor.checkProcesses(ImmutableSet.of(
        new ProcessInfo(1, running.getTaskId(), ImmutableSet.of(allocatedPort))));
  }

  @Test
  public void testScanUnallocatedReservedPort() throws Exception {
    int unallocatedPort = RESERVED_PORT_RANGE.lowerEndpoint() + 5;

    AssignedTask running = setupRunningTask();
    expect(runningTask.isRunning()).andReturn(true);
    expect(runningTask.getAssignedTask()).andReturn(
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

    AssignedTask running = setupRunningTask();
    expect(runningTask.isRunning()).andReturn(true);
    expect(runningTask.getAssignedTask()).andReturn(
        new AssignedTask().setAssignedPorts(ImmutableMap.<String, Integer>of()));

    control.replay();

    executor.executeTask(running);

    executor.checkProcesses(ImmutableSet.of(
        new ProcessInfo(1, running.getTaskId(), ImmutableSet.of(unallocatedUnreservedPort))));
  }

  private static AssignedTask makeTask(Identity owner, String jobName) {
    TwitterTaskInfo task = new TwitterTaskInfo()
        .setOwner(owner)
        .setJobName(jobName);

    return new AssignedTask()
        .setTaskId(String.valueOf(new Random().nextInt(10000)))
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
