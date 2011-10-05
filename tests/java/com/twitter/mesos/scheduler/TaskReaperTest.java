package com.twitter.mesos.scheduler;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.twitter.mesos.ExecutorKey;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.SlaveID;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

/**
 * @author William Farner
 */
public class TaskReaperTest extends BaseStateManagerTest {

  private static final String HOST_A = "host_a";
  private static final String HOST_B = "host_b";
  private static final String HOST_C = "host_c";

  private static final ExecutorKey EXEC_A = makeExecutorKey(HOST_A);
  private static final ExecutorKey EXEC_B = makeExecutorKey(HOST_B);
  private static final ExecutorKey EXEC_C = makeExecutorKey(HOST_C);

  private Supplier<Set<ExecutorKey>> knownExecutorSupplier;

  private TaskReaper reaper;

  @Before
  public void setUp() throws Exception {
    knownExecutorSupplier = createMock(new Clazz<Supplier<Set<ExecutorKey>>>() {});
    reaper = new TaskReaper(stateManager, knownExecutorSupplier);
  }

  @Test
  public void testNoTasks() {
    expect(knownExecutorSupplier.get()).andReturn(ImmutableSet.<ExecutorKey>of());

    control.replay();

    assertEquals(ImmutableSet.<String>of(), reaper.getAbandonedTasks(stateManager));
  }

  @Test
  public void testExecutorsAliveNoTasks() {
    expect(knownExecutorSupplier.get()).andReturn(ImmutableSet.of(EXEC_A, EXEC_B, EXEC_C));

    control.replay();

    assertEquals(ImmutableSet.<String>of(), reaper.getAbandonedTasks(stateManager));
  }

  @Test
  public void testTasksAndExecutors() {
    expect(knownExecutorSupplier.get()).andReturn(ImmutableSet.of(EXEC_A, EXEC_B, EXEC_C));

    Set<String> taskIds = stateManager.insertTasks(ImmutableSet.of(
        makeTask("jim", "myJob", 0),
        makeTask("jack", "otherJob", 0)
    ));
    String task1 = Iterables.get(taskIds, 0);
    String task2 = Iterables.get(taskIds, 1);
    assignTask(task1, HOST_A);
    assignTask(task2, HOST_B);

    control.replay();

    assertEquals(ImmutableSet.<String>of(), reaper.getAbandonedTasks(stateManager));
  }

  @Test
  public void testDeadExecutor() {
    expect(knownExecutorSupplier.get()).andReturn(ImmutableSet.of(EXEC_A, EXEC_B, EXEC_C));
    expect(knownExecutorSupplier.get()).andReturn(ImmutableSet.of(EXEC_B, EXEC_C));

    Set<String> taskIds = stateManager.insertTasks(ImmutableSet.of(
        makeTask("jim", "myJob", 0),
        makeTask("jack", "otherJob", 0)
    ));
    String task1 = Iterables.get(taskIds, 0);
    String task2 = Iterables.get(taskIds, 1);
    assignTask(task1, HOST_A);
    assignTask(task2, HOST_B);

    control.replay();

    assertEquals(ImmutableSet.<String>of(), reaper.getAbandonedTasks(stateManager));
    assertEquals(ImmutableSet.of(task1), reaper.getAbandonedTasks(stateManager));
  }

  private static ExecutorKey makeExecutorKey(String hostname) {
    return new ExecutorKey(
        SlaveID.newBuilder().setValue("slave_id_" + hostname).build(),
        ExecutorID.newBuilder().setValue("executor_id_" + hostname).build(),
        hostname);
  }
}
