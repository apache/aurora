package com.twitter.mesos.scheduler.sync;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.application.ActionRegistry;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.ExecutorKey;
import com.twitter.mesos.gen.comm.StateUpdateRequest;
import com.twitter.mesos.scheduler.sync.ExecutorWatchdog.ExecutorWatchdogImpl;
import com.twitter.mesos.scheduler.sync.ExecutorWatchdog.UpdateRequest;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.SlaveID;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static com.twitter.mesos.scheduler.sync.ExecutorWatchdog.ExecutorWatchdogImpl.NO_POSITION;
import static org.junit.Assert.assertEquals;

/**
 * @author William Farner
 */
public class ExecutorWatchdogImplTest extends EasyMockTest {

  private static final ExecutorKey A = key("a", "a", "a");
  private static final ExecutorKey B = key("b", "b", "b");
  private static final ExecutorKey C = key("c", "c", "c");
  private static final ExecutorKey D = key("d", "d", "d");
  private static final Set<ExecutorKey> EXECUTORS = ImmutableSet.of(A, B, C);

  private Supplier<Set<ExecutorKey>> knownExecutorSupplier;
  private ExecutorWatchdogImpl watchdog;

  @Before
  public void setUp() {
    knownExecutorSupplier = createMock(new Clazz<Supplier<Set<ExecutorKey>>>() {});
    watchdog =
        new ExecutorWatchdogImpl(knownExecutorSupplier, control.createMock(ActionRegistry.class));

    // This is non-standard, but since we don't actually call the mock, it prevents us from
    // needing to repeat this line in every test.
    control.replay();
  }

  @Test
  public void testEmpty() {
    UpdateRequest expected = UpdateRequest.none();
    Set<ExecutorKey> executors = ImmutableSet.of();
    checkOutput(executors, expected.executor, expected.request);
    checkOutput(executors, expected.executor, expected.request);
    checkOutput(executors, expected.executor, expected.request);
  }

  @Test
  public void testCycle() {
    checkOutput(A, NO_POSITION);
    checkOutput(B, NO_POSITION);
    checkOutput(C, NO_POSITION);
    checkOutput(A, NO_POSITION);
    checkOutput(B, NO_POSITION);
    checkOutput(C, NO_POSITION);
  }

  @Test
  public void testNewExecutorAdded() {
    Set<ExecutorKey> withNewExecutor =
        ImmutableSet.<ExecutorKey>builder().addAll(EXECUTORS).add(D).build();

    checkOutput(A, NO_POSITION);
    checkOutput(B, NO_POSITION);
    checkOutput(C, NO_POSITION);
    checkOutput(A, NO_POSITION);
    checkOutput(withNewExecutor, D, NO_POSITION);
    checkOutput(withNewExecutor, B, NO_POSITION);
  }

  @Test
  public void testExecutorRemoved() {
    Set<ExecutorKey> executorRemoved = ImmutableSet.of(A, C);

    checkOutput(A, NO_POSITION);
    checkOutput(B, NO_POSITION);
    checkOutput(executorRemoved, C, NO_POSITION);
    checkOutput(executorRemoved, A, NO_POSITION);

    // Even though we lost track of B, we continue to try to revive it.
    checkOutput(executorRemoved, B, NO_POSITION);
  }

  @Test
  public void testPositionUpdated() {
    checkOutput(A, NO_POSITION);

    watchdog.stateUpdated(A, "foo", 5);

    checkOutput(B, NO_POSITION);
    checkOutput(C, NO_POSITION);

    watchdog.stateUpdated(C, "bar", 2);

    checkOutput(A, new StateUpdateRequest("foo", 5));
    checkOutput(B, NO_POSITION);
    checkOutput(C, new StateUpdateRequest("bar", 2));

    watchdog.stateUpdated(A, "baz", 1);
    checkOutput(B, NO_POSITION);
  }

  @Test
  public void testShuffle() {
    // Available executors: A, B, C
    watchdog.stateUpdated(C, "foo", 4);
    watchdog.stateUpdated(A, "foo", 3);

    // Least recently heard from B.
    checkOutput(B, NO_POSITION);

    watchdog.stateUpdated(C, "foo", 4);

    // B responds.
    watchdog.stateUpdated(B, "foo", 2);

    // Least recently heard from A.
    checkOutput(A, new StateUpdateRequest("foo", 3));

    // Introduce D.
    Set<ExecutorKey> withNewExecutor =
        ImmutableSet.<ExecutorKey>builder().addAll(EXECUTORS).add(D).build();
    checkOutput(withNewExecutor, D, NO_POSITION);

    // Next LRU is C.
    checkOutput(C, new StateUpdateRequest("foo", 4));
  }

  private void checkOutput(ExecutorKey executor, StateUpdateRequest expected) {
    checkOutput(EXECUTORS, executor, expected);
  }

  private void checkOutput(Set<ExecutorKey> executors, ExecutorKey executor,
      StateUpdateRequest expected) {
    UpdateRequest request = watchdog.getNextUpdateRequest(executors);
    assertEquals(executor, request.executor);
    assertEquals(expected, request.request);
  }

  private static ExecutorKey key(String slaveId, String executorId, String hostname) {
    return new ExecutorKey(
        SlaveID.newBuilder().setValue(slaveId).build(),
        ExecutorID.newBuilder().setValue(executorId).build(),
        hostname);
  }
}
