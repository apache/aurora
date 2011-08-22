package com.twitter.mesos.executor.sync;

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.executor.sync.SyncBuffer.SyncBufferImpl;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.comm.StateUpdateResponse;
import com.twitter.mesos.gen.comm.TaskStateUpdate;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author William Farner
 */
public class SyncBufferImplTest extends EasyMockTest {

  private static final int MAX_SIZE = 4;

  private static final Map<String, ScheduleStatus> EMPTY_STATUSES = ImmutableMap.of();
  private static final Map<String, TaskStateUpdate> EMPTY_STATE_UPDATE = ImmutableMap.of();

  private Supplier<Map<String, ScheduleStatus>> fullStateAccessor;
  private SyncBuffer buffer;

  @Before
  public void setUp() {
    fullStateAccessor = createMock(new Clazz<Supplier<Map<String, ScheduleStatus>>>() { });
    buffer = new SyncBufferImpl(MAX_SIZE, fullStateAccessor);
  }

  @Test
  public void testEmptyBuffer() {
    expect(fullStateAccessor.get()).andReturn(EMPTY_STATUSES);

    control.replay();

    StateUpdateResponse response = getInitialFullUpdate(EMPTY_STATE_UPDATE);
    String bufferId = response.getExecutorUUID();
    int position = response.getPosition();

    response = getIncrementalUpdate(bufferId, position, EMPTY_STATE_UPDATE);
    assertEquals(bufferId, response.getExecutorUUID());
  }

  @Test
  public void testSizeLimit() throws Exception {
    expect(fullStateAccessor.get()).andReturn(EMPTY_STATUSES);
    expect(fullStateAccessor.get()).andReturn(ImmutableMap.of(
        "2", ScheduleStatus.PENDING,
        "3", ScheduleStatus.PENDING,
        "4", ScheduleStatus.PENDING,
        "5", ScheduleStatus.PENDING
    ));

    control.replay();
    TaskStateUpdate update = update(ScheduleStatus.PENDING);

    StateUpdateResponse response = getInitialFullUpdate(EMPTY_STATE_UPDATE);
    String bufferId = response.getExecutorUUID();
    int position = response.getPosition();

    buffer.add("1", update);

    // Deliberately do not update position.
    getIncrementalUpdate(bufferId, position, ImmutableMap.of("1", update));

    // Assumes MAX_SIZE == 4.
    buffer.add("2", update);
    buffer.add("3", update);
    buffer.add("4", update);
    buffer.add("5", update);

    // We ask for state since the same position again, but the position has since rolled off
    // the buffer.  We expect a full update in response.
    getFullUpdate(bufferId, position, ImmutableMap.of(
        "2", update(ScheduleStatus.PENDING),
        "3", update(ScheduleStatus.PENDING),
        "4", update(ScheduleStatus.PENDING),
        "5", update(ScheduleStatus.PENDING)));
  }

  @Test
  public void testLatestRecordWins() {
    expect(fullStateAccessor.get()).andReturn(EMPTY_STATUSES);

    control.replay();

    StateUpdateResponse response = getInitialFullUpdate(EMPTY_STATE_UPDATE);
    String bufferId = response.getExecutorUUID();
    int position = response.getPosition();

    buffer.add("1", update(ScheduleStatus.PENDING));
    buffer.add("1", update(ScheduleStatus.STARTING));
    buffer.add("1", update(ScheduleStatus.RUNNING));

    getIncrementalUpdate(bufferId, position, ImmutableMap.of("1", update(ScheduleStatus.RUNNING)));
  }

  @Test
  public void testBadPosition() {
    expect(fullStateAccessor.get()).andReturn(ImmutableMap.of(
        "1", ScheduleStatus.RUNNING,
        "2", ScheduleStatus.STARTING
    ));
    expect(fullStateAccessor.get()).andReturn(ImmutableMap.of(
        "2", ScheduleStatus.RUNNING
    ));

    control.replay();

    StateUpdateResponse response = getInitialFullUpdate(ImmutableMap.of(
        "1", update(ScheduleStatus.RUNNING),
        "2", update(ScheduleStatus.STARTING)));
    String bufferId = response.getExecutorUUID();

    buffer.add("1", update(ScheduleStatus.FINISHED));
    buffer.add("2", update(ScheduleStatus.RUNNING));
    buffer.add("1", deleteUpdate());

    response = getFullUpdate(bufferId, Integer.MIN_VALUE,
        ImmutableMap.of("2", update(ScheduleStatus.RUNNING)));
    int position = response.getPosition();

    buffer.add("2", update(ScheduleStatus.FAILED));
    buffer.add("2", deleteUpdate());

    response = getIncrementalUpdate(bufferId, position,
        ImmutableMap.of("2", deleteUpdate()));
    position = response.getPosition();

    getIncrementalUpdate(bufferId, position, EMPTY_STATE_UPDATE);
  }

  @Test
  public void testBufferReset() {
    expect(fullStateAccessor.get()).andReturn(EMPTY_STATUSES).times(2);

    control.replay();

    StateUpdateResponse response = getInitialFullUpdate(EMPTY_STATE_UPDATE);
    String bufferId = response.getExecutorUUID();
    int position = response.getPosition();

    buffer.add("1", update(ScheduleStatus.FINISHED));
    buffer.add("2", update(ScheduleStatus.RUNNING));
    buffer.add("1", deleteUpdate());

    response = getIncrementalUpdate(bufferId, position,
        ImmutableMap.of("1", deleteUpdate(), "2", update(ScheduleStatus.RUNNING)));
    position = response.getPosition();

    buffer.add("2", update(ScheduleStatus.FAILED));
    buffer.add("2", deleteUpdate());

    buffer = new SyncBufferImpl(MAX_SIZE, fullStateAccessor);

    response = getFullUpdate(bufferId, position, EMPTY_STATE_UPDATE);
    bufferId = response.getExecutorUUID();
    position = response.getPosition();

    getIncrementalUpdate(bufferId, position, EMPTY_STATE_UPDATE);
  }

  @Test
  public void testRepeatedStatRequest() {
    expect(fullStateAccessor.get()).andReturn(EMPTY_STATUSES);

    control.replay();

    StateUpdateResponse response = getInitialFullUpdate(EMPTY_STATE_UPDATE);
    String bufferId = response.getExecutorUUID();
    int position = response.getPosition();

    buffer.add("1", update(ScheduleStatus.FINISHED));
    buffer.add("2", update(ScheduleStatus.RUNNING));
    buffer.add("1", deleteUpdate());

    response = getIncrementalUpdate(bufferId, position,
        ImmutableMap.of("1", deleteUpdate(), "2", update(ScheduleStatus.RUNNING)));
    position = response.getPosition();

    getIncrementalUpdate(bufferId, position, EMPTY_STATE_UPDATE);
    getIncrementalUpdate(bufferId, position, EMPTY_STATE_UPDATE);

    buffer.add("2", update(ScheduleStatus.FINISHED));

    getIncrementalUpdate(bufferId, position, ImmutableMap.of("2", update(ScheduleStatus.FINISHED)));
  }

  private StateUpdateResponse getIncrementalUpdate(String bufferId, int sincePosition,
      Map<String, TaskStateUpdate> expectedUpdate) {
    return getStateUpdate(bufferId, sincePosition, expectedUpdate, true);
  }

  private StateUpdateResponse getInitialFullUpdate(Map<String, TaskStateUpdate> expectedUpdate) {
    return getFullUpdate(null, 0, expectedUpdate);
  }

  private StateUpdateResponse getFullUpdate(@Nullable String bufferId, int sincePosition,
      Map<String, TaskStateUpdate> expectedUpdate) {
    return getStateUpdate(bufferId, sincePosition, expectedUpdate, false);
  }

  private StateUpdateResponse getStateUpdate(@Nullable String bufferId, int sincePosition,
      Map<String, TaskStateUpdate> expectedUpdate, boolean expectedIncremental) {
    StateUpdateResponse response = buffer.stateSince(bufferId, sincePosition);
    if (expectedIncremental) {
      assertTrue("Should have been an incremental update", response.isIncrementalUpdate());
    } else {
      assertFalse("Should have been a full update", response.isIncrementalUpdate());
    }

    assertEquals(expectedUpdate, response.getState());
    return response;
  }

  private TaskStateUpdate update(ScheduleStatus status) {
    return new TaskStateUpdate().setStatus(status);
  }

  private TaskStateUpdate deleteUpdate() {
    return new TaskStateUpdate().setDeleted(true);
  }
}
