/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;

import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.TaskStatusHandlerImpl.statName;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TaskStatusHandlerImplTest extends EasyMockTest {

  private static final String TASK_ID_A = "task_id_a";

  private StateManager stateManager;
  private StorageTestUtil storageUtil;
  private Driver driver;
  private BlockingQueue<TaskStatus> queue;
  private FakeStatsProvider stats;

  private TaskStatusHandlerImpl statusHandler;

  @Before
  public void setUp() {
    stateManager = createMock(StateManager.class);
    storageUtil = new StorageTestUtil(this);
    driver = createMock(Driver.class);
    queue = new LinkedBlockingQueue<>();
    stats = new FakeStatsProvider();

    statusHandler = new TaskStatusHandlerImpl(
        storageUtil.storage,
        stateManager,
        driver,
        queue,
        1000,
        new CachedCounters(stats));

    statusHandler.startAsync();
  }

  @After
  public void after() {
    statusHandler.stopAsync();
  }

  @Test
  public void testForwardsStatusUpdates() throws Exception {
    TaskStatus status = TaskStatus.newBuilder()
        .setState(TaskState.TASK_RUNNING)
        .setReason(TaskStatus.Reason.REASON_RECONCILIATION)
        .setTaskId(TaskID.newBuilder().setValue(TASK_ID_A))
        .setMessage("fake message")
        .build();

    storageUtil.expectWrite();

    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID_A,
        Optional.<ScheduleStatus>absent(),
        RUNNING,
        Optional.of("fake message")))
        .andReturn(StateChangeResult.SUCCESS);

    final CountDownLatch latch = new CountDownLatch(1);

    driver.acknowledgeStatusUpdate(status);
    expectLastCall().andAnswer(() -> {
        latch.countDown();
        return null;
      });

    control.replay();

    statusHandler.statusUpdate(status);
    assertTrue(latch.await(5L, TimeUnit.SECONDS));
    assertEquals(1L, stats.getValue(statName(status, StateChangeResult.SUCCESS)));
  }

  @Test
  public void testFailedStatusUpdate() throws Exception {
    storageUtil.expectWrite();

    final CountDownLatch latch = new CountDownLatch(1);

    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID_A,
        Optional.<ScheduleStatus>absent(),
        RUNNING,
        Optional.of("fake message")))
        .andAnswer(() -> {
            latch.countDown();
            throw new StorageException("Injected error");
          });

    control.replay();

    TaskStatus status = TaskStatus.newBuilder()
        .setState(TaskState.TASK_RUNNING)
        .setTaskId(TaskID.newBuilder().setValue(TASK_ID_A))
        .setMessage("fake message")
        .build();

    statusHandler.statusUpdate(status);

    assertTrue(latch.await(5L, TimeUnit.SECONDS));
  }

  @Test
  public void testMemoryLimitTranslation() throws Exception {
    storageUtil.expectWrite();

    TaskStatus status = TaskStatus.newBuilder()
        .setState(TaskState.TASK_FAILED)
        .setTaskId(TaskID.newBuilder().setValue(TASK_ID_A))
        .setReason(TaskStatus.Reason.REASON_MEMORY_LIMIT)
        .setMessage("Some Message")
        .build();

    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID_A,
        Optional.absent(),
        FAILED,
        Optional.of(TaskStatusHandlerImpl.MEMORY_LIMIT_DISPLAY)))
        .andReturn(StateChangeResult.SUCCESS);

    final CountDownLatch latch = new CountDownLatch(1);

    driver.acknowledgeStatusUpdate(status);
    expectLastCall().andAnswer(() -> {
        latch.countDown();
        return null;
      });

    control.replay();

    statusHandler.statusUpdate(status);

    assertTrue(latch.await(5L, TimeUnit.SECONDS));
  }

  @Test
  public void testThreadFailure() throws Exception {
    // Re-create the objects from @Before, since we need to inject a mock queue.
    statusHandler.stopAsync();
    statusHandler.awaitTerminated();

    stateManager = createMock(StateManager.class);
    storageUtil = new StorageTestUtil(this);
    driver = createMock(Driver.class);
    queue = createMock(BlockingQueue.class);

    statusHandler = new TaskStatusHandlerImpl(
        storageUtil.storage,
        stateManager,
        driver,
        queue,
        1000,
        new CachedCounters(stats));

    expect(queue.add(EasyMock.<TaskStatus>anyObject()))
        .andReturn(true);

    expect(queue.take())
        .andAnswer(() -> {
            throw new RuntimeException();
          });

    final CountDownLatch latch = new CountDownLatch(1);

    driver.abort();
    expectLastCall().andAnswer(() -> {
        latch.countDown();
        return null;
      });

    control.replay();

    statusHandler.startAsync();

    TaskStatus status = TaskStatus.newBuilder()
        .setState(TaskState.TASK_RUNNING)
        .setTaskId(TaskID.newBuilder().setValue(TASK_ID_A))
        .setMessage("fake message")
        .build();

    statusHandler.statusUpdate(status);

    assertTrue(latch.await(5L, TimeUnit.SECONDS));
  }
}
