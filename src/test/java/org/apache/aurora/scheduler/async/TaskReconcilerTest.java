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
package org.apache.aurora.scheduler.async;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableSet;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeScheduledExecutor;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.async.TaskReconciler.EXPLICIT_STAT_NAME;
import static org.apache.aurora.scheduler.async.TaskReconciler.IMPLICIT_STAT_NAME;
import static org.apache.aurora.scheduler.async.TaskReconciler.TASK_TO_PROTO;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

public class TaskReconcilerTest extends EasyMockTest {
  private StorageTestUtil storageUtil;
  private StatsProvider statsProvider;
  private Driver driver;
  private ScheduledExecutorService executorService;
  private FakeScheduledExecutor clock;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    statsProvider = createMock(StatsProvider.class);
    driver = createMock(Driver.class);
    executorService = createMock(ScheduledExecutorService.class);
    clock = FakeScheduledExecutor.scheduleAtFixedRateExecutor(executorService, 2, 5);
  }

  @Test
  public void testExecution() {
    AtomicLong explicitRuns = new AtomicLong();
    AtomicLong implicitRuns = new AtomicLong();
    expect(statsProvider.makeCounter(EXPLICIT_STAT_NAME)).andReturn(explicitRuns);
    expect(statsProvider.makeCounter(IMPLICIT_STAT_NAME)).andReturn(implicitRuns);

    IScheduledTask task = TaskTestUtil.makeTask("id1", TaskTestUtil.JOB);
    storageUtil.expectOperations();
    storageUtil.expectTaskFetch(Query.unscoped().active(), task).times(5);

    driver.reconcileTasks(ImmutableSet.of(TASK_TO_PROTO.apply(task)));
    expectLastCall().times(5);

    driver.reconcileTasks(ImmutableSet.of());
    expectLastCall().times(2);

    control.replay();

    Amount<Long, Time> initialDelay = Amount.of(10L, Time.MINUTES);
    Amount<Long, Time> explicitSchedule = Amount.of(60L, Time.MINUTES);
    Amount<Long, Time> implicitSchedule = Amount.of(180L, Time.MINUTES);
    Amount<Long, Time> spread = Amount.of(30L, Time.MINUTES);

    TaskReconciler reconciler = new TaskReconciler(
        new TaskReconciler.TaskReconcilerSettings(
            initialDelay,
            explicitSchedule,
            implicitSchedule,
            spread),
        storageUtil.storage,
        driver,
        executorService,
        statsProvider);

    reconciler.startAsync().awaitRunning();

    clock.advance(initialDelay);
    assertEquals(1L, explicitRuns.get());
    assertEquals(0L, implicitRuns.get());

    clock.advance(spread);
    assertEquals(1L, explicitRuns.get());
    assertEquals(1L, implicitRuns.get());

    clock.advance(explicitSchedule);
    assertEquals(2L, explicitRuns.get());
    assertEquals(1L, implicitRuns.get());

    clock.advance(implicitSchedule);
    assertEquals(5L, explicitRuns.get());
    assertEquals(2L, implicitRuns.get());
  }
}
