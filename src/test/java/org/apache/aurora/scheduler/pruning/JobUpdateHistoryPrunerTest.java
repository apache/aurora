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
package org.apache.aurora.scheduler.pruning;

import java.util.concurrent.ScheduledExecutorService;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.scheduler.pruning.JobUpdateHistoryPruner.HistoryPrunerSettings;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeScheduledExecutor;
import org.junit.Test;

import static org.easymock.EasyMock.expect;

public class JobUpdateHistoryPrunerTest extends EasyMockTest {
  @Test
  public void testExecution() throws Exception {
    StorageTestUtil storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();

    final ScheduledExecutorService executor = createMock(ScheduledExecutorService.class);
    FakeScheduledExecutor executorClock =
        FakeScheduledExecutor.scheduleAtFixedRateExecutor(executor, 2);

    Clock mockClock = createMock(Clock.class);
    expect(mockClock.nowMillis()).andReturn(2L).times(2);

    expect(storageUtil.jobUpdateStore.pruneHistory(1, 1))
        .andReturn(ImmutableSet.of(
            IJobUpdateKey.build(
                new JobUpdateKey().setJob(new JobKey("role", "env", "job")).setId("id1"))));
    expect(storageUtil.jobUpdateStore.pruneHistory(1, 1)).andReturn(ImmutableSet.of());

    control.replay();

    executorClock.assertEmpty();
    JobUpdateHistoryPruner pruner = new JobUpdateHistoryPruner(
        mockClock,
        executor,
        storageUtil.storage,
        new HistoryPrunerSettings(
            Amount.of(1L, Time.MILLISECONDS),
            Amount.of(1L, Time.MILLISECONDS),
            1));

    pruner.startAsync().awaitRunning();
    executorClock.advance(Amount.of(1L, Time.MILLISECONDS));
    executorClock.advance(Amount.of(1L, Time.MILLISECONDS));
  }
}
