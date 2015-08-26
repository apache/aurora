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
package org.apache.aurora.scheduler.mesos;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.stats.StatsProvider.RequestTimer;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStatusReceived;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus.Reason;
import org.apache.mesos.Protos.TaskStatus.Source;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.mesos.TaskStatusStats.latencyTimerName;
import static org.apache.aurora.scheduler.mesos.TaskStatusStats.lostCounterName;
import static org.apache.aurora.scheduler.mesos.TaskStatusStats.reasonCounterName;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

public class TaskStatusStatsTest extends EasyMockTest {

  private static final Amount<Long, Time> ONE_SECOND = Amount.of(1L, Time.SECONDS);

  private StatsProvider statsProvider;
  private FakeClock clock;
  private EventBus eventBus;

  @Before
  public void setUp() {
    statsProvider = createMock(StatsProvider.class);
    clock = new FakeClock();
    eventBus = new EventBus();
    eventBus.register(new TaskStatusStats(statsProvider, clock));
  }

  private long agoMicros(Amount<Long, Time> duration) {
    return clock.nowMillis() * 1000 - duration.as(Time.MICROSECONDS);
  }

  @Test
  public void testAccumulateEvents() {
    RequestTimer masterDeliveryDelay = createMock(RequestTimer.class);
    expect(statsProvider.makeRequestTimer(latencyTimerName(Source.SOURCE_MASTER)))
        .andReturn(masterDeliveryDelay);
    masterDeliveryDelay.requestComplete(ONE_SECOND.as(Time.MICROSECONDS));
    expectLastCall().times(3);

    AtomicLong masterLostCounter = new AtomicLong();
    expect(statsProvider.makeCounter(lostCounterName(Source.SOURCE_MASTER)))
        .andReturn(masterLostCounter);

    AtomicLong slaveDisconnectedCounter = new AtomicLong();
    expect(statsProvider.makeCounter(reasonCounterName(Reason.REASON_SLAVE_DISCONNECTED)))
        .andReturn(slaveDisconnectedCounter);

    AtomicLong memoryLimitCounter = new AtomicLong();
    expect(statsProvider.makeCounter(reasonCounterName(Reason.REASON_MEMORY_LIMIT)))
        .andReturn(memoryLimitCounter);

    control.replay();

    clock.advance(Amount.of(1L, Time.HOURS));
    eventBus.post(new TaskStatusReceived(
        TaskState.TASK_RUNNING,
        Optional.of(Source.SOURCE_MASTER),
        Optional.absent(),
        Optional.of(agoMicros(ONE_SECOND))));

    clock.advance(ONE_SECOND);
    eventBus.post(new TaskStatusReceived(
        TaskState.TASK_LOST,
        Optional.of(Source.SOURCE_MASTER),
        Optional.of(Reason.REASON_SLAVE_DISCONNECTED),
        Optional.of(agoMicros(ONE_SECOND))));
    eventBus.post(new TaskStatusReceived(
        TaskState.TASK_FAILED,
        Optional.of(Source.SOURCE_MASTER),
        Optional.of(Reason.REASON_MEMORY_LIMIT),
        Optional.of(agoMicros(ONE_SECOND))));

    // No counting for these since they do not have both a source and timestamp.
    eventBus.post(new TaskStatusReceived(
        TaskState.TASK_LOST,
        Optional.absent(),
        Optional.absent(),
        Optional.absent()));
    eventBus.post(new TaskStatusReceived(
        TaskState.TASK_LOST,
        Optional.absent(),
        Optional.absent(),
        Optional.of(agoMicros(ONE_SECOND))));
    eventBus.post(new TaskStatusReceived(
        TaskState.TASK_LOST,
        Optional.of(Source.SOURCE_MASTER),
        Optional.of(Reason.REASON_SLAVE_DISCONNECTED),
        Optional.absent()));

    // No time tracking for this since the timestamp is the current time.
    eventBus.post(new TaskStatusReceived(
        TaskState.TASK_LOST,
        Optional.of(Source.SOURCE_MASTER),
        Optional.of(Reason.REASON_SLAVE_DISCONNECTED),
        Optional.of(clock.nowMillis() * 1000)
    ));

    assertEquals(3L, masterLostCounter.get());
    assertEquals(3L, slaveDisconnectedCounter.get());
    assertEquals(1L, memoryLimitCounter.get());
  }
}
