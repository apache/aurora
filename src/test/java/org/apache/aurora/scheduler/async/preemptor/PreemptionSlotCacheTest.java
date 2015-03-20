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
package org.apache.aurora.scheduler.async.preemptor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.scheduler.async.preemptor.PreemptionSlotFinder.PreemptionSlot;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PreemptionSlotCacheTest {
  private static final Amount<Long, Time> HOLD_DURATION = Amount.of(1L, Time.MINUTES);
  private static final String TASK_ID = "task_id";
  private static final PreemptionSlot SLOT =
      new PreemptionSlot(ImmutableSet.<PreemptionVictim>of(), "slave_id");

  private FakeStatsProvider statsProvider;
  private FakeClock clock;
  private PreemptionSlotCache slotCache;

  @Before
  public void setUp() {
    statsProvider = new FakeStatsProvider();
    clock = new FakeClock();
    slotCache = new PreemptionSlotCache(statsProvider, HOLD_DURATION, clock);
  }

  @Test
  public void testExpiration() {
    slotCache.add(TASK_ID, SLOT);
    assertEquals(Optional.of(SLOT), slotCache.get(TASK_ID));
    assertEquals(1L, statsProvider.getLongValue(
        PreemptionSlotCache.PREEMPTION_SLOT_CACHE_SIZE_STAT));

    clock.advance(HOLD_DURATION);

    assertEquals(Optional.<PreemptionSlot>absent(), slotCache.get(TASK_ID));
  }

  @Test
  public void testRemoval() {
    slotCache.add(TASK_ID, SLOT);
    assertEquals(Optional.of(SLOT), slotCache.get(TASK_ID));
    slotCache.remove(TASK_ID);
    assertEquals(Optional.<PreemptionSlot>absent(), slotCache.get(TASK_ID));
  }
}
