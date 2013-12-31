/*
 * Copyright 2013 Twitter, Inc.
 *
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
package org.apache.aurora.scheduler.stats;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.twitter.common.stats.StatsProvider;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.Quota;
import org.apache.aurora.scheduler.quota.Quotas;
import org.apache.aurora.scheduler.stats.SlotSizeCounter.ResourceSlotProvider;
import org.apache.aurora.scheduler.storage.entities.IQuota;

import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;

import static org.junit.Assert.assertEquals;

public class SlotSizeCounterTest extends EasyMockTest {

  private static final IQuota SMALL = IQuota.build(new Quota(1.0, 1024, 4096));
  private static final IQuota LARGE = Quotas.scale(SMALL, 4);

  private static final Map<String, IQuota> SLOT_SIZES = ImmutableMap.of(
      "small", SMALL,
      "large", LARGE);

  private ResourceSlotProvider slotProvider;
  private StatsProvider statsProvider;
  private Runnable slotCounter;

  private AtomicLong smallCounter = new AtomicLong();
  private AtomicLong largeCounter = new AtomicLong();

  @Before
  public void setUp() {
    slotProvider = createMock(ResourceSlotProvider.class);
    statsProvider = createMock(StatsProvider.class);
    slotCounter = new SlotSizeCounter(SLOT_SIZES, slotProvider, new CachedCounters(statsProvider));
  }

  private void expectStatExport() {
    expect(statsProvider.makeCounter(SlotSizeCounter.getStatName("small")))
        .andReturn(smallCounter);
    expect(statsProvider.makeCounter(SlotSizeCounter.getStatName("large")))
        .andReturn(largeCounter);
  }

  private void expectGetSlots(IQuota... returned) {
    expect(slotProvider.get()).andReturn(ImmutableList.copyOf(returned));
  }

  @Test
  public void testNoOffers() {
    expectStatExport();
    expectGetSlots();

    control.replay();

    slotCounter.run();
    assertEquals(0, smallCounter.get());
    assertEquals(0, largeCounter.get());
  }

  @Test
  public void testTinyOffers() {
    expectStatExport();
    expectGetSlots(IQuota.build(new Quota(0.1, 1, 1)));

    control.replay();

    slotCounter.run();
    assertEquals(0, smallCounter.get());
    assertEquals(0, largeCounter.get());
  }

  @Test
  public void testStarvedResourceVector() {
    expectStatExport();
    expectGetSlots(IQuota.build(new Quota(1000, 16384, 1)));

    control.replay();

    slotCounter.run();
    assertEquals(0, smallCounter.get());
    assertEquals(0, largeCounter.get());
  }

  @Test
  public void testCountSlots() {
    expectStatExport();
    expectGetSlots(
        SMALL,
        SMALL,
        LARGE,
        Quotas.scale(LARGE, 4),
        IQuota.build(new Quota(1, 1, 1)));

    control.replay();

    slotCounter.run();
    assertEquals(22, smallCounter.get());
    assertEquals(5, largeCounter.get());
  }
}
