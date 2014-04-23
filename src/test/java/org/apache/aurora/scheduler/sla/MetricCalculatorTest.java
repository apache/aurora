/**
 * Copyright 2014 Apache Software Foundation
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
package org.apache.aurora.scheduler.sla;

import com.google.common.collect.ImmutableMap;
import com.twitter.common.base.Supplier;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stat;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.sla.MetricCalculator.MetricCalculatorSettings;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.easymock.EasyMock.expect;

public class MetricCalculatorTest extends EasyMockTest {

  private final FakeClock clock = new FakeClock();
  private StorageTestUtil storageUtil;
  private MetricCalculator calculator;

  @Before
  public void setUp() throws Exception {
    StatsProvider statsProvider = createMock(StatsProvider.class);
    MetricCalculatorSettings settings = new MetricCalculatorSettings(10000);
    storageUtil = new StorageTestUtil(this);
    calculator = new MetricCalculator(storageUtil.storage, clock, settings, statsProvider);
    expect(statsProvider.makeGauge(EasyMock.anyString(), EasyMock.<Supplier<Number>>anyObject()))
        .andReturn(EasyMock.<Stat<Number>>anyObject())
        .anyTimes();
  }

  @Test
  public void runTest() {
    clock.advance(Amount.of(10L, Time.SECONDS));
    storageUtil.expectTaskFetch(Query.unscoped(),
            SlaTestUtil.makeTask(ImmutableMap.of(clock.nowMillis() - 1000, PENDING), 0),
            SlaTestUtil.makeTask(ImmutableMap.of(clock.nowMillis() - 2000, PENDING), 1),
            SlaTestUtil.makeTask(ImmutableMap.of(clock.nowMillis() - 3000, PENDING), 2));
    storageUtil.expectOperations();

    control.replay();
    calculator.run();
  }
}
