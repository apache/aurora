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
package org.apache.aurora.scheduler.sla;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.sla.MetricCalculator.MetricCalculatorSettings;
import org.apache.aurora.scheduler.sla.SlaGroup.GroupType;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.sla.MetricCalculator.MetricCategory.JOB_UPTIMES;
import static org.apache.aurora.scheduler.sla.MetricCalculator.MetricCategory.MEDIANS;
import static org.apache.aurora.scheduler.sla.MetricCalculator.MetricCategory.PLATFORM_UPTIME;
import static org.apache.aurora.scheduler.sla.MetricCalculator.NAME_QUALIFIER_NON_PROD;
import static org.apache.aurora.scheduler.sla.MetricCalculator.NAME_QUALIFIER_PROD;
import static org.apache.aurora.scheduler.sla.SlaAlgorithm.AlgorithmType;
import static org.apache.aurora.scheduler.sla.SlaTestUtil.makeTask;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class MetricCalculatorTest extends EasyMockTest {

  static final Multimap<AlgorithmType, GroupType> PROD_METRICS =
      ImmutableMultimap.<AlgorithmType, GroupType>builder()
          .putAll(JOB_UPTIMES.getMetrics())
          .putAll(MEDIANS.getMetrics())
          .putAll(PLATFORM_UPTIME.getMetrics())
          .build();

  static final Multimap<AlgorithmType, GroupType> NON_PROD_METRICS =
      ImmutableMultimap.<AlgorithmType, GroupType>builder()
          .putAll(JOB_UPTIMES.getMetrics())
          .putAll(MEDIANS.getMetrics())
          .putAll(PLATFORM_UPTIME.getMetrics())
          .build();

  @Test
  public void testRun() {
    FakeClock clock = new FakeClock();
    StatsProvider statsProvider = createMock(StatsProvider.class);
    StatsProvider untracked = createMock(StatsProvider.class);
    MetricCalculatorSettings settings = new MetricCalculatorSettings(
        10000,
        ImmutableSet.of(JOB_UPTIMES, MEDIANS, PLATFORM_UPTIME),
        ImmutableSet.of(JOB_UPTIMES, MEDIANS, PLATFORM_UPTIME));
    StorageTestUtil storageUtil = new StorageTestUtil(this);
    MetricCalculator calculator = new MetricCalculator(
        storageUtil.storage,
        clock,
        settings,
        statsProvider);

    expect(statsProvider.untracked()).andReturn(untracked).anyTimes();

    Capture<String> names = new Capture<>(CaptureType.ALL);
    expect(untracked.makeGauge(EasyMock.capture(names), EasyMock.anyObject()))
        .andReturn(EasyMock.anyObject())
        .anyTimes();

    IScheduledTask task1 = makeTask(ImmutableMap.of(clock.nowMillis() - 1000, PENDING), 0);
    IScheduledTask task2 = makeTask(ImmutableMap.of(clock.nowMillis() - 2000, PENDING), 1);
    IScheduledTask task3 = makeTask(ImmutableMap.of(clock.nowMillis() - 3000, PENDING), 2);
    IScheduledTask task4 = makeTask(ImmutableMap.of(clock.nowMillis() - 4000, PENDING), 3, false);

    clock.advance(Amount.of(10L, Time.SECONDS));
    storageUtil.expectTaskFetch(Query.unscoped(), task1, task2, task3, task4);
    storageUtil.expectOperations();

    control.replay();
    calculator.run();

    Set<String> metricNames = generateMetricNames(
        ImmutableSet.of(task1, task2, task3, task4),
        ImmutableSet.of(PROD_METRICS, NON_PROD_METRICS));

    assertEquals(PROD_METRICS.size() + NON_PROD_METRICS.size(), names.getValues().size());
    assertEquals(metricNames, ImmutableSet.copyOf(names.getValues()));
  }

  private Set<String> generateMetricNames(
      Set<IScheduledTask> tasks,
      Set<Multimap<AlgorithmType, SlaGroup.GroupType>> definitions) {

    ImmutableSet.Builder<String> names = ImmutableSet.builder();
    for (Multimap<AlgorithmType, SlaGroup.GroupType> definition : definitions) {
      for (Map.Entry<AlgorithmType, SlaGroup.GroupType> entry : definition.entries()) {
        for (String metric : entry.getValue().getSlaGroup().createNamedGroups(tasks).keys()) {
          names.add(metric + entry.getKey().getAlgorithmName() + NAME_QUALIFIER_PROD);
          names.add(metric + entry.getKey().getAlgorithmName() + NAME_QUALIFIER_NON_PROD);
        }
      }
    }

    return names.build();
  }
}
