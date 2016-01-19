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

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.scheduler.app.LifecycleModule;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.sla.MetricCalculator.MetricCategory;
import org.apache.aurora.scheduler.sla.SlaModule.SlaUpdater;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.sla.MetricCalculator.MetricCategory.JOB_UPTIMES;
import static org.apache.aurora.scheduler.sla.MetricCalculator.MetricCategory.MEDIANS;
import static org.apache.aurora.scheduler.sla.MetricCalculator.MetricCategory.PLATFORM_UPTIME;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SlaModuleTest extends EasyMockTest {

  private Injector injector;
  private FakeClock clock;
  private StorageTestUtil storageUtil;
  private StatsProvider statsProvider;
  private SlaModule module;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    clock = new FakeClock();
    statsProvider = createMock(StatsProvider.class);
    module = new SlaModule(
        new SlaModule.Params() {
          @Override
          public Amount<Long, Time> slaStatRefreshInterval() {
            return Amount.of(5L, Time.MILLISECONDS);
          }

          @Override
          public Set<MetricCategory> slaProdMetrics() {
            return ImmutableSet.of(JOB_UPTIMES, MEDIANS, PLATFORM_UPTIME);
          }

          @Override
          public Set<MetricCategory> slaNonProdMetrics() {
            return slaProdMetrics();
          }
        });
    injector = Guice.createInjector(
        ImmutableList.<Module>builder()
            .add(module)
            .add(new LifecycleModule())
            .add(new AbstractModule() {
              @Override
              protected void configure() {
                bind(Clock.class).toInstance(clock);
                bind(Storage.class).toInstance(storageUtil.storage);
                bind(StatsProvider.class).toInstance(statsProvider);
              }
            }).build()
    );
  }

  @Test
  public void testNoSchedulingOnStart() {
    assertNotNull(module);

    control.replay();

    ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor) injector.getInstance(
        Key.get(ScheduledExecutorService.class, SlaModule.SlaExecutor.class));

    assertEquals(0, executor.getQueue().size());
    assertEquals(0, executor.getActiveCount());
  }

  @Test
  public void testSchedulingOnEvent() throws Exception {
    assertNotNull(module);

    final CountDownLatch latch = new CountDownLatch(1);
    StatsProvider untracked = createMock(StatsProvider.class);
    expect(statsProvider.untracked()).andReturn(untracked).anyTimes();
    expect(untracked.makeGauge(EasyMock.anyString(), EasyMock.anyObject()))
        .andReturn(EasyMock.anyObject())
        .andAnswer(() -> {
          latch.countDown();
          return null;
        }).anyTimes();

    storageUtil.expectTaskFetch(
        Query.unscoped(),
        SlaTestUtil.makeTask(ImmutableMap.of(clock.nowMillis() - 1000, PENDING), 0)).anyTimes();
    storageUtil.expectOperations();

    control.replay();

    injector.getInstance(SlaUpdater.class).startAsync().awaitRunning();
    latch.await();
  }
}
