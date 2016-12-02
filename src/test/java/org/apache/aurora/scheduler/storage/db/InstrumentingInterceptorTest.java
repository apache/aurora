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
package org.apache.aurora.scheduler.storage.db;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.aurora.common.stats.SlidingStats;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.Clock;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Invocation;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import static junit.framework.TestCase.assertNotNull;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MappedStatement.class, Method.class})
public class InstrumentingInterceptorTest extends EasyMockTest {
  private InstrumentingInterceptor interceptor;
  private Invocation invocation;
  private SlidingStats slidingStats;

  private Clock mockClock;

  @Before
  public void setUp() throws Exception {
    invocation = createMock(Invocation.class);
    mockClock = createMock(Clock.class);
    slidingStats = createMock(SlidingStats.class);
    interceptor = new InstrumentingInterceptor(mockClock, name -> slidingStats);
  }

  private void expectGetArgs(Object[] args, int times) {
    expect(invocation.getArgs()).andReturn(args).times(times);
  }

  @Test
  public void testStatIsCreatedOnce() throws Throwable {
    final AtomicLong factoryCallCount = new AtomicLong(0);
    interceptor = new InstrumentingInterceptor(mockClock, name -> {
      factoryCallCount.incrementAndGet();
      return slidingStats;
    });

    String statName = "test";
    MappedStatement fakeMappedStatement = createMock(MappedStatement.class);
    Object[] args = new Object[] {fakeMappedStatement};

    expect(mockClock.nowNanos()).andReturn(0L).andReturn(1000L);

    expectGetArgs(args, 3);

    expect(fakeMappedStatement.getId()).andReturn(statName);
    expect(invocation.proceed()).andReturn("result");

    slidingStats.accumulate(1000);
    expectLastCall();

    expect(mockClock.nowNanos()).andReturn(0L).andReturn(1000L);

    expectGetArgs(args, 3);

    expect(fakeMappedStatement.getId()).andReturn(statName);
    expect(invocation.proceed()).andReturn("result");

    slidingStats.accumulate(1000);
    expectLastCall();

    control.replay();

    // Perform the test
    interceptor.intercept(invocation);

    assertEquals(1L, factoryCallCount.get());
    interceptor.intercept(invocation);
    assertEquals(1L, factoryCallCount.get());
  }

  @Test
  public void testInterceptMarksMetrics() throws Throwable {
    MappedStatement fakeMappedStatement = createMock(MappedStatement.class);
    Object[] args = new Object[] {fakeMappedStatement};

    expect(mockClock.nowNanos()).andReturn(0L).andReturn(1000L);

    expectGetArgs(args, 3);

    expect(fakeMappedStatement.getId()).andReturn("test");
    expect(invocation.proceed()).andReturn("result");

    slidingStats.accumulate(1000);
    expectLastCall();

    control.replay();

    // Perform the test
    Object res = interceptor.intercept(invocation);
    assertEquals("result", res);
  }

  @Test
  public void testInterceptNotAMappedStatement() throws Throwable {
    interceptor = new InstrumentingInterceptor(mockClock);
    Method mockMethod = PowerMock.createMock(Method.class);

    Object notAMappedStatement = new Object();
    Object[] args = new Object[] {notAMappedStatement};

    expect(mockClock.nowNanos()).andReturn(0L).andReturn(1000L);

    expectGetArgs(args, 2);

    expect(invocation.getMethod()).andReturn(mockMethod);
    expect(invocation.getTarget()).andReturn("test");
    expect(invocation.proceed()).andReturn(null);

    control.replay();

    assertNull(Stats.getVariable("mybatis.invalid_invocations_nanos_total"));
    assertNull(Stats.getVariable("mybatis.invalid_invocations_nanos_total_per_sec"));
    assertNull(Stats.getVariable("mybatis.invalid_invocations_events"));
    assertNull(Stats.getVariable("mybatis.invalid_invocations_nanos_per_event"));
    assertNull(Stats.getVariable("mybatis.invalid_invocations_events_per_sec"));

    interceptor.intercept(invocation);

    // upon interception of invocation that does not have a valid MappedStatement use
    // invalid_invocations as the name
    assertNotNull(Stats.getVariable("mybatis.invalid_invocations_nanos_total"));
    assertNotNull(Stats.getVariable("mybatis.invalid_invocations_nanos_total_per_sec"));
    assertNotNull(Stats.getVariable("mybatis.invalid_invocations_events"));
    assertNotNull(Stats.getVariable("mybatis.invalid_invocations_nanos_per_event"));
    assertNotNull(Stats.getVariable("mybatis.invalid_invocations_events_per_sec"));

    assertEquals(1000L, Stats.getVariable("mybatis.invalid_invocations_nanos_total").read());
    assertEquals(1L, Stats.getVariable("mybatis.invalid_invocations_events").read());
  }
}
