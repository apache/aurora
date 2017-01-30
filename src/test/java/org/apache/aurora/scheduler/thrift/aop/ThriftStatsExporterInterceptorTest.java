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
package org.apache.aurora.scheduler.thrift.aop;

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.matcher.Matchers;

import org.aopalliance.intercept.MethodInvocation;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.GetJobsResult;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.Result;
import org.apache.aurora.scheduler.thrift.auth.DecoratedThrift;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ResponseCode.OK;
import static org.apache.aurora.scheduler.thrift.Responses.error;
import static org.apache.aurora.scheduler.thrift.Responses.ok;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class ThriftStatsExporterInterceptorTest extends EasyMockTest {

  private static final String ROLE = "bob";

  private AnnotatedAuroraAdmin realThrift;
  private AnnotatedAuroraAdmin decoratedThrift;
  private ThriftStatsExporterInterceptor statsInterceptor;

  @Before
  public void setUp() {
    statsInterceptor = new ThriftStatsExporterInterceptor();
    realThrift = createMock(AnnotatedAuroraAdmin.class);
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        MockDecoratedThrift.bindForwardedMock(binder(), realThrift);
        AopModule.bindThriftDecorator(
            binder(),
            Matchers.annotatedWith(DecoratedThrift.class),
            statsInterceptor);
      }
    });
    decoratedThrift = injector.getInstance(AnnotatedAuroraAdmin.class);
    addTearDown(Stats::flush);
  }

  @Test
  public void testIncrementStat() throws Exception {
    Response response = new Response().setResponseCode(OK)
        .setResult(Result.getJobsResult(new GetJobsResult()
        .setConfigs(ImmutableSet.of())));

    expect(realThrift.getJobs(ROLE)).andReturn(response);
    control.replay();

    assertSame(response, decoratedThrift.getJobs(ROLE));
    String statName = timingStatName("getJobs");
    assertEquals(1L, Stats.getVariable(statName + "_events").read());
    assertNotNull(Stats.getVariable(statName + "_events_per_sec"));
    assertNotNull(Stats.getVariable(statName + "_nanos_per_event"));
    assertNotNull(Stats.getVariable(statName + "_nanos_total"));
    assertNotNull(Stats.getVariable(statName + "_nanos_total_per_sec"));
  }

  @Test
  public void testMeasuredMethod() throws Throwable {
    MethodInvocation invocation = createMock(MethodInvocation.class);

    expect(invocation.getMethod())
        .andReturn(InterceptedClass.class.getDeclaredMethod("measuredMethod"));
    expect(invocation.proceed()).andReturn(ok().setResult(Result.getJobsResult(
        new GetJobsResult(ImmutableSet.of(new JobConfiguration())))));

    control.replay();

    new ThriftStatsExporterInterceptor().invoke(invocation);
    assertEquals(1L, Stats.getVariable(workloadStatName("measuredMethod")).read());
  }

  @Test
  public void testUnmeasuredMethod() throws Throwable {
    MethodInvocation invocation = createMock(MethodInvocation.class);

    expect(invocation.getMethod())
        .andReturn(InterceptedClass.class.getDeclaredMethod("unmeasuredMethod"));
    expect(invocation.proceed()).andReturn(ok().setResult(Result.getJobsResult(
        new GetJobsResult(ImmutableSet.of(new JobConfiguration())))));

    control.replay();

    new ThriftStatsExporterInterceptor().invoke(invocation);
    assertNull(Stats.getVariable(workloadStatName("unmeasuredMethod")));
  }

  @Test
  public void testExceptionalMeasuredMethod() throws Throwable {
    MethodInvocation invocation = createMock(MethodInvocation.class);

    expect(invocation.getMethod())
        .andReturn(InterceptedClass.class.getDeclaredMethod("measuredMethod"));
    expect(invocation.proceed()).andThrow(new Exception());

    control.replay();

    try {
      new ThriftStatsExporterInterceptor().invoke(invocation);
      fail("Should not be reached");
    } catch (Exception e) {
      assertNull(Stats.getVariable(workloadStatName("measuredMethod")));
    }
  }

  @Test
  public void testMeasuredMethodWithErrorResponse() throws Throwable {
    MethodInvocation invocation = createMock(MethodInvocation.class);

    expect(invocation.getMethod())
        .andReturn(InterceptedClass.class.getDeclaredMethod("measuredMethod"));
    expect(invocation.proceed()).andReturn(error("ERROR"));

    control.replay();

    new ThriftStatsExporterInterceptor().invoke(invocation);
    assertNull(Stats.getVariable(workloadStatName("measuredMethod")));
  }

  private static class InterceptedClass {
    @ThriftWorkload
    public Response measuredMethod() {
      throw new UnsupportedOperationException("Should not be called.");
    }

    public Response unmeasuredMethod() {
      throw new UnsupportedOperationException("Should not be called.");
    }
  }

  private static String timingStatName(String methodName) {
    return statName(ThriftStatsExporterInterceptor.TIMING_STATS_NAME_TEMPLATE, methodName);
  }

  private static String workloadStatName(String methodName) {
    return statName(ThriftStatsExporterInterceptor.WORKLOAD_STATS_NAME_TEMPLATE, methodName);
  }

  private static String statName(String template, String methodName) {
    return String.format(template, methodName);
  }

}
