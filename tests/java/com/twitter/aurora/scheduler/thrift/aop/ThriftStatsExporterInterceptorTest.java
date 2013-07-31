package com.twitter.aurora.scheduler.thrift.aop;

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.matcher.Matchers;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.GetJobsResponse;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.MesosAdmin;
import com.twitter.aurora.scheduler.thrift.auth.DecoratedThrift;
import com.twitter.common.stats.Stats;
import com.twitter.common.testing.EasyMockTest;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import static com.twitter.aurora.gen.ResponseCode.OK;

public class ThriftStatsExporterInterceptorTest extends EasyMockTest {

  private static final String ROLE = "bob";

  private MesosAdmin.Iface realThrift;
  private MesosAdmin.Iface decoratedThrift;
  private ThriftStatsExporterInterceptor statsInterceptor;

  @Before
  public void setUp() {
    statsInterceptor = new ThriftStatsExporterInterceptor();
    realThrift = createMock(MesosAdmin.Iface.class);
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override protected void configure() {
        MockDecoratedThrift.bindForwardedMock(binder(), realThrift);
        AopModule.bindThriftDecorator(
            binder(),
            Matchers.annotatedWith(DecoratedThrift.class),
            statsInterceptor);
      }
    });
    decoratedThrift = injector.getInstance(MesosAdmin.Iface.class);
  }

  @Test
  public void testIncrementStat() throws Exception {
    GetJobsResponse getJobsResponse = new GetJobsResponse()
        .setConfigs(ImmutableSet.<JobConfiguration>of())
        .setResponseCode(OK);

    expect(realThrift.getJobs(ROLE)).andReturn(getJobsResponse);
    control.replay();

    assertSame(getJobsResponse, decoratedThrift.getJobs(ROLE));
    assertNotNull(Stats.getVariable("scheduler_thrift_getJobs_events"));
    assertNotNull(Stats.getVariable("scheduler_thrift_getJobs_events_per_sec"));
    assertNotNull(Stats.getVariable("scheduler_thrift_getJobs_nanos_per_event"));
    assertNotNull(Stats.getVariable("scheduler_thrift_getJobs_nanos_total"));
    assertNotNull(Stats.getVariable("scheduler_thrift_getJobs_nanos_total_per_sec"));
  }
}
