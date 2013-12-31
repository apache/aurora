package com.twitter.aurora.scheduler.thrift.aop;

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.matcher.Matchers;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.APIVersion;
import com.twitter.aurora.gen.AuroraAdmin;
import com.twitter.aurora.gen.GetJobsResult;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.Response;
import com.twitter.aurora.gen.Result;
import com.twitter.aurora.scheduler.thrift.auth.DecoratedThrift;
import com.twitter.common.testing.easymock.EasyMockTest;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static com.twitter.aurora.gen.ResponseCode.OK;

public class APIVersionInterceptorTest extends EasyMockTest {

  private static final String ROLE = "bob";

  private AuroraAdmin.Iface realThrift;
  private AuroraAdmin.Iface decoratedThrift;

  private APIVersionInterceptor interceptor;

  @Before
  public void setUp() {
    interceptor = new APIVersionInterceptor();
    realThrift = createMock(AuroraAdmin.Iface.class);
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        MockDecoratedThrift.bindForwardedMock(binder(), realThrift);
        AopModule.bindThriftDecorator(
            binder(),
            Matchers.annotatedWith(DecoratedThrift.class),
            interceptor);
      }
    });
    decoratedThrift = injector.getInstance(AuroraAdmin.Iface.class);
  }

  @Test
  public void testVersionIsSet() throws Exception {
    Response response = new Response().setResponseCode(OK)
        .setResult(Result.getJobsResult(new GetJobsResult()
            .setConfigs(ImmutableSet.<JobConfiguration>of())));

    expect(realThrift.getJobs(ROLE)).andReturn(response);
    control.replay();

    assertNotNull(decoratedThrift.getJobs(ROLE).version);
  }

  @Test
  public void testExistingVersion() throws Exception {
    Response response = new Response().setResponseCode(OK)
        .setResult(Result.getJobsResult(new GetJobsResult()
            .setConfigs(ImmutableSet.<JobConfiguration>of())))
        .setVersion(new APIVersion().setMajor(1));

    expect(realThrift.getJobs(ROLE)).andReturn(response);
    control.replay();

    Response decoratedResponse = decoratedThrift.getJobs(ROLE);
    assertNotNull(decoratedResponse.version);
    assertEquals(decoratedResponse.version.getMajor(), 1);
  }

}
