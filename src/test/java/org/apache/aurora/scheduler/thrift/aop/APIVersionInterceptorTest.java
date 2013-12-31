package org.apache.aurora.scheduler.thrift.aop;

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.matcher.Matchers;

import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.APIVersion;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.GetJobsResult;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.Result;
import org.apache.aurora.scheduler.thrift.auth.DecoratedThrift;

import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ResponseCode.OK;

import static org.easymock.EasyMock.expect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
