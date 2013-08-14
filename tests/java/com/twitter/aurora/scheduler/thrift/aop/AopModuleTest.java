package com.twitter.aurora.scheduler.thrift.aop;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.AuroraAdmin.Iface;
import com.twitter.aurora.gen.CreateJobResponse;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.ResponseCode;
import com.twitter.aurora.gen.SessionKey;
import com.twitter.aurora.scheduler.thrift.auth.CapabilityValidator;
import com.twitter.common.testing.easymock.EasyMockTest;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class AopModuleTest extends EasyMockTest {

  private CapabilityValidator capabilityValidator;
  private Iface mockThrift;

  @Before
  public void setUp() {
    capabilityValidator = createMock(CapabilityValidator.class);
    mockThrift = createMock(Iface.class);
  }

  private Iface getIface(Map<String, Boolean> toggledMethods) {
    Injector injector = Guice.createInjector(
        new AbstractModule() {
          @Override protected void configure() {
            bind(CapabilityValidator.class).toInstance(capabilityValidator);
            MockDecoratedThrift.bindForwardedMock(binder(), mockThrift);
          }
        },
        new AopModule(toggledMethods));
    return injector.getInstance(Iface.class);
  }

  @Test
  public void testNonFlaggedMethod() throws Exception {
    assertCreateAllowed(ImmutableMap.of("startUpdate", false));
  }

  @Test
  public void testNoFlaggedMethods() throws Exception {
    assertCreateAllowed(ImmutableMap.<String, Boolean>of());
  }

  @Test
  public void testFlaggedMethodEnabled() throws Exception {
    assertCreateAllowed(ImmutableMap.of("createJob", true));
  }

  @Test
  public void testFlaggedMethodDisabled() throws Exception {
    JobConfiguration job = new JobConfiguration();
    SessionKey session = new SessionKey();

    control.replay();

    Iface thrift = getIface(ImmutableMap.of("createJob", false));
    assertEquals(ResponseCode.ERROR, thrift.createJob(job, session).getResponseCode());
  }

  @Test(expected = CreationException.class)
  public void testMissingMethod() {
    control.replay();
    getIface(ImmutableMap.of("notamethod", true));
  }

  private void assertCreateAllowed(Map<String, Boolean> toggledMethods) throws Exception {
    JobConfiguration job = new JobConfiguration();
    SessionKey session = new SessionKey();
    CreateJobResponse response = new CreateJobResponse();
    expect(mockThrift.createJob(job, session)).andReturn(response);

    control.replay();

    Iface thrift = getIface(toggledMethods);
    assertSame(response, thrift.createJob(job, session));
  }
}
