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
package com.twitter.aurora.scheduler.thrift.aop;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.auth.CapabilityValidator;
import com.twitter.aurora.gen.AuroraAdmin.Iface;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.Response;
import com.twitter.aurora.gen.ResponseCode;
import com.twitter.aurora.gen.SessionKey;
import com.twitter.common.testing.easymock.EasyMockTest;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class AopModuleTest extends EasyMockTest {

  private static final SessionKey SESSION_KEY = new SessionKey();

  private CapabilityValidator capabilityValidator;
  private Iface mockThrift;

  @Before
  public void setUp() throws Exception {
    capabilityValidator = createMock(CapabilityValidator.class);
    expect(capabilityValidator.toString(SESSION_KEY))
        .andReturn("user")
        .anyTimes();
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
    assertCreateAllowed(ImmutableMap.of("acquireLock", false));
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
    assertEquals(ResponseCode.ERROR, thrift.createJob(job, null, session).getResponseCode());
  }

  @Test(expected = CreationException.class)
  public void testMissingMethod() {
    control.replay();
    getIface(ImmutableMap.of("notamethod", true));
  }

  private void assertCreateAllowed(Map<String, Boolean> toggledMethods) throws Exception {
    JobConfiguration job = new JobConfiguration();
    Response response = new Response();
    expect(mockThrift.createJob(job, null, SESSION_KEY)).andReturn(response);

    control.replay();

    Iface thrift = getIface(toggledMethods);
    assertSame(response, thrift.createJob(job, null, SESSION_KEY));
  }
}
