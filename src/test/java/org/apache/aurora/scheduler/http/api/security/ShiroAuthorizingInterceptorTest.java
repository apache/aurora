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
package org.apache.aurora.scheduler.http.api.security;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;

import com.google.inject.util.Providers;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.aopalliance.intercept.MethodInvocation;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.SessionKey;
import org.apache.aurora.scheduler.spi.Permissions;
import org.apache.aurora.scheduler.spi.Permissions.Domain;
import org.apache.aurora.scheduler.thrift.Responses;
import org.apache.shiro.subject.Subject;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.http.api.security.ShiroAuthorizingInterceptor.SHIRO_AUTHORIZATION_FAILURES;
import static org.apache.aurora.scheduler.spi.Permissions.Domain.THRIFT_AURORA_ADMIN;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ShiroAuthorizingInterceptorTest extends EasyMockTest {
  private static final Domain DOMAIN = THRIFT_AURORA_ADMIN;

  private Subject subject;
  private StatsProvider statsProvider;
  private MethodInvocation methodInvocation;
  private Method interceptedMethod;

  private ShiroAuthorizingInterceptor interceptor;

  @Before
  public void setUp() throws NoSuchMethodException {
    interceptor = new ShiroAuthorizingInterceptor(DOMAIN);
    subject = createMock(Subject.class);
    statsProvider = createMock(StatsProvider.class);
    methodInvocation = createMock(MethodInvocation.class);
    interceptedMethod = AuroraAdmin.Iface.class.getMethod("snapshot", SessionKey.class);
    expect(statsProvider.makeCounter(SHIRO_AUTHORIZATION_FAILURES)).andReturn(new AtomicLong());
  }

  private void replayAndInitialize() {
    control.replay();
    interceptor.initialize(Providers.of(subject), statsProvider);
  }

  private IExpectationSetters<Boolean> expectSubjectPermitted() {
    return expect(subject.isPermitted(
        Permissions.createUnscopedPermission(DOMAIN, interceptedMethod.getName())));
  }

  @Test
  public void testAuthorized() throws Throwable {
    Response response = Responses.ok();
    expect(methodInvocation.getMethod()).andReturn(interceptedMethod);
    expectSubjectPermitted().andReturn(true);
    expect(methodInvocation.proceed()).andReturn(response);

    replayAndInitialize();

    assertSame(response, interceptor.invoke(methodInvocation));
  }

  @Test
  public void testNotAuthorized() throws Throwable {
    expect(methodInvocation.getMethod()).andReturn(interceptedMethod);
    expectSubjectPermitted().andReturn(false);
    expect(subject.getPrincipal()).andReturn("ksweeney");

    replayAndInitialize();

    assertEquals(
        ResponseCode.AUTH_FAILED,
        ((Response) interceptor.invoke(methodInvocation)).getResponseCode());
  }
}
