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
import org.apache.aurora.scheduler.thrift.Responses;
import org.apache.shiro.authz.UnauthenticatedException;
import org.apache.shiro.authz.permission.WildcardPermission;
import org.apache.shiro.subject.Subject;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class ShiroThriftInterceptorTest extends EasyMockTest {
  private static final String PERMISSION = "test";
  private static final String PRINCIPAL = "test-user";

  private ShiroThriftInterceptor interceptor;
  private Subject subject;
  private StatsProvider statsProvider;
  private AtomicLong shiroAuthorizationFailures;
  private MethodInvocation methodInvocation;
  private Method interceptedMethod;

  @Before
  public void setUp() throws Exception {
    interceptor = new ShiroThriftInterceptor(PERMISSION);
    subject = createMock(Subject.class);
    statsProvider = createMock(StatsProvider.class);
    shiroAuthorizationFailures = new AtomicLong();
    expect(statsProvider.makeCounter(ShiroThriftInterceptor.SHIRO_AUTHORIZATION_FAILURES))
        .andReturn(shiroAuthorizationFailures);
    methodInvocation = createMock(MethodInvocation.class);
    interceptedMethod = AuroraAdmin.Iface.class.getMethod("snapshot", SessionKey.class);
  }

  private void replayAndInitialize() {
    control.replay();
    interceptor.initialize(Providers.of(subject), statsProvider);
  }

  @Test(expected = UnauthenticatedException.class)
  public void testInvokeNotAuthenticated() throws Throwable {
    expect(subject.isAuthenticated()).andReturn(false);

    replayAndInitialize();

    interceptor.invoke(methodInvocation);
  }

  private IExpectationSetters<Boolean> expectSubjectPermitted() {
    return expect(subject.isPermitted(
        new WildcardPermission(PERMISSION + ":" + interceptedMethod.getName())));
  }

  @Test
  public void testInvokeNotAuthorized() throws Throwable {
    expect(subject.isAuthenticated()).andReturn(true);
    expect(methodInvocation.getMethod()).andReturn(interceptedMethod);
    expectSubjectPermitted().andReturn(false);
    expect(subject.getPrincipal()).andReturn(PRINCIPAL);

    replayAndInitialize();

    assertEquals(
        ResponseCode.AUTH_FAILED,
        ((Response) interceptor.invoke(methodInvocation)).getResponseCode());
  }

  @Test
  public void testInvokeAuthorized() throws Throwable {
    expect(subject.isAuthenticated()).andReturn(true);
    expect(methodInvocation.getMethod()).andReturn(interceptedMethod);
    expectSubjectPermitted().andReturn(true);
    expect(methodInvocation.proceed()).andReturn(Responses.ok());

    replayAndInitialize();

    assertEquals(Responses.ok(), interceptor.invoke(methodInvocation));
  }
}
