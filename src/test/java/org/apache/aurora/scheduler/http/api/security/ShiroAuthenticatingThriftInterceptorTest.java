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

import com.google.inject.util.Providers;

import org.aopalliance.intercept.MethodInvocation;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.Response;
import org.apache.aurora.scheduler.thrift.Responses;
import org.apache.shiro.authz.UnauthenticatedException;
import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertSame;

public class ShiroAuthenticatingThriftInterceptorTest extends EasyMockTest {
  private ShiroAuthenticatingThriftInterceptor interceptor;
  private Subject subject;
  private MethodInvocation methodInvocation;

  @Before
  public void setUp() throws Exception {
    interceptor = new ShiroAuthenticatingThriftInterceptor();
    subject = createMock(Subject.class);
    methodInvocation = createMock(MethodInvocation.class);
  }

  private void replayAndInitialize() {
    control.replay();
    interceptor.initialize(Providers.of(subject));
  }

  @Test(expected = UnauthenticatedException.class)
  public void testInvokeNotAuthenticated() throws Throwable {
    expect(subject.isAuthenticated()).andReturn(false);

    replayAndInitialize();

    interceptor.invoke(methodInvocation);
  }

  @Test
  public void testInvokeAuthenticated() throws Throwable {
    Response response = Responses.ok();
    expect(subject.isAuthenticated()).andReturn(true);
    expect(methodInvocation.proceed()).andReturn(response);

    replayAndInitialize();

    assertSame(response, interceptor.invoke(methodInvocation));
  }
}
