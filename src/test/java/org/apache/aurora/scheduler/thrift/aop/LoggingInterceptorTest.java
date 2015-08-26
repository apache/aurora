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

import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.aopalliance.intercept.MethodInvocation;
import org.apache.aurora.auth.CapabilityValidator;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.SessionKey;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.storage.Storage.TransientStorageException;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LoggingInterceptorTest extends EasyMockTest {
  private CapabilityValidator capabilityValidator;

  private MethodInvocation methodInvocation;

  private LoggingInterceptor loggingInterceptor;

  // java.lang.reflect.Method is final so we need to create an actual class to get instances of
  // them.
  // TODO(ksweeney): Investigate using PowerMock to do this instead.
  private static class InterceptedClass {
    Response respond() {
      throw new UnsupportedOperationException();
    }

    Response respond(Object arg1, @Nullable Object arg2) {
      throw new UnsupportedOperationException();
    }

    Response respond(JobConfiguration jobConfiguration) {
      throw new UnsupportedOperationException();
    }

    Response respond(SessionKey sessionKey) {
      throw new UnsupportedOperationException();
    }
  }

  @Before
  public void setUp() throws Exception {
    capabilityValidator = createMock(CapabilityValidator.class);

    loggingInterceptor = new LoggingInterceptor(capabilityValidator);

    methodInvocation = createMock(MethodInvocation.class);
  }

  @Test
  public void testInvokeTransientStorageException() throws Throwable {
    expect(methodInvocation.getMethod())
        .andReturn(InterceptedClass.class.getDeclaredMethod("respond"));
    expect(methodInvocation.getArguments()).andReturn(new Object[]{});
    expect(methodInvocation.proceed()).andThrow(new TransientStorageException("try again"));

    control.replay();

    assertEquals(
        ResponseCode.ERROR_TRANSIENT,
        ((Response) loggingInterceptor.invoke(methodInvocation)).getResponseCode());
  }

  @Test
  public void testInvokeRuntimeException() throws Throwable {
    expect(methodInvocation.getMethod())
        .andReturn(InterceptedClass.class.getDeclaredMethod("respond"));
    expect(methodInvocation.getArguments()).andReturn(new Object[] {});
    expect(methodInvocation.proceed()).andThrow(new RuntimeException());

    control.replay();

    assertEquals(
        ResponseCode.ERROR,
        ((Response) loggingInterceptor.invoke(methodInvocation)).getResponseCode());
  }

  @Test
  public void testInvokePrintsArgs() throws Throwable {
    Response response = new Response();

    expect(methodInvocation.getMethod())
        .andReturn(InterceptedClass.class.getDeclaredMethod("respond", Object.class, Object.class));

    // EasyMock cannot mock toString.
    final AtomicBoolean calledAtLeastOnce = new AtomicBoolean(false);
    Object arg1 = new Object() {
      @Override
      public String toString() {
        calledAtLeastOnce.set(true);
        return "arg1";
      }
    };
    expect(methodInvocation.getArguments()).andReturn(new Object[] {arg1, null});
    expect(methodInvocation.proceed()).andReturn(response);

    control.replay();

    assertSame(response, loggingInterceptor.invoke(methodInvocation));
    assertTrue(calledAtLeastOnce.get());
  }

  @Test
  public void testInvokePrintsBlankedJobConfiguration() throws Throwable {
    Response response = new Response();
    expect(methodInvocation.getMethod())
        .andReturn(InterceptedClass.class.getDeclaredMethod("respond", JobConfiguration.class));
    expect(methodInvocation.getArguments())
        .andReturn(new Object[] {new JobConfiguration().setTaskConfig(new TaskConfig())});
    expect(methodInvocation.proceed()).andReturn(response);

    control.replay();

    assertSame(response, loggingInterceptor.invoke(methodInvocation));
  }

  @Test
  public void testInvokePrintsJobConfiguration() throws Throwable {
    Response response = new Response();
    expect(methodInvocation.getMethod())
        .andReturn(InterceptedClass.class.getDeclaredMethod("respond", JobConfiguration.class));
    expect(methodInvocation.getArguments()).andReturn(new Object[] {new JobConfiguration()});
    expect(methodInvocation.proceed()).andReturn(response);

    control.replay();

    assertSame(response, loggingInterceptor.invoke(methodInvocation));
  }

  @Test
  public void testInvokePrintsSessionKey() throws Throwable {
    Response response = new Response();
    SessionKey sessionKey = new SessionKey();

    expect(methodInvocation.getMethod())
        .andReturn(InterceptedClass.class.getDeclaredMethod("respond", SessionKey.class));
    expect(methodInvocation.getArguments()).andReturn(new Object[] {sessionKey});
    expect(methodInvocation.proceed()).andReturn(response);

    expect(capabilityValidator.toString(sessionKey)).andReturn("session-key");

    control.replay();

    assertSame(response, loggingInterceptor.invoke(methodInvocation));
  }
}
