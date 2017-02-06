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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.matcher.Matchers;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.thrift.Responses;
import org.apache.aurora.scheduler.thrift.aop.AnnotatedAuroraAdmin;
import org.apache.aurora.scheduler.thrift.aop.MockDecoratedThrift;
import org.apache.shiro.subject.Subject;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.http.api.security.ShiroAuthorizingParamInterceptor.SHIRO_AUTHORIZATION_FAILURES;
import static org.apache.aurora.scheduler.http.api.security.ShiroAuthorizingParamInterceptor.SHIRO_BAD_REQUESTS;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ShiroAuthorizingParamInterceptorTest extends EasyMockTest {
  private ShiroAuthorizingParamInterceptor interceptor;

  private Subject subject;
  private AnnotatedAuroraAdmin thrift;
  private StatsProvider statsProvider;

  private AnnotatedAuroraAdmin decoratedThrift;

  private static final IJobKey JOB_KEY = JobKeys.from("role", "env", "name");

  @Before
  public void setUp() {
    interceptor = new ShiroAuthorizingParamInterceptor();
    subject = createMock(Subject.class);
    statsProvider = createMock(StatsProvider.class);
    thrift = createMock(AnnotatedAuroraAdmin.class);
  }

  private void replayAndInitialize() {
    expect(statsProvider.makeCounter(SHIRO_AUTHORIZATION_FAILURES))
        .andReturn(new AtomicLong());
    expect(statsProvider.makeCounter(SHIRO_BAD_REQUESTS))
        .andReturn(new AtomicLong());
    control.replay();
    decoratedThrift = Guice
        .createInjector(new AbstractModule() {
          @Override
          protected void configure() {
            bind(Subject.class).toInstance(subject);
            MockDecoratedThrift.bindForwardedMock(binder(), thrift);
            bindInterceptor(
                Matchers.subclassesOf(AnnotatedAuroraAdmin.class),
                HttpSecurityModule.AURORA_SCHEDULER_MANAGER_SERVICE,
                interceptor);
            bind(StatsProvider.class).toInstance(statsProvider);
            requestInjection(interceptor);
          }
        }).getInstance(AnnotatedAuroraAdmin.class);
  }

  @Test
  public void testHandlesAllDecoratedParamTypes() {
    control.replay();

    for (Method method : AnnotatedAuroraAdmin.class.getMethods()) {
      if (HttpSecurityModule.AURORA_SCHEDULER_MANAGER_SERVICE.matches(method)) {
        interceptor.getAuthorizingParamGetters().getUnchecked(method);
      }
    }
  }

  @Test
  public void testCreateJobWithScopedPermission() throws TException {
    JobConfiguration jobConfiguration = new JobConfiguration().setKey(JOB_KEY.newBuilder());
    Response response = Responses.ok();

    expect(subject
        .isPermitted(interceptor.makeTargetPermission("createJob", JOB_KEY)))
        .andReturn(true);
    expect(thrift.createJob(jobConfiguration)).andReturn(response);

    replayAndInitialize();

    assertSame(response, decoratedThrift.createJob(jobConfiguration));
  }

  @Test
  public void testKillTasksWithTargetedPermission() throws TException {
    expect(subject.isPermitted(interceptor.makeTargetPermission("killTasks", JOB_KEY)))
        .andReturn(false);
    expect(subject.getPrincipal()).andReturn("zmanji");

    replayAndInitialize();

    assertEquals(
        ResponseCode.AUTH_FAILED,
        decoratedThrift.killTasks(JOB_KEY.newBuilder(), null, null).getResponseCode());
  }

  @Test
  public void testKillTasksInvalidJobKey() throws TException {
    replayAndInitialize();

    assertEquals(
        ResponseCode.INVALID_REQUEST,
        decoratedThrift.killTasks(
            JOB_KEY.newBuilder().setName(null),
            null, null).getResponseCode());
  }

  @Test
  public void testHandlesMultipleAnnotations() {
    control.replay();

    Function<Object[], Optional<JobKey>> func =
        interceptor.getAuthorizingParamGetters().getUnchecked(Params.class.getMethods()[0]);

    func.apply(new Object[]{null, new JobKey(), null});
    func.apply(new Object[]{null, null, new JobUpdateRequest()});
  }

  @Test(expected = IllegalStateException.class)
  public void testThrowsOnMultipleNonNullArguments() {
    control.replay();

    Function<Object[], Optional<JobKey>> func =
        interceptor.getAuthorizingParamGetters().getUnchecked(Params.class.getMethods()[0]);

    func.apply(new Object[]{new JobConfiguration(), new JobKey(), null});
  }

  @Test(expected = UncheckedExecutionException.class)
     public void testThrowsNoAuthParams() {
    control.replay();

    interceptor.getAuthorizingParamGetters().getUnchecked(NoParams.class.getMethods()[0]);
  }

  @Test(expected = UncheckedExecutionException.class)
  public void testThrowsNoResponseReturned() {
    control.replay();

    interceptor.getAuthorizingParamGetters().getUnchecked(NoResponse.class.getMethods()[0]);
  }

  private interface NoResponse {
    void test(@AuthorizingParam TaskQuery query);
  }

  private interface NoParams {
    Response test(TaskQuery query);
  }

  private interface Params {
    Response test(
        @AuthorizingParam JobConfiguration jobConfig,
        @AuthorizingParam JobKey job,
        @AuthorizingParam JobUpdateRequest request);
  }
}
