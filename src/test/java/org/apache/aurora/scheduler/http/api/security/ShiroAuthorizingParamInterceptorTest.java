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

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.matcher.Matchers;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.spi.Permissions.Domain;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.thrift.Responses;
import org.apache.aurora.scheduler.thrift.aop.AnnotatedAuroraAdmin;
import org.apache.aurora.scheduler.thrift.aop.MockDecoratedThrift;
import org.apache.shiro.subject.Subject;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.http.api.security.ShiroAuthorizingParamInterceptor.QUERY_TO_JOB_KEY;
import static org.apache.aurora.scheduler.http.api.security.ShiroAuthorizingParamInterceptor.SHIRO_AUTHORIZATION_FAILURES;
import static org.apache.aurora.scheduler.http.api.security.ShiroAuthorizingParamInterceptor.SHIRO_BAD_REQUESTS;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class ShiroAuthorizingParamInterceptorTest extends EasyMockTest {
  private static final Domain DOMAIN = Domain.THRIFT_AURORA_SCHEDULER_MANAGER;

  private ShiroAuthorizingParamInterceptor interceptor;

  private Subject subject;
  private AnnotatedAuroraAdmin thrift;
  private StatsProvider statsProvider;

  private AnnotatedAuroraAdmin decoratedThrift;

  private static final IJobKey JOB_KEY = JobKeys.from("role", "env", "name");

  @Before
  public void setUp() {
    interceptor = new ShiroAuthorizingParamInterceptor(DOMAIN);
    subject = createMock(Subject.class);
    statsProvider = createMock(StatsProvider.class);
    thrift = createMock(AnnotatedAuroraAdmin.class);
  };

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

    expect(subject.isPermitted(interceptor.makeWildcardPermission("createJob")))
        .andReturn(false);
    expect(subject
        .isPermitted(interceptor.makeTargetPermission("createJob", JOB_KEY)))
        .andReturn(true);
    expect(thrift.createJob(jobConfiguration, null, null))
        .andReturn(response);

    replayAndInitialize();

    assertSame(response, decoratedThrift.createJob(jobConfiguration, null, null));
  }

  @Test
  public void testKillTasksWithWildcardPermission() throws TException {
    TaskQuery taskQuery = Query.unscoped().get();
    Response response = Responses.ok();

    expect(subject.isPermitted(interceptor.makeWildcardPermission("killTasks")))
        .andReturn(true);
    expect(thrift.killTasks(taskQuery, null, null))
        .andReturn(response);

    replayAndInitialize();

    assertSame(response, decoratedThrift.killTasks(taskQuery, null, null));
  }

  @Test
  public void testKillTasksWithoutWildcardPermission() throws TException {
    TaskQuery taskQuery = Query.unscoped().get();

    expect(subject.isPermitted(interceptor.makeWildcardPermission("killTasks")))
        .andReturn(false);

    replayAndInitialize();

    assertEquals(
        ResponseCode.INVALID_REQUEST,
        decoratedThrift.killTasks(taskQuery, null, null).getResponseCode());
  }

  @Test
  public void testExtractTaskQuerySingleJobKey() {
    replayAndInitialize();

    assertEquals(
        JOB_KEY.newBuilder(),
        QUERY_TO_JOB_KEY
            .apply(new TaskQuery()
                .setRole(JOB_KEY.getRole())
                .setEnvironment(JOB_KEY.getEnvironment())
                .setJobName(JOB_KEY.getName()))
            .orNull());

    assertEquals(
        JOB_KEY.newBuilder(),
        QUERY_TO_JOB_KEY.apply(new TaskQuery().setJobKeys(ImmutableSet.of(JOB_KEY.newBuilder())))
            .orNull());
  }

  @Test
  public void testExtractTaskQueryBroadlyScoped() {
    control.replay();

    assertNull(QUERY_TO_JOB_KEY.apply(new TaskQuery().setRole("role")).orNull());
  }

  @Test
  public void testExtractTaskQueryMultiScoped() {
    // TODO(ksweeney): Reconsider behavior here, this is possibly too restrictive as it
    // will mean that only admins are authorized to operate on multiple jobs at once regardless
    // of whether they share a common role.
    control.replay();

    assertNull(QUERY_TO_JOB_KEY
        .apply(
            new TaskQuery().setJobKeys(
                ImmutableSet.of(JOB_KEY.newBuilder(), JOB_KEY.newBuilder().setName("other"))))
        .orNull());
  }
}
