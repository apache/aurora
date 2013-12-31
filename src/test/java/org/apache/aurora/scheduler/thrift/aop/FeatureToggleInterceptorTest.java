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
package org.apache.aurora.scheduler.thrift.aop;

import java.lang.reflect.Method;

import com.google.common.base.Predicate;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;

import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.thrift.auth.DecoratedThrift;

import org.easymock.EasyMock;

import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;

import static org.junit.Assert.assertSame;

public class FeatureToggleInterceptorTest extends EasyMockTest {

  private AuroraAdmin.Iface realThrift;
  private AuroraAdmin.Iface decoratedThrift;
  private Predicate<Method> predicate;

  @Before
  public void setUp() {
    realThrift = createMock(AuroraAdmin.Iface.class);
    predicate = createMock(new Clazz<Predicate<Method>>() { });
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override protected void configure() {
        MockDecoratedThrift.bindForwardedMock(binder(), realThrift);
        bind(new TypeLiteral<Predicate<Method>>() { }).toInstance(predicate);
        AopModule.bindThriftDecorator(
            binder(),
            Matchers.annotatedWith(DecoratedThrift.class),
            new FeatureToggleInterceptor());
      }
    });
    decoratedThrift = injector.getInstance(AuroraAdmin.Iface.class);
  }

  @Test
  public void testPredicatePass() throws Exception {
    TaskQuery query = new TaskQuery();
    Response response = new Response()
        .setResponseCode(ResponseCode.OK);

    expect(predicate.apply(EasyMock.<Method>anyObject())).andReturn(true);
    expect(realThrift.getTasksStatus(query)).andReturn(response);

    control.replay();

    assertSame(response, decoratedThrift.getTasksStatus(query));
  }

  @Test
  public void testPredicateDeny() throws Exception {
    TaskQuery query = new TaskQuery();
    expect(predicate.apply(EasyMock.<Method>anyObject())).andReturn(false);

    control.replay();

    assertSame(ResponseCode.ERROR, decoratedThrift.getTasksStatus(query).getResponseCode());
  }
}
