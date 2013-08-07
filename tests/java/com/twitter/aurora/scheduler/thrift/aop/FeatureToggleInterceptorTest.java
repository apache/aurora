package com.twitter.aurora.scheduler.thrift.aop;

import java.lang.reflect.Method;

import com.google.common.base.Predicate;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.AuroraAdmin;
import com.twitter.aurora.gen.ResponseCode;
import com.twitter.aurora.gen.ScheduleStatusResponse;
import com.twitter.aurora.gen.TaskQuery;
import com.twitter.aurora.scheduler.thrift.auth.DecoratedThrift;
import com.twitter.common.testing.EasyMockTest;

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
    ScheduleStatusResponse response = new ScheduleStatusResponse()
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
