package com.twitter.mesos.scheduler.events;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.matcher.Matchers;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.scheduler.events.PubsubEvent.DriverRegistered;
import com.twitter.mesos.scheduler.events.PubsubEvent.Interceptors.Event;
import com.twitter.mesos.scheduler.events.PubsubEvent.Interceptors.SendNotification;
import com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;

import static org.junit.Assert.assertEquals;

public class NotifyingMethodInterceptorTest extends EasyMockTest {

  private Closure<PubsubEvent> eventSink;
  private NotifyingMethodInterceptor interceptor;

  @Before
  public void setUp() throws Exception {
    eventSink = createMock(new Clazz<Closure<PubsubEvent>>() { });
    interceptor = new NotifyingMethodInterceptor(eventSink);
  }

  @Test
  public void testNotifications() {
    eventSink.execute(new DriverRegistered());
    eventSink.execute(new StorageStarted());
    eventSink.execute(new DriverRegistered());

    control.replay();

    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override protected void configure() {
        bind(Math.class).in(Singleton.class);
        bindInterceptor(
            Matchers.any(),
            Matchers.annotatedWith(SendNotification.class),
            interceptor);
      }
    });

    Math math = injector.getInstance(Math.class);
    assertEquals(4, math.add(2, 2));
    assertEquals(0, math.subtract(2, 2));
    assertEquals(4, math.multiply(2, 2));
    assertEquals(1, math.divide(2, 2));
  }

  static class Math {
    @SendNotification(before = Event.DriverRegistered, after = Event.StorageStarted)
    int add(int a, int b) {
      return a + b;
    }

    @SendNotification(after = Event.DriverRegistered)
    int subtract(int a, int b) {
      return a - b;
    }

    @SendNotification
    int multiply(int a, int b) {
      return a * b;
    }

    int divide(int a, int b) {
      return a / b;
    }
  }
}
