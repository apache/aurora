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
package com.twitter.aurora.scheduler.events;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.scheduler.events.PubsubEvent.DriverRegistered;
import com.twitter.aurora.scheduler.events.PubsubEvent.Interceptors.Event;
import com.twitter.aurora.scheduler.events.PubsubEvent.Interceptors.SendNotification;
import com.twitter.aurora.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.common.base.Closure;
import com.twitter.common.testing.easymock.EasyMockTest;

import static org.junit.Assert.assertEquals;

public class NotifyingMethodInterceptorTest extends EasyMockTest {

  private Closure<PubsubEvent> eventSink;
  private NotifyingMethodInterceptor interceptor;

  @Before
  public void setUp() throws Exception {
    eventSink = createMock(new Clazz<Closure<PubsubEvent>>() { });
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override protected void configure() {
        bind(new TypeLiteral<Closure<PubsubEvent>>() { }).toInstance(eventSink);
        NotifyingMethodInterceptor bound = new NotifyingMethodInterceptor();
        bind(NotifyingMethodInterceptor.class).toInstance(bound);
        requestInjection(bound);
      }
    });
    interceptor = injector.getInstance(NotifyingMethodInterceptor.class);
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
