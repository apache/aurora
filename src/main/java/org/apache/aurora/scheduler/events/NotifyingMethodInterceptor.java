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
package org.apache.aurora.scheduler.events;

import java.lang.reflect.Method;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.base.Preconditions;

import com.twitter.common.base.Closure;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import org.apache.aurora.scheduler.events.PubsubEvent.Interceptors.Event;
import org.apache.aurora.scheduler.events.PubsubEvent.Interceptors.SendNotification;

/**
 * A method interceptor that sends pubsub notifications before and/or after a method annotated
 * with {@link SendNotification}
 * is invoked.
 */
class NotifyingMethodInterceptor implements MethodInterceptor {
  private static final Logger LOG = Logger.getLogger(NotifyingMethodInterceptor.class.getName());

  @Inject
  private Closure<PubsubEvent> eventSink;

  private void maybeFire(Event event) {
    if (event != Event.None) {
      eventSink.execute(event.getEvent());
    }
  }

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    Preconditions.checkNotNull(eventSink, "Event sink has not yet been set.");

    Method method = invocation.getMethod();
    SendNotification sendNotification = method.getAnnotation(SendNotification.class);
    if (sendNotification == null) {
      LOG.warning("Interceptor should not match methods without @"
          + SendNotification.class.getSimpleName());
      return invocation.proceed();
    }

    maybeFire(sendNotification.before());
    Object result = invocation.proceed();
    maybeFire(sendNotification.after());
    return result;
  }
}
