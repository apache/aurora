package com.twitter.aurora.scheduler.events;

import java.lang.reflect.Method;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import com.twitter.aurora.scheduler.events.PubsubEvent.Interceptors.Event;
import com.twitter.aurora.scheduler.events.PubsubEvent.Interceptors.SendNotification;
import com.twitter.common.base.Closure;

/**
 * A method interceptor that sends pubsub notifications before and/or after a method annotated
 * with {@link SendNotification}
 * is invoked.
 */
class NotifyingMethodInterceptor implements MethodInterceptor {
  private static final Logger LOG = Logger.getLogger(NotifyingMethodInterceptor.class.getName());

  private final Closure<PubsubEvent> eventSink;

  NotifyingMethodInterceptor(Closure<PubsubEvent> eventSink) {
    this.eventSink = Preconditions.checkNotNull(eventSink);
  }

  private void maybeFire(Event event) {
    if (event != Event.None) {
      eventSink.execute(event.getEvent());
    }
  }

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
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
