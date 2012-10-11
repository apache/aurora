package com.twitter.mesos.scheduler.events;

import java.lang.reflect.Method;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import com.twitter.common.base.Closure;
import com.twitter.mesos.scheduler.events.PubsubEvent.Interceptors.Event;
import com.twitter.mesos.scheduler.events.PubsubEvent.Interceptors.Notify;

/**
 * A method interceptor that sends pubsub notifications before and/or after a method annotated
 * with {@link Notify} is invoked.
 */
class NotifyingMethodInterceptor implements MethodInterceptor {
  private static final Logger LOG = Logger.getLogger(NotifyingMethodInterceptor.class.getName());

  private final Closure<PubsubEvent> eventSink;

  NotifyingMethodInterceptor(Closure<PubsubEvent> eventSink) {
    this.eventSink = Preconditions.checkNotNull(eventSink);
  }

  private void maybeFire(Event event) {
    if (event != Event.None) {
      eventSink.execute(event.event);
    }
  }

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    Method method = invocation.getMethod();
    Notify notify = method.getAnnotation(Notify.class);
    if (notify == null) {
      LOG.warning("Interceptor should not match methods without @Notify");
      return invocation.proceed();
    }

    maybeFire(notify.before());
    Object result = invocation.proceed();
    maybeFire(notify.after());
    return result;
  }
}
