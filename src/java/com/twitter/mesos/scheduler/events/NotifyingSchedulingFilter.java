package com.twitter.mesos.scheduler.events;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Set;

import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import com.twitter.common.base.Closure;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.configuration.Resources;
import com.twitter.mesos.scheduler.events.PubsubEvent.Vetoed;
import com.twitter.mesos.scheduler.filter.SchedulingFilter;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A decorating scheduling filter that sends an event when a scheduling assignment is vetoed.
 */
class NotifyingSchedulingFilter implements SchedulingFilter {

  /**
   * Binding annotation that the underlying {@link SchedulingFilter} must be bound with.
   */
  @BindingAnnotation
  @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME)
  public @interface NotifyDelegate { }

  private final SchedulingFilter delegate;
  private final Closure<PubsubEvent> eventSink;

  @Inject
  NotifyingSchedulingFilter(
      @NotifyDelegate SchedulingFilter delegate,
      Closure<PubsubEvent> eventSink) {

    this.delegate = checkNotNull(delegate);
    this.eventSink = checkNotNull(eventSink);
  }

  @Override
  public Set<Veto> filter(Resources offer, String slaveHost, TwitterTaskInfo task, String taskId) {
    Set<Veto> vetoes = delegate.filter(offer, slaveHost, task, taskId);
    if (!vetoes.isEmpty()) {
      eventSink.execute(new Vetoed(taskId, vetoes));
    }

    return vetoes;
  }
}
