package com.twitter.mesos.scheduler;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import com.twitter.mesos.gen.TwitterTaskInfo;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A decorating scheduling filter that applies a backoff to tasks.
 */
public class BackoffSchedulingFilter implements SchedulingFilter {

  @VisibleForTesting
  static final Set<Veto> BACKOFF_VETO =
      ImmutableSet.of(new Veto("Scheduler is backing off", Veto.MAX_SCORE));

  private final ScheduleBackoff backoff;
  private final SchedulingFilter delegate;

  @Inject
  BackoffSchedulingFilter(ScheduleBackoff backoff, @BackoffDelegate SchedulingFilter delegate) {
    this.backoff = checkNotNull(backoff);
    this.delegate = checkNotNull(delegate);
  }

  @Override
  public Set<Veto> filter(
      Resources offer,
      String slaveHost,
      TwitterTaskInfo task,
      String taskId) {

    return backoff.isSchedulable(task)
        ? delegate.filter(offer, slaveHost, task, taskId)
        : BACKOFF_VETO;
  }

  @BindingAnnotation
  @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME)
  public @interface BackoffDelegate { }
}
