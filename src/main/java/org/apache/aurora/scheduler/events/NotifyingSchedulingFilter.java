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

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Set;

import javax.inject.Inject;

import com.google.inject.BindingAnnotation;

import com.twitter.aurora.scheduler.ResourceSlot;
import com.twitter.aurora.scheduler.events.PubsubEvent.Vetoed;
import com.twitter.aurora.scheduler.filter.SchedulingFilter;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;
import com.twitter.common.base.Closure;

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
  public Set<Veto> filter(ResourceSlot offer, String slaveHost, ITaskConfig task, String taskId) {
    Set<Veto> vetoes = delegate.filter(offer, slaveHost, task, taskId);
    if (!vetoes.isEmpty()) {
      eventSink.execute(new Vetoed(taskId, vetoes));
    }

    return vetoes;
  }
}
