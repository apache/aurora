/**
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

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Qualifier;

import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.events.PubsubEvent.Vetoed;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

/**
 * A decorating scheduling filter that sends an event when a scheduling assignment is vetoed.
 */
class NotifyingSchedulingFilter implements SchedulingFilter {

  /**
   * Binding annotation that the underlying {@link SchedulingFilter} must be bound with.
   */
  @Qualifier
  @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME)
  public @interface NotifyDelegate { }

  private final SchedulingFilter delegate;
  private final EventSink eventSink;

  @Inject
  NotifyingSchedulingFilter(
      @NotifyDelegate SchedulingFilter delegate,
      EventSink eventSink) {

    this.delegate = requireNonNull(delegate);
    this.eventSink = requireNonNull(eventSink);
  }

  @Override
  public Set<Veto> filter(
      ResourceSlot offer,
      String slaveHost,
      MaintenanceMode mode,
      ITaskConfig task,
      String taskId,
      AttributeAggregate jobState) {

    Set<Veto> vetoes = delegate.filter(offer, slaveHost, mode, task, taskId, jobState);
    if (!vetoes.isEmpty()) {
      eventSink.post(new Vetoed(taskId, vetoes));
    }

    return vetoes;
  }
}
