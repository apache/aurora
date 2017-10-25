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

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.scheduler.TaskVars;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.metadata.NearestFit;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

/**
 * A decorating scheduling filter that notifies metadata classes when a scheduling assignment is
 * vetoed.
 */
public class NotifyingSchedulingFilter implements SchedulingFilter {

  /**
   * Binding annotation that the underlying {@link SchedulingFilter} must be bound with.
   */
  @Qualifier
  @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME)
  public @interface NotifyDelegate { }

  private final SchedulingFilter delegate;
  private final NearestFit nearestFit;
  private final TaskVars taskVars;

  @Inject
  NotifyingSchedulingFilter(
      @NotifyDelegate SchedulingFilter delegate,
      NearestFit nearestFit,
      TaskVars taskVars) {

    this.delegate = requireNonNull(delegate);
    this.nearestFit = requireNonNull(nearestFit);
    this.taskVars = requireNonNull(taskVars);
  }

  @Timed("notifying_scheduling_filter")
  @Override
  public Set<Veto> filter(UnusedResource resource, ResourceRequest request) {
    Set<Veto> vetoes = delegate.filter(resource, request);
    if (!vetoes.isEmpty()) {
      nearestFit.vetoed(TaskGroupKey.from(request.getTask()), vetoes);
      taskVars.taskVetoed(vetoes);
    }

    return vetoes;
  }
}
