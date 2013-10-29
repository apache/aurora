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
package com.twitter.aurora.scheduler.storage.entities;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public final class IScheduledTask {
  private final ScheduledTask wrapped;
  private final IAssignedTask assignedTask;
  private final ImmutableList<ITaskEvent> taskEvents;

  private IScheduledTask(ScheduledTask wrapped) {
    this.wrapped = Preconditions.checkNotNull(wrapped);
    this.assignedTask = !wrapped.isSetAssignedTask()
        ? null
        : IAssignedTask.buildNoCopy(wrapped.getAssignedTask());
    this.taskEvents = !wrapped.isSetTaskEvents()
        ? ImmutableList.<ITaskEvent>of()
        : FluentIterable.from(wrapped.getTaskEvents())
              .transform(ITaskEvent.FROM_BUILDER)
              .toList();
  }

  static IScheduledTask buildNoCopy(ScheduledTask wrapped) {
    return new IScheduledTask(wrapped);
  }

  public static IScheduledTask build(ScheduledTask wrapped) {
    return buildNoCopy(wrapped.deepCopy());
  }

  public static final Function<IScheduledTask, ScheduledTask> TO_BUILDER =
      new Function<IScheduledTask, ScheduledTask>() {
        @Override
        public ScheduledTask apply(IScheduledTask input) {
          return input.newBuilder();
        }
      };

  public static final Function<ScheduledTask, IScheduledTask> FROM_BUILDER =
      new Function<ScheduledTask, IScheduledTask>() {
        @Override
        public IScheduledTask apply(ScheduledTask input) {
          return new IScheduledTask(input);
        }
      };

  public static ImmutableList<ScheduledTask> toBuildersList(Iterable<IScheduledTask> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toList();
  }

  public static ImmutableList<IScheduledTask> listFromBuilders(Iterable<ScheduledTask> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toList();
  }

  public static ImmutableSet<ScheduledTask> toBuildersSet(Iterable<IScheduledTask> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toSet();
  }

  public static ImmutableSet<IScheduledTask> setFromBuilders(Iterable<ScheduledTask> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toSet();
  }

  public ScheduledTask newBuilder() {
    return wrapped.deepCopy();
  }

  public boolean isSetAssignedTask() {
    return wrapped.isSetAssignedTask();
  }

  public IAssignedTask getAssignedTask() {
    return assignedTask;
  }

  public ScheduleStatus getStatus() {
    return wrapped.getStatus();
  }

  public boolean isSetFailureCount() {
    return wrapped.isSetFailureCount();
  }

  public int getFailureCount() {
    return wrapped.getFailureCount();
  }

  public boolean isSetTaskEvents() {
    return wrapped.isSetTaskEvents();
  }

  public List<ITaskEvent> getTaskEvents() {
    return taskEvents;
  }

  public boolean isSetAncestorId() {
    return wrapped.isSetAncestorId();
  }

  public String getAncestorId() {
    return wrapped.getAncestorId();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IScheduledTask)) {
      return false;
    }
    IScheduledTask other = (IScheduledTask) o;
    return wrapped.equals(other.wrapped);
  }

  @Override
  public int hashCode() {
    return wrapped.hashCode();
  }

  @Override
  public String toString() {
    return wrapped.toString();
  }
}
