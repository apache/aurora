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
import com.google.common.collect.FluentIterable;

import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public class IScheduledTask {
  private final ScheduledTask wrapped;

  public IScheduledTask(ScheduledTask wrapped) {
    this.wrapped = wrapped.deepCopy();
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

  public ScheduledTask newBuilder() {
    return wrapped.deepCopy();
  }

  public IAssignedTask getAssignedTask() {
    return new IAssignedTask(wrapped.getAssignedTask());
  }

  public ScheduleStatus getStatus() {
    return wrapped.getStatus();
  }

  public int getFailureCount() {
    return wrapped.getFailureCount();
  }

  public List<ITaskEvent> getTaskEvents() {
    return FluentIterable
        .from(wrapped.getTaskEvents())
        .transform(ITaskEvent.FROM_BUILDER)
        .toList();
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
