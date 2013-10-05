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

import com.google.common.base.Function;

import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.TaskEvent;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public class ITaskEvent {
  private final TaskEvent wrapped;

  public ITaskEvent(TaskEvent wrapped) {
    this.wrapped = wrapped.deepCopy();
  }

  public static final Function<ITaskEvent, TaskEvent> TO_BUILDER =
      new Function<ITaskEvent, TaskEvent>() {
        @Override
        public TaskEvent apply(ITaskEvent input) {
          return input.newBuilder();
        }
      };

  public static final Function<TaskEvent, ITaskEvent> FROM_BUILDER =
      new Function<TaskEvent, ITaskEvent>() {
        @Override
        public ITaskEvent apply(TaskEvent input) {
          return new ITaskEvent(input);
        }
      };

  public TaskEvent newBuilder() {
    return wrapped.deepCopy();
  }

  public long getTimestamp() {
    return wrapped.getTimestamp();
  }

  public ScheduleStatus getStatus() {
    return wrapped.getStatus();
  }

  public String getMessage() {
    return wrapped.getMessage();
  }

  public String getScheduler() {
    return wrapped.getScheduler();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ITaskEvent)) {
      return false;
    }
    ITaskEvent other = (ITaskEvent) o;
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
