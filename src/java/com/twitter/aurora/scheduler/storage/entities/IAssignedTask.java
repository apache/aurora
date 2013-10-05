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

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

import com.twitter.aurora.gen.AssignedTask;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public class IAssignedTask {
  private final AssignedTask wrapped;

  public IAssignedTask(AssignedTask wrapped) {
    this.wrapped = wrapped.deepCopy();
  }

  public static final Function<IAssignedTask, AssignedTask> TO_BUILDER =
      new Function<IAssignedTask, AssignedTask>() {
        @Override
        public AssignedTask apply(IAssignedTask input) {
          return input.newBuilder();
        }
      };

  public static final Function<AssignedTask, IAssignedTask> FROM_BUILDER =
      new Function<AssignedTask, IAssignedTask>() {
        @Override
        public IAssignedTask apply(AssignedTask input) {
          return new IAssignedTask(input);
        }
      };

  public AssignedTask newBuilder() {
    return wrapped.deepCopy();
  }

  public String getTaskId() {
    return wrapped.getTaskId();
  }

  public String getSlaveId() {
    return wrapped.getSlaveId();
  }

  public String getSlaveHost() {
    return wrapped.getSlaveHost();
  }

  public ITaskConfig getTask() {
    return new ITaskConfig(wrapped.getTask());
  }

  public Map<String, Integer> getAssignedPorts() {
    return ImmutableMap.copyOf(wrapped.getAssignedPorts());
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IAssignedTask)) {
      return false;
    }
    IAssignedTask other = (IAssignedTask) o;
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
