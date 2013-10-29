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
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.twitter.aurora.gen.AssignedTask;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public final class IAssignedTask {
  private final AssignedTask wrapped;
  private final ITaskConfig task;
  private final ImmutableMap<String, Integer> assignedPorts;

  private IAssignedTask(AssignedTask wrapped) {
    this.wrapped = Preconditions.checkNotNull(wrapped);
    this.task = !wrapped.isSetTask()
        ? null
        : ITaskConfig.buildNoCopy(wrapped.getTask());
    this.assignedPorts = !wrapped.isSetAssignedPorts()
        ? ImmutableMap.<String, Integer>of()
        : ImmutableMap.copyOf(wrapped.getAssignedPorts());
  }

  static IAssignedTask buildNoCopy(AssignedTask wrapped) {
    return new IAssignedTask(wrapped);
  }

  public static IAssignedTask build(AssignedTask wrapped) {
    return buildNoCopy(wrapped.deepCopy());
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

  public static ImmutableList<AssignedTask> toBuildersList(Iterable<IAssignedTask> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toList();
  }

  public static ImmutableList<IAssignedTask> listFromBuilders(Iterable<AssignedTask> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toList();
  }

  public static ImmutableSet<AssignedTask> toBuildersSet(Iterable<IAssignedTask> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toSet();
  }

  public static ImmutableSet<IAssignedTask> setFromBuilders(Iterable<AssignedTask> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toSet();
  }

  public AssignedTask newBuilder() {
    return wrapped.deepCopy();
  }

  public boolean isSetTaskId() {
    return wrapped.isSetTaskId();
  }

  public String getTaskId() {
    return wrapped.getTaskId();
  }

  public boolean isSetSlaveId() {
    return wrapped.isSetSlaveId();
  }

  public String getSlaveId() {
    return wrapped.getSlaveId();
  }

  public boolean isSetSlaveHost() {
    return wrapped.isSetSlaveHost();
  }

  public String getSlaveHost() {
    return wrapped.getSlaveHost();
  }

  public boolean isSetTask() {
    return wrapped.isSetTask();
  }

  public ITaskConfig getTask() {
    return task;
  }

  public boolean isSetAssignedPorts() {
    return wrapped.isSetAssignedPorts();
  }

  public Map<String, Integer> getAssignedPorts() {
    return assignedPorts;
  }

  public boolean isSetInstanceId() {
    return wrapped.isSetInstanceId();
  }

  public int getInstanceId() {
    return wrapped.getInstanceId();
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
