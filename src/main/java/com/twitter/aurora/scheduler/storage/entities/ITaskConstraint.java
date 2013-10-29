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
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.twitter.aurora.gen.TaskConstraint;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public final class ITaskConstraint {
  private final TaskConstraint wrapped;

  private ITaskConstraint(TaskConstraint wrapped) {
    this.wrapped = Preconditions.checkNotNull(wrapped);
  }

  static ITaskConstraint buildNoCopy(TaskConstraint wrapped) {
    return new ITaskConstraint(wrapped);
  }

  public static ITaskConstraint build(TaskConstraint wrapped) {
    return buildNoCopy(wrapped.deepCopy());
  }

  public static final Function<ITaskConstraint, TaskConstraint> TO_BUILDER =
      new Function<ITaskConstraint, TaskConstraint>() {
        @Override
        public TaskConstraint apply(ITaskConstraint input) {
          return input.newBuilder();
        }
      };

  public static final Function<TaskConstraint, ITaskConstraint> FROM_BUILDER =
      new Function<TaskConstraint, ITaskConstraint>() {
        @Override
        public ITaskConstraint apply(TaskConstraint input) {
          return new ITaskConstraint(input);
        }
      };

  public static ImmutableList<TaskConstraint> toBuildersList(Iterable<ITaskConstraint> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toList();
  }

  public static ImmutableList<ITaskConstraint> listFromBuilders(Iterable<TaskConstraint> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toList();
  }

  public static ImmutableSet<TaskConstraint> toBuildersSet(Iterable<ITaskConstraint> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toSet();
  }

  public static ImmutableSet<ITaskConstraint> setFromBuilders(Iterable<TaskConstraint> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toSet();
  }

  public TaskConstraint newBuilder() {
    return wrapped.deepCopy();
  }

  public TaskConstraint._Fields getSetField() {
    return wrapped.getSetField();
  }

  public IValueConstraint getValue() {
    return IValueConstraint.build(wrapped.getValue());
  }

  public ILimitConstraint getLimit() {
    return ILimitConstraint.build(wrapped.getLimit());
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ITaskConstraint)) {
      return false;
    }
    ITaskConstraint other = (ITaskConstraint) o;
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
