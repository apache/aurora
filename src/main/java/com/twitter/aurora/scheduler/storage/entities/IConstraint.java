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

import com.twitter.aurora.gen.Constraint;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public final class IConstraint {
  private final Constraint wrapped;
  private final ITaskConstraint constraint;

  private IConstraint(Constraint wrapped) {
    this.wrapped = Preconditions.checkNotNull(wrapped);
    this.constraint = !wrapped.isSetConstraint()
        ? null
        : ITaskConstraint.buildNoCopy(wrapped.getConstraint());
  }

  static IConstraint buildNoCopy(Constraint wrapped) {
    return new IConstraint(wrapped);
  }

  public static IConstraint build(Constraint wrapped) {
    return buildNoCopy(wrapped.deepCopy());
  }

  public static final Function<IConstraint, Constraint> TO_BUILDER =
      new Function<IConstraint, Constraint>() {
        @Override
        public Constraint apply(IConstraint input) {
          return input.newBuilder();
        }
      };

  public static final Function<Constraint, IConstraint> FROM_BUILDER =
      new Function<Constraint, IConstraint>() {
        @Override
        public IConstraint apply(Constraint input) {
          return new IConstraint(input);
        }
      };

  public static ImmutableList<Constraint> toBuildersList(Iterable<IConstraint> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toList();
  }

  public static ImmutableList<IConstraint> listFromBuilders(Iterable<Constraint> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toList();
  }

  public static ImmutableSet<Constraint> toBuildersSet(Iterable<IConstraint> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toSet();
  }

  public static ImmutableSet<IConstraint> setFromBuilders(Iterable<Constraint> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toSet();
  }

  public Constraint newBuilder() {
    return wrapped.deepCopy();
  }

  public boolean isSetName() {
    return wrapped.isSetName();
  }

  public String getName() {
    return wrapped.getName();
  }

  public boolean isSetConstraint() {
    return wrapped.isSetConstraint();
  }

  public ITaskConstraint getConstraint() {
    return constraint;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IConstraint)) {
      return false;
    }
    IConstraint other = (IConstraint) o;
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
