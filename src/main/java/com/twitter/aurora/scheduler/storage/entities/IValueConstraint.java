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

import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.twitter.aurora.gen.ValueConstraint;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public final class IValueConstraint {
  private final ValueConstraint wrapped;
  private final ImmutableSet<String> values;

  private IValueConstraint(ValueConstraint wrapped) {
    this.wrapped = Preconditions.checkNotNull(wrapped);
    this.values = !wrapped.isSetValues()
        ? ImmutableSet.<String>of()
        : ImmutableSet.copyOf(wrapped.getValues());
  }

  static IValueConstraint buildNoCopy(ValueConstraint wrapped) {
    return new IValueConstraint(wrapped);
  }

  public static IValueConstraint build(ValueConstraint wrapped) {
    return buildNoCopy(wrapped.deepCopy());
  }

  public static final Function<IValueConstraint, ValueConstraint> TO_BUILDER =
      new Function<IValueConstraint, ValueConstraint>() {
        @Override
        public ValueConstraint apply(IValueConstraint input) {
          return input.newBuilder();
        }
      };

  public static final Function<ValueConstraint, IValueConstraint> FROM_BUILDER =
      new Function<ValueConstraint, IValueConstraint>() {
        @Override
        public IValueConstraint apply(ValueConstraint input) {
          return new IValueConstraint(input);
        }
      };

  public static ImmutableList<ValueConstraint> toBuildersList(Iterable<IValueConstraint> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toList();
  }

  public static ImmutableList<IValueConstraint> listFromBuilders(Iterable<ValueConstraint> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toList();
  }

  public static ImmutableSet<ValueConstraint> toBuildersSet(Iterable<IValueConstraint> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toSet();
  }

  public static ImmutableSet<IValueConstraint> setFromBuilders(Iterable<ValueConstraint> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toSet();
  }

  public ValueConstraint newBuilder() {
    return wrapped.deepCopy();
  }

  public boolean isSetNegated() {
    return wrapped.isSetNegated();
  }

  public boolean isNegated() {
    return wrapped.isNegated();
  }

  public boolean isSetValues() {
    return wrapped.isSetValues();
  }

  public Set<String> getValues() {
    return values;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IValueConstraint)) {
      return false;
    }
    IValueConstraint other = (IValueConstraint) o;
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
