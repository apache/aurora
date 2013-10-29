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

import com.twitter.aurora.gen.LockKey;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public final class ILockKey {
  private final LockKey wrapped;

  private ILockKey(LockKey wrapped) {
    this.wrapped = Preconditions.checkNotNull(wrapped);
  }

  static ILockKey buildNoCopy(LockKey wrapped) {
    return new ILockKey(wrapped);
  }

  public static ILockKey build(LockKey wrapped) {
    return buildNoCopy(wrapped.deepCopy());
  }

  public static final Function<ILockKey, LockKey> TO_BUILDER =
      new Function<ILockKey, LockKey>() {
        @Override
        public LockKey apply(ILockKey input) {
          return input.newBuilder();
        }
      };

  public static final Function<LockKey, ILockKey> FROM_BUILDER =
      new Function<LockKey, ILockKey>() {
        @Override
        public ILockKey apply(LockKey input) {
          return new ILockKey(input);
        }
      };

  public static ImmutableList<LockKey> toBuildersList(Iterable<ILockKey> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toList();
  }

  public static ImmutableList<ILockKey> listFromBuilders(Iterable<LockKey> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toList();
  }

  public static ImmutableSet<LockKey> toBuildersSet(Iterable<ILockKey> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toSet();
  }

  public static ImmutableSet<ILockKey> setFromBuilders(Iterable<LockKey> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toSet();
  }

  public LockKey newBuilder() {
    return wrapped.deepCopy();
  }

  public LockKey._Fields getSetField() {
    return wrapped.getSetField();
  }

  public IJobKey getJob() {
    return IJobKey.build(wrapped.getJob());
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ILockKey)) {
      return false;
    }
    ILockKey other = (ILockKey) o;
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
