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

import com.twitter.aurora.gen.Lock;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public final class ILock {
  private final Lock wrapped;
  private final ILockKey key;

  private ILock(Lock wrapped) {
    this.wrapped = Preconditions.checkNotNull(wrapped);
    this.key = !wrapped.isSetKey()
        ? null
        : ILockKey.buildNoCopy(wrapped.getKey());
  }

  static ILock buildNoCopy(Lock wrapped) {
    return new ILock(wrapped);
  }

  public static ILock build(Lock wrapped) {
    return buildNoCopy(wrapped.deepCopy());
  }

  public static final Function<ILock, Lock> TO_BUILDER =
      new Function<ILock, Lock>() {
        @Override
        public Lock apply(ILock input) {
          return input.newBuilder();
        }
      };

  public static final Function<Lock, ILock> FROM_BUILDER =
      new Function<Lock, ILock>() {
        @Override
        public ILock apply(Lock input) {
          return new ILock(input);
        }
      };

  public static ImmutableList<Lock> toBuildersList(Iterable<ILock> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toList();
  }

  public static ImmutableList<ILock> listFromBuilders(Iterable<Lock> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toList();
  }

  public static ImmutableSet<Lock> toBuildersSet(Iterable<ILock> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toSet();
  }

  public static ImmutableSet<ILock> setFromBuilders(Iterable<Lock> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toSet();
  }

  public Lock newBuilder() {
    return wrapped.deepCopy();
  }

  public boolean isSetKey() {
    return wrapped.isSetKey();
  }

  public ILockKey getKey() {
    return key;
  }

  public boolean isSetToken() {
    return wrapped.isSetToken();
  }

  public String getToken() {
    return wrapped.getToken();
  }

  public boolean isSetUser() {
    return wrapped.isSetUser();
  }

  public String getUser() {
    return wrapped.getUser();
  }

  public boolean isSetTimestampMs() {
    return wrapped.isSetTimestampMs();
  }

  public long getTimestampMs() {
    return wrapped.getTimestampMs();
  }

  public boolean isSetMessage() {
    return wrapped.isSetMessage();
  }

  public String getMessage() {
    return wrapped.getMessage();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ILock)) {
      return false;
    }
    ILock other = (ILock) o;
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
