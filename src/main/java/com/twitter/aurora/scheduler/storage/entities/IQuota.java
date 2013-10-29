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

import com.twitter.aurora.gen.Quota;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public final class IQuota {
  private final Quota wrapped;

  private IQuota(Quota wrapped) {
    this.wrapped = Preconditions.checkNotNull(wrapped);
  }

  static IQuota buildNoCopy(Quota wrapped) {
    return new IQuota(wrapped);
  }

  public static IQuota build(Quota wrapped) {
    return buildNoCopy(wrapped.deepCopy());
  }

  public static final Function<IQuota, Quota> TO_BUILDER =
      new Function<IQuota, Quota>() {
        @Override
        public Quota apply(IQuota input) {
          return input.newBuilder();
        }
      };

  public static final Function<Quota, IQuota> FROM_BUILDER =
      new Function<Quota, IQuota>() {
        @Override
        public IQuota apply(Quota input) {
          return new IQuota(input);
        }
      };

  public static ImmutableList<Quota> toBuildersList(Iterable<IQuota> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toList();
  }

  public static ImmutableList<IQuota> listFromBuilders(Iterable<Quota> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toList();
  }

  public static ImmutableSet<Quota> toBuildersSet(Iterable<IQuota> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toSet();
  }

  public static ImmutableSet<IQuota> setFromBuilders(Iterable<Quota> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toSet();
  }

  public Quota newBuilder() {
    return wrapped.deepCopy();
  }

  public boolean isSetNumCpus() {
    return wrapped.isSetNumCpus();
  }

  public double getNumCpus() {
    return wrapped.getNumCpus();
  }

  public boolean isSetRamMb() {
    return wrapped.isSetRamMb();
  }

  public long getRamMb() {
    return wrapped.getRamMb();
  }

  public boolean isSetDiskMb() {
    return wrapped.isSetDiskMb();
  }

  public long getDiskMb() {
    return wrapped.getDiskMb();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IQuota)) {
      return false;
    }
    IQuota other = (IQuota) o;
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
