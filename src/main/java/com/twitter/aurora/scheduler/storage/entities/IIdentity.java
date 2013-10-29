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

import com.twitter.aurora.gen.Identity;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public final class IIdentity {
  private final Identity wrapped;

  private IIdentity(Identity wrapped) {
    this.wrapped = Preconditions.checkNotNull(wrapped);
  }

  static IIdentity buildNoCopy(Identity wrapped) {
    return new IIdentity(wrapped);
  }

  public static IIdentity build(Identity wrapped) {
    return buildNoCopy(wrapped.deepCopy());
  }

  public static final Function<IIdentity, Identity> TO_BUILDER =
      new Function<IIdentity, Identity>() {
        @Override
        public Identity apply(IIdentity input) {
          return input.newBuilder();
        }
      };

  public static final Function<Identity, IIdentity> FROM_BUILDER =
      new Function<Identity, IIdentity>() {
        @Override
        public IIdentity apply(Identity input) {
          return new IIdentity(input);
        }
      };

  public static ImmutableList<Identity> toBuildersList(Iterable<IIdentity> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toList();
  }

  public static ImmutableList<IIdentity> listFromBuilders(Iterable<Identity> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toList();
  }

  public static ImmutableSet<Identity> toBuildersSet(Iterable<IIdentity> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toSet();
  }

  public static ImmutableSet<IIdentity> setFromBuilders(Iterable<Identity> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toSet();
  }

  public Identity newBuilder() {
    return wrapped.deepCopy();
  }

  public boolean isSetRole() {
    return wrapped.isSetRole();
  }

  public String getRole() {
    return wrapped.getRole();
  }

  public boolean isSetUser() {
    return wrapped.isSetUser();
  }

  public String getUser() {
    return wrapped.getUser();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IIdentity)) {
      return false;
    }
    IIdentity other = (IIdentity) o;
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
