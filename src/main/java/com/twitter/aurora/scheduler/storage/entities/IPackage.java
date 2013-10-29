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

import com.twitter.aurora.gen.Package;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public final class IPackage {
  private final Package wrapped;

  private IPackage(Package wrapped) {
    this.wrapped = Preconditions.checkNotNull(wrapped);
  }

  static IPackage buildNoCopy(Package wrapped) {
    return new IPackage(wrapped);
  }

  public static IPackage build(Package wrapped) {
    return buildNoCopy(wrapped.deepCopy());
  }

  public static final Function<IPackage, Package> TO_BUILDER =
      new Function<IPackage, Package>() {
        @Override
        public Package apply(IPackage input) {
          return input.newBuilder();
        }
      };

  public static final Function<Package, IPackage> FROM_BUILDER =
      new Function<Package, IPackage>() {
        @Override
        public IPackage apply(Package input) {
          return new IPackage(input);
        }
      };

  public static ImmutableList<Package> toBuildersList(Iterable<IPackage> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toList();
  }

  public static ImmutableList<IPackage> listFromBuilders(Iterable<Package> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toList();
  }

  public static ImmutableSet<Package> toBuildersSet(Iterable<IPackage> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toSet();
  }

  public static ImmutableSet<IPackage> setFromBuilders(Iterable<Package> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toSet();
  }

  public Package newBuilder() {
    return wrapped.deepCopy();
  }

  public boolean isSetRole() {
    return wrapped.isSetRole();
  }

  public String getRole() {
    return wrapped.getRole();
  }

  public boolean isSetName() {
    return wrapped.isSetName();
  }

  public String getName() {
    return wrapped.getName();
  }

  public boolean isSetVersion() {
    return wrapped.isSetVersion();
  }

  public int getVersion() {
    return wrapped.getVersion();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IPackage)) {
      return false;
    }
    IPackage other = (IPackage) o;
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
